package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

const SnapshotDateFormat = "02-Jan-2006-15_04_05"

const (
	Insert Action = iota
	Replace
	Delete
	Select
	Snapshot
)

var PostTransactions = []Action{Insert, Delete, Insert}
var port = ":8080"

type Action int

type Router struct {
	maxNumOfSelectQueries uint64
}

type Message struct {
	Err  error  `json:"err"`
	Body []byte `json:"body"`
}

type VectorClock struct {
	vclock map[string]uint64
	mtx    sync.Mutex
}

var VClock = VectorClock{
	vclock: make(map[string]uint64),
	mtx:    sync.Mutex{},
}

type Storage struct {
	name            string
	leader          bool
	replicas        []string
	engine          *StorageEngine
	featuresPrimInd map[string]*geojson.Feature
	featuresRTree   rtree.RTreeG[*geojson.Feature]
	ctx             context.Context
	cancel          context.CancelFunc
}

type StorageEngine struct {
	lsnCounter       uint64
	replicasRegistry map[string]*websocket.Conn
	transactionCh    chan Transaction
	ResponseCh       chan Message
	snapshotsDir     string
	logFilename      string
}

type Transaction struct {
	Lsn     uint64           `json:"lsn"`
	Name    string           `json:"name"`
	Action  Action           `json:"action"`
	Feature *geojson.Feature `json:"feature"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

const snapshotsDirectory = "snapshots/"

func cleanFile(filename string) error {
	return os.Truncate(filename, 0)
}

func (s *Storage) leaderCheck() error {
	if !s.leader {
		return errors.New("the given storage is not a leader")
	}
	return nil
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		panic(err.Error())
	}
}

func getLastFileFilenameInDir(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var newestFile string
	var newestTime int64

	for _, snapshot := range entries {
		fileInfo, err := os.Stat(dir + snapshot.Name())
		if err != nil {
			return "", err
		}

		currTime := fileInfo.ModTime().Unix()
		if currTime > newestTime {
			newestTime = currTime
			newestFile = snapshot.Name()
		}
	}

	return newestFile, nil
}

func NewRouter(mux *http.ServeMux, nodes [][]string, maxNumOfSelectQueries uint64) *Router {
	router := Router{maxNumOfSelectQueries: maxNumOfSelectQueries}

	var numOfSelectQueries uint64
	var url string

	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))

	mux.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
		numOfSelectQueries++
		if numOfSelectQueries == router.maxNumOfSelectQueries {
			url = "/" + nodes[0][1+rand.Intn(len(nodes[0])-1)] + "/select"
			numOfSelectQueries = 0
		} else {
			url = "/" + nodes[0][0] + "/select"
		}
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	})

	mux.Handle("/insert", http.RedirectHandler("/"+nodes[0][0]+"/insert", http.StatusTemporaryRedirect))
	mux.Handle("/replace", http.RedirectHandler("/"+nodes[0][0]+"/replace", http.StatusTemporaryRedirect))
	mux.Handle("/delete", http.RedirectHandler("/"+nodes[0][0]+"/delete", http.StatusTemporaryRedirect))
	mux.Handle("/snapshot", http.RedirectHandler("/"+nodes[0][0]+"/snapshot", http.StatusTemporaryRedirect))

	return &router
}

func (r *Router) Run() {
	log.Print("Starting router...")
	log.Print("Router has been started")
}

func (r *Router) Stop() {
	log.Print("Stopping router...")
	log.Print("Router has been stopped")
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())

	engine := StorageEngine{
		lsnCounter:       1,
		replicasRegistry: make(map[string]*websocket.Conn),
		transactionCh:    make(chan Transaction),
		ResponseCh:       make(chan Message),
		snapshotsDir:     snapshotsDirectory,
		logFilename:      name + ".wal",
	}

	storage := Storage{
		name:            name,
		leader:          leader,
		replicas:        replicas,
		engine:          &engine,
		featuresPrimInd: make(map[string]*geojson.Feature),
		featuresRTree:   rtree.RTreeG[*geojson.Feature]{},
		ctx:             ctx,
		cancel:          cancel,
	}

	storage.setupHandlers(mux)
	return &storage
}

func (s *Storage) setupHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/"+s.name+"/select", func(w http.ResponseWriter, _ *http.Request) {
		log.Print("SELECT QUERY...")
		s.handleGetRequest(w, Select)
	})

	mux.HandleFunc("/"+s.name+"/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		log.Print("SAVING SNAPSHOT...")
		s.handleGetRequest(w, Snapshot)
	})

	if s.leader {
		mux.HandleFunc("/"+s.name+"/insert", func(w http.ResponseWriter, r *http.Request) {
			log.Print("INSERT QUERY")
			s.handlePostRequest(w, r, Insert)
		})

		mux.HandleFunc("/"+s.name+"/replace", func(w http.ResponseWriter, r *http.Request) {
			log.Print("REPLACE QUERY")
			s.handlePostRequest(w, r, Replace)
		})

		mux.HandleFunc("/"+s.name+"/delete", func(w http.ResponseWriter, r *http.Request) {
			log.Print("DELETE QUERY")
			s.handlePostRequest(w, r, Delete)
		})
	} else {
		mux.HandleFunc("/"+s.name+"/replication", func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err.Error())
			}
			defer func() {
				err := conn.Close()
				if err != nil {
					panic(err.Error())
				}
			}()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					var err *websocket.CloseError
					if errors.As(err, &err) && err.Code == websocket.CloseNormalClosure {
						s.Stop()
						err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
						if err != nil {
							panic(err.Error())
						}
						return
					}
					writeError(w, err)
				}

				var tmpTr Transaction
				if err := json.Unmarshal(message, &tmpTr); err != nil {
					writeError(w, err)
				}

				s.engine.transactionCh <- tmpTr
				resp := <-s.engine.ResponseCh

				jsonBody, err := json.Marshal(resp)
				if err != nil {
					writeError(w, err)
				}

				err = conn.WriteMessage(websocket.TextMessage, jsonBody)
				if err != nil {
					panic(err.Error())
				}
			}
		})
	}
}

func (s *Storage) handlePostRequest(w http.ResponseWriter, r *http.Request, action Action) {
	var feature geojson.Feature
	if err := json.NewDecoder(r.Body).Decode(&feature); err != nil {
		writeError(w, err)
		return
	}

	s.engine.transactionCh <- Transaction{
		Lsn:     s.engine.lsnCounter,
		Name:    s.name,
		Action:  action,
		Feature: &feature,
	}

	engineResp := <-s.engine.ResponseCh
	if engineResp.Err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		writeError(w, engineResp.Err)
	}
}

func (s *Storage) handleGetRequest(w http.ResponseWriter, action Action) {
	s.engine.transactionCh <- Transaction{Name: s.name, Action: action}
	engineResp := <-s.engine.ResponseCh

	if engineResp.Err == nil {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(engineResp.Body)
		if err != nil {
			panic(err.Error())
		}
	} else {
		writeError(w, engineResp.Err)
	}
}

func (s *Storage) handleTransaction(tr Transaction) {
	VClock.mtx.Lock()
	if slices.Contains(PostTransactions, tr.Action) && tr.Lsn <= VClock.vclock[s.name] && tr.Lsn > 0 {
		s.engine.ResponseCh <- Message{}
		VClock.mtx.Unlock()
		return
	}
	VClock.mtx.Unlock()

	msg, err := s.runTransaction(tr)
	if err != nil {
		s.engine.ResponseCh <- Message{Err: err}
	} else {
		if s.leader {
			logFd, err := os.OpenFile(s.engine.logFilename, os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				panic(err.Error())
			}
			if err = logTransaction(logFd, tr, true); err != nil {
				panic(err.Error())
			}
			if err = logFd.Close(); err != nil {
				panic(err.Error())
			}
		}

		if slices.Contains(PostTransactions, tr.Action) && tr.Lsn > 0 {
			VClock.mtx.Lock()
			VClock.vclock[s.name] = s.engine.lsnCounter
			VClock.mtx.Unlock()

			s.engine.lsnCounter++
		}

		s.engine.ResponseCh <- Message{Err: err, Body: msg}
	}
}

func (s *Storage) Init() error {
	if err := s.leaderCheck(); err != nil {
		return err
	}

	if err := initFiles(s.engine.logFilename, s.engine.snapshotsDir); err != nil {
		return err
	}

	snapshot, err := getLastFileFilenameInDir(s.engine.snapshotsDir)
	if err != nil {
		return err
	}

	if err = s.runTransactionsFromFile(s.engine.snapshotsDir + snapshot); err != nil && !os.IsNotExist(err) {
		return err
	}

	err = s.runTransactionsFromFile(s.engine.logFilename)
	if err != nil {
		return err
	}

	if _, err := s.saveSnapshot(); err != nil {
		return err
	}

	if err = cleanFile(s.engine.logFilename); err != nil {
		return err
	}

	return nil
}

func initFiles(logFilename string, snapshotsDir string) error {
	if err := os.Mkdir(snapshotsDir, 0755); err != nil && !os.IsExist(err) {
		return err
	}

	file, err := os.OpenFile(logFilename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	return nil
}

func (s *Storage) Run() {
	log.Printf("Starting storage '%s'...", s.name)

	go func() {

		if s.leader {
			if err := s.connectToReplicas(); err != nil {
				panic("Can't init DB: " + err.Error())
			}
			if err := s.Init(); err != nil {
				panic("Can't init DB: " + err.Error())
			}
		}
		log.Printf("Storage '%s' has been started", s.name)

		for {
			select {
			case <-s.ctx.Done():
				return

			case tr := <-s.engine.transactionCh:
				s.handleTransaction(tr)
			}
		}
	}()
}

func (s *Storage) Stop() {
	log.Printf("Stopping storage '%s'...", s.name)

	if s.leader {
		for _, conn := range s.engine.replicasRegistry {
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				panic(err.Error())
			}

			err = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			if err != nil {
				panic(err.Error())
			}
			_, _, err = conn.ReadMessage()
			if err != nil {
				var err *websocket.CloseError
				if errors.As(err, &err) && (err.Code == websocket.CloseNormalClosure) {
					continue
				}
				panic(err.Error())
			}
		}
	}

	s.cancel()
	log.Printf("Storage '%s' has been stopped", s.name)
}

func (s *Storage) saveSnapshot() (string, error) {
	snapshotFilename := s.engine.snapshotsDir + "snapshot-" + time.Now().Format(SnapshotDateFormat) + ".ckp"
	file, err := os.OpenFile(snapshotFilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}

	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	for _, feature := range s.featuresPrimInd {
		if err = logTransaction(file, Transaction{Name: s.name, Action: Insert, Feature: feature}, true); err != nil {
			return "", err
		}
	}

	return snapshotFilename, nil
}

func logTransaction(file *os.File, transaction Transaction, onlyPostTransaction bool) error {
	if onlyPostTransaction && !slices.Contains(PostTransactions, transaction.Action) {
		return nil
	}
	marshal, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	_, err = file.WriteString(string(marshal) + "\n")
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) runTransaction(transaction Transaction) ([]byte, error) {

	switch transaction.Action {
	case Insert:
		if s.leader {
			if err := s.notifyReplicas(&transaction); err != nil {
				return nil, err
			}
		}

		s.featuresPrimInd[transaction.Feature.ID.(string)] = transaction.Feature
		s.featuresRTree.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Replace:
		if s.leader {
			if err := s.notifyReplicas(&transaction); err != nil {
				return nil, err
			}
		}

		s.featuresPrimInd[transaction.Feature.ID.(string)] = transaction.Feature
		s.featuresRTree.Replace(
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, s.featuresPrimInd[transaction.Feature.ID.(string)],
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature,
		)
	case Delete:
		if s.leader {
			if err := s.notifyReplicas(&transaction); err != nil {
				return nil, err
			}
		}

		delete(s.featuresPrimInd, transaction.Feature.ID.(string))
		s.featuresRTree.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Snapshot:
		snapshotFilename, err := s.saveSnapshot()
		return []byte("Snapshot '" + snapshotFilename + "' has been saved!"), err

	case Select:
		featureCollection := geojson.NewFeatureCollection()
		s.featuresRTree.Scan(
			func(_, _ [2]float64, data *geojson.Feature) bool {
				featureCollection.Append(data)
				return true
			},
		)
		marshal, err := json.Marshal(featureCollection)
		if err != nil {
			return nil, err
		}
		return marshal, nil
	default:
		return nil, errors.ErrUnsupported
	}
	return nil, nil
}

func (s *Storage) runTransactionsFromFile(filename string) error {
	file, err := os.Open(filename)
	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tmpTr Transaction
		if err = json.Unmarshal(scanner.Bytes(), &tmpTr); err != nil {
			return err
		}

		tmpTr.Lsn = 0
		if _, err = s.runTransaction(tmpTr); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) connectToReplicas() error {
	if !s.leader {
		return errors.New(s.name + " is not a leader")
	}
	for _, replica := range s.replicas {
		conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1"+port+"/"+replica+"/replication", nil)
		if err != nil {
			return err
		}
		s.engine.replicasRegistry[replica] = conn
	}
	return nil
}

func (s *Storage) notifyReplicas(tr *Transaction) error {
	if err := s.leaderCheck(); err != nil {
		panic(err.Error())
	}
	jsonBody, err := json.Marshal(tr)
	if err != nil {
		panic(err.Error())
	}

	for _, conn := range s.engine.replicasRegistry {
		err = conn.WriteMessage(websocket.TextMessage, jsonBody)
		if err != nil {
			return err
		}

		err := conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			return err
		}
		_, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var tmpMsg Message
		if err = json.Unmarshal(message, &tmpMsg); err != nil {
			return err
		}
		if tmpMsg.Err != nil {
			return err
		}
	}
	return nil
}

func (v *VectorClock) Init(nodes [][]string) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	for _, nodeRow := range nodes {
		for _, node := range nodeRow {
			v.vclock[node] = 0
		}
	}
}

func signalHandler(server *http.Server) {
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigChan
		log.Printf("Got signal: %s", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := server.Shutdown(ctx)
		if err != nil {
			panic(err.Error())
		}
	}()
}

func main() {
	mux := http.ServeMux{}
	nodes := [][]string{{"master", "slave1", "slave2"}}

	router := NewRouter(&mux, nodes, 3)
	m := NewStorage(&mux, nodes[0][0], []string{nodes[0][1], nodes[0][2]}, true)
	s1 := NewStorage(&mux, nodes[0][1], make([]string, 0), false)
	s2 := NewStorage(&mux, nodes[0][2], make([]string, 0), false)

	VClock.Init(nodes)

	router.Run()

	m.Run()
	s1.Run()
	s2.Run()

	server := http.Server{
		Addr:    "127.0.0.1" + port,
		Handler: &mux,
	}
	signalHandler(&server)

	log.Printf("Listen http://%s", server.Addr)
	err := server.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err.Error())
	}

	router.Stop()
	m.Stop()

	log.Print("Shutting down...")
}
