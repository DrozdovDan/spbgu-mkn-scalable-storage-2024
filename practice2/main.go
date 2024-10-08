package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Transaction struct {
	Action  string           `json:"action"`
	Name    string           `json:"name"`
	LSN     uint64           `json:"lsn"`
	Feature *geojson.Feature `json:"feature"`
}

type Engine struct {
	data           map[string]*geojson.Feature
	rTree          rtree.RTreeG[*geojson.Feature]
	transactionLog string
	lsn            uint64
	path           string
}

type Message struct {
	err  error
	data []byte
}

type Router struct {
	// Some attributes
}

func NewRouter(r *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	r.Handle("/", http.FileServer(http.Dir("../front/dist")))

	redirecting := []string{"/select", "/insert", "/replace", "/delete", "/checkpoint"}

	for _, row := range nodes {
		for _, name := range row {
			for _, method := range redirecting {
				r.Handle(method, http.RedirectHandler("/"+name+method, http.StatusTemporaryRedirect))
			}
		}
	}
	return &result
}

func (r *Router) Run() {
	slog.Info("Running router")
}

func (r *Router) Stop() {
	slog.Info("Stopping router")
}

type Storage struct {
	name               string
	transactionChannel chan Transaction
	messageChannel     chan Message
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewStorageErrorHandling(writer http.ResponseWriter, err error) {
	writer.WriteHeader(http.StatusInternalServerError)
	_, err = writer.Write([]byte(err.Error()))
	if err != nil {
		panic(err.Error())
	}
}

func NewStorage(r *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	storage := Storage{name, make(chan Transaction), make(chan Message), ctx, cancel}

	post := []string{"/insert", "/replace", "/delete"}

	for _, method := range post {
		r.HandleFunc("/"+name+method, func(writer http.ResponseWriter, request *http.Request) {
			slog.Info(method + " query")
			var feature geojson.Feature
			err := json.NewDecoder(request.Body).Decode(&feature)
			if err != nil {
				NewStorageErrorHandling(writer, err)
				return
			}
			storage.transactionChannel <- Transaction{Name: name, Action: method, Feature: &feature}
			response := <-storage.messageChannel
			if response.err != nil {
				NewStorageErrorHandling(writer, response.err)
				return
			}
			writer.WriteHeader(http.StatusOK)
		})
	}
	get := []string{"/select", "/checkpoint"}
	for _, method := range get {
		r.HandleFunc("/"+name+method, func(writer http.ResponseWriter, request *http.Request) {
			slog.Info(method + " query")
			storage.transactionChannel <- Transaction{Name: name, Action: method}
			response := <-storage.messageChannel
			if response.err != nil {
				NewStorageErrorHandling(writer, response.err)
				return
			}
			writer.WriteHeader(http.StatusOK)
			_, err := writer.Write(response.data)
			if err != nil {
				panic(err.Error())
			}
		})
	}
	return &storage
}

func (r *Storage) Run(path string, transactionLog string) {
	slog.Info("Running storage `" + r.name + "`")
	RunEngine(r, path, transactionLog)
}

func (r *Storage) Stop() {
	slog.Info("Stopping storage `" + r.name + "`")
	r.cancel()
}

func WriteSnapshot(engine *Engine) (string, error) {
	fileName := engine.path + "snapshot" + "--" + time.Now().Format("2006-01-02--15-04-05") + ".bin"
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return "", err
	}
	for _, feature := range engine.data {
		err = WriteTransaction(file, Transaction{Action: "/insert", Feature: feature})
		if err != nil {
			return "", err
		}
	}

	err = os.Truncate(engine.transactionLog, 0)

	if err != nil {
		return "", err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	return fileName, nil
}

func RunTransaction(engine *Engine, transaction Transaction) ([]byte, error) {
	switch transaction.Action {
	case "/insert":
		engine.data[transaction.Feature.ID.(string)] = transaction.Feature
		engine.rTree.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case "/replace":
		engine.rTree.Replace(
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, engine.data[transaction.Feature.ID.(string)],
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature,
		)
		engine.data[transaction.Feature.ID.(string)] = transaction.Feature
	case "/delete":
		engine.rTree.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
		delete(engine.data, transaction.Feature.ID.(string))
	case "/checkpoint":
		fileName, err := WriteSnapshot(engine)
		if err != nil {
			return nil, err
		}
		return []byte("Saved: " + fileName), nil
	case "/select":
		featureCollection := geojson.NewFeatureCollection()
		engine.rTree.Scan(
			func(min, max [2]float64, data *geojson.Feature) bool {
				featureCollection.Features = append(featureCollection.Features, data)
				return true
			},
		)
		marshal, err := json.Marshal(featureCollection)
		if err != nil {
			return nil, err
		}
		return marshal, nil
	default:
		return nil, errors.New("Unknown transaction action: " + transaction.Action)
	}
	return nil, nil
}

func WriteTransaction(file *os.File, transaction Transaction) error {
	marshal, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	_, err = file.WriteString(string(marshal) + "\n")
	return err
}

func NewEngine(path string, transactionLog string) (*Engine, error) {
	engine := Engine{map[string]*geojson.Feature{}, rtree.RTreeG[*geojson.Feature]{}, transactionLog, 0, path}
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(transactionLog, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var lastFile string
	var lastTime int64 = 0
	for _, snapshot := range files {
		stat, err := os.Stat(path + snapshot.Name())
		if err != nil {
			return nil, err
		}
		currentTime := stat.ModTime().Unix()
		if lastTime < currentTime {
			lastFile = snapshot.Name()
			lastTime = currentTime
		}
	}

	err = ReadTransaction(&engine, engine.path+lastFile)

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	err = ReadTransaction(&engine, engine.transactionLog)

	if err != nil {
		return nil, err
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			return
		}
	}(file)

	return &engine, nil
}

func ReadTransaction(engine *Engine, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var transaction Transaction
		err := json.Unmarshal(scanner.Bytes(), &transaction)
		if err != nil {
			return err
		}
		_, err = RunTransaction(engine, transaction)
		if err != nil {
			return err
		}
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			return
		}
	}(file)
	return nil
}

func RunEngine(r *Storage, path string, transactionLog string) {
	go func() {
		engine, err := NewEngine(path, transactionLog)
		if err != nil {
			panic(err.Error())
		}

		_, err = WriteSnapshot(engine)

		if err != nil {
			panic(err.Error())
		}

		for {
			select {
			case <-r.ctx.Done():
				return
			case transaction := <-r.transactionChannel:
				transaction.LSN = engine.lsn
				message, err := RunTransaction(engine, transaction)
				if err != nil {
					r.messageChannel <- Message{err: err, data: nil}
				} else {
					file, err := os.OpenFile(engine.transactionLog, os.O_WRONLY|os.O_APPEND, 0666)
					if err != nil {
						panic(err.Error())
					}
					r.messageChannel <- Message{err: WriteTransaction(file, transaction), data: message}
					err = file.Close()
					if err != nil {
						panic(err.Error())
					}
				}
				engine.lsn += 1
			}
		}
	}()
}

func main() {
	r := http.ServeMux{}

	router := NewRouter(&r, [][]string{{"storage"}})
	router.Run()

	storage := NewStorage(&r, "storage", []string{}, true)
	storage.Run("snapshots/", storage.name+".log")

	l := http.Server{}
	l.Addr = "127.0.0.1:8080"
	l.Handler = &r

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		slog.Info("Got signal: ", sig.String(), 1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := l.Shutdown(ctx); err != nil {
			panic(err.Error())
		}
	}()

	defer slog.Info("we are going down")
	slog.Info("listen http://" + l.Addr)
	err := l.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}

	storage.Stop()
	router.Stop()
}
