package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/paulmach/orb/geojson"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type Router struct {
	// Some attributes
}

func NewRouter(r *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	r.Handle("/", http.FileServer(http.Dir("../front/dist")))

	redirecting := []string{"/select", "/insert", "/replace", "/delete"}

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
	name  string
	path  string
	mutex sync.Mutex
}

func NewStorageErrorHandling(writer http.ResponseWriter, mutex *sync.Mutex, err error) {
	mutex.Unlock()
	writer.WriteHeader(http.StatusInternalServerError)
	_, err = writer.Write([]byte(err.Error()))
	if err != nil {
		panic(err)
	}
}

func NewStorage(r *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	root, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	result := Storage{name, filepath.Join(root, name), sync.Mutex{}}

	redirecting := []string{"/select", "/insert", "/replace", "/delete"}

	for _, method := range redirecting {
		r.HandleFunc("/"+name+method, func(writer http.ResponseWriter, request *http.Request) {
			result.mutex.Lock()
			slog.Info(method + " query")
			switch method {
			case "/select":
				featureCollection := geojson.NewFeatureCollection()
				dir, err := os.ReadDir(result.path)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}
				for _, entry := range dir {
					file, err := os.ReadFile(filepath.Join(result.path, entry.Name()))
					if err != nil {
						NewStorageErrorHandling(writer, &result.mutex, err)
						return
					}
					var feature geojson.Feature
					err = json.Unmarshal(file, &feature)
					if err != nil {
						NewStorageErrorHandling(writer, &result.mutex, err)
						return
					}
					featureCollection.Features = append(featureCollection.Features, &feature)
				}

				marshal, err := json.Marshal(featureCollection)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}

				writer.WriteHeader(http.StatusOK)

				_, err = writer.Write(marshal)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}

			case "/insert", "/replace":
				var feature geojson.Feature
				dec := json.NewDecoder(request.Body)
				err := dec.Decode(&feature)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}
				marshal, err := json.Marshal(feature)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}
				err = os.WriteFile(filepath.Join(result.path, feature.ID.(string)+".json"), marshal, 0666)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}
				writer.WriteHeader(http.StatusOK)

			case "/delete":
				var feature geojson.Feature
				dec := json.NewDecoder(request.Body)
				err := dec.Decode(&feature)
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}
				err = os.Remove(filepath.Join(result.path, feature.ID.(string)+".json"))
				if err != nil {
					NewStorageErrorHandling(writer, &result.mutex, err)
					return
				}
				writer.WriteHeader(http.StatusOK)

			default:
				writer.WriteHeader(http.StatusOK)
			}
			result.mutex.Unlock()
		})
	}
	return &result
}

func (r *Storage) Run() {
	slog.Info("Running storage `" + r.name + "`")
	err := os.MkdirAll(r.path, 0755)
	if err != nil {
		panic(err)
	}
}

func (r *Storage) Stop() {
	slog.Info("Stopping storage `" + r.name + "`")
	err := os.RemoveAll(r.path)
	if err != nil {
		panic(err)
	}
}

func main() {
	r := http.ServeMux{}

	router := NewRouter(&r, [][]string{{"storage"}})
	router.Run()

	storage := NewStorage(&r, "storage", []string{}, true)
	storage.Run()

	l := http.Server{}
	l.Addr = "127.0.0.1:8080"
	l.Handler = &r

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		for _ = range sigs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			l.Shutdown(ctx)
		}
	}()

	defer slog.Info("we are going down")
	slog.Info("listen http://" + l.Addr)
	err := l.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}
}
