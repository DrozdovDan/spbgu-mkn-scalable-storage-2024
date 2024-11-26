package main

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", []string{}, true)
	snapshotsDir, logFilename := "testSnapshots/", s.name+".wal"

	go func() { s.Run() }()

	r := NewRouter(mux, [][]string{{"test"}}, 3)
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)
	defer CleanTestFiles(snapshotsDir, logFilename)

	InsertTest(t, mux, s)
	ReplaceTest(t, mux, s)
	DeleteTest(t, mux, s)
	CheckpointTest(t, mux, s)
	SelectTest(t, mux, s)
}

func PostTest(t *testing.T, mux *http.ServeMux, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func InsertTest(t *testing.T, mux *http.ServeMux, storage *Storage) *geojson.Feature {
	point := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	point.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/insert", point)
	return point
}

func ReplaceTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	line := geojson.NewFeature(orb.LineString{orb.Point{rand.Float64(), rand.Float64()}, orb.Point{rand.Float64(), rand.Float64()}})
	line.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/replace", line)
}

func DeleteTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	feature := InsertTest(t, mux, storage)
	PostTest(t, mux, "/"+storage.name+"/delete", feature)
}

func GetTest(t *testing.T, mux *http.ServeMux, url string) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	} else if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("GET", rr.Header().Get("location"), nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	}
}

func CheckpointTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	GetTest(t, mux, "/"+storage.name+"/snapshot")
}

func SelectTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	GetTest(t, mux, "/"+storage.name+"/select")
}

func CleanTestFiles(snapshotsDir string, logFilename string) {
	if err := os.RemoveAll(snapshotsDir); err != nil {
		panic(err.Error())
	}

	if err := os.Remove(logFilename); err != nil {
		panic(err.Error())
	}
}
