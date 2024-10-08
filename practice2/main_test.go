package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSimple(t *testing.T) {
	mux := http.NewServeMux()
	storage := NewStorage(mux, "GlobalTest", []string{}, true)
	storage.Run("testSnap/", "test.log")
	router := NewRouter(mux, [][]string{{"GlobalTest"}})
	router.Run()

	InsertTest(t, mux, storage)
	ReplaceTest(t, mux, storage)
	DeleteTest(t, mux, storage)

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
}

func PostTest(t *testing.T, mux *http.ServeMux, path string, feature *geojson.Feature) {
	marshalJSON, err := json.Marshal(feature)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest("POST", path, bytes.NewReader(marshalJSON))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code == http.StatusTemporaryRedirect {
		request, err = http.NewRequest("POST", recorder.Header().Get("location"), bytes.NewReader(marshalJSON))
		if err != nil {
			t.Fatal(err)
		}
		recorder = httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Errorf("got %d, want %d", recorder.Code, http.StatusOK)
		}
	} else if recorder.Code != http.StatusOK {
		t.Errorf("got %d, want %d", recorder.Code, http.StatusOK)
	}
}

func InsertTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	point := geojson.NewFeature(orb.Point{rand.NormFloat64(), rand.NormFloat64()})
	point.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/insert", point)
}

func ReplaceTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	line := geojson.NewFeature(orb.LineString{orb.Point{rand.NormFloat64(), rand.NormFloat64()}, orb.Point{rand.NormFloat64(), rand.NormFloat64()}})
	line.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/replace", line)
}

func DeleteTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	point := geojson.NewFeature(orb.Point{rand.NormFloat64(), rand.NormFloat64()})
	point.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/insert", point)

	PostTest(t, mux, "/"+storage.name+"/delete", point)
}
