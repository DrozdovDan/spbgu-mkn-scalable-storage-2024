package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSimple(t *testing.T) {
	mux := http.NewServeMux()
	storage := NewStorage(mux, "GlobalTest", []string{}, true)
	storage.Run()
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

	file, err := os.ReadFile(filepath.Join(storage.path, point.ID.(string)+".json"))

	if err != nil {
		t.Fatal(err)
	}

	marshal, err := json.Marshal(point)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, file, marshal)
}

func ReplaceTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	line := geojson.NewFeature(orb.LineString{orb.Point{rand.NormFloat64(), rand.NormFloat64()}, orb.Point{rand.NormFloat64(), rand.NormFloat64()}})
	line.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/replace", line)

	file, err := os.ReadFile(filepath.Join(storage.path, line.ID.(string)+".json"))

	if err != nil {
		t.Fatal(err)
	}

	marshal, err := json.Marshal(line)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, file, marshal)
}

func DeleteTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	point := geojson.NewFeature(orb.Point{rand.NormFloat64(), rand.NormFloat64()})
	point.ID = uuid.New().String()

	PostTest(t, mux, "/"+storage.name+"/insert", point)

	PostTest(t, mux, "/"+storage.name+"/delete", point)

	_, err := os.ReadFile(filepath.Join(storage.path, point.ID.(string)+".json"))

	assert.True(t, os.IsNotExist(err))
}
