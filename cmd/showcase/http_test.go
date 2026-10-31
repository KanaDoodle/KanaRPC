package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestShowcaseRejectsForeignControlsAndDuplicateOperations(t *testing.T) {
	a := &app{}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/api/load", strings.NewReader(`{}`))
	request.Header.Set("Origin", "http://foreign.invalid")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	a.routes(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != 403 {
		t.Fatal(response.Code)
	}
	a.op.Lock()
	defer a.op.Unlock()
	if err := a.operation(func() error { t.Error("duplicate operation ran"); return nil }); err == nil {
		t.Fatal("overlapping session operation accepted")
	}
}
