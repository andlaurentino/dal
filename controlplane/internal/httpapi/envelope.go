package httpapi

import (
	"encoding/json"
	"net/http"
)

// okResponse is returned for a single resource: the decoded fields the web
// UI's list/summary views key off, plus the raw YAML text as stored so the
// UI never has to re-serialize JSON back into YAML.
type okResponse struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	Spec any    `json:"spec"`
	Raw  string `json:"raw"`
}

type listResponse struct {
	Items []okResponse `json:"items"`
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

const (
	codeSchemaInvalid    = "schema_invalid"
	codeReferenceInvalid = "reference_invalid"
	codeNotFound         = "not_found"
	codeInternal         = "internal"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: message, Code: code})
}
