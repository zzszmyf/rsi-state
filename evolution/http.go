package evolution

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// NewHTTPServer exposes the evolution control plane over a small JSON API.
func NewHTTPServer(service *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
			return
		}

		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		switch {
		case len(parts) == 2 && parts[0] == "v1" && parts[1] == "workspaces":
			createWorkspace(w, r, service)
		case len(parts) == 4 && parts[0] == "v1" && parts[1] == "workspaces" && parts[3] == "events":
			appendEvent(w, r, service, parts[2])
		case len(parts) == 4 && parts[0] == "v1" && parts[1] == "workspaces" && parts[3] == "candidates":
			createCandidate(w, r, service, parts[2])
		case len(parts) == 6 && parts[0] == "v1" && parts[1] == "workspaces" && parts[3] == "candidates" && parts[5] == "promote":
			promoteCandidate(w, r, service, parts[2], parts[4])
		default:
			writeError(w, http.StatusNotFound, "route not found")
		}
	})
}

func createWorkspace(w http.ResponseWriter, r *http.Request, service *Service) {
	var input struct {
		ID string `json:"id"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	head, err := service.CreateWorkspace(r.Context(), input.ID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, head)
}

func appendEvent(w http.ResponseWriter, r *http.Request, service *Service, workspaceID string) {
	var input EventInput
	if !decodeJSON(w, r, &input) {
		return
	}
	input.WorkspaceID = workspaceID
	event, err := service.AppendEvent(r.Context(), input)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, event)
}

func createCandidate(w http.ResponseWriter, r *http.Request, service *Service, workspaceID string) {
	var input CandidateInput
	if !decodeJSON(w, r, &input) {
		return
	}
	input.WorkspaceID = workspaceID
	candidate, err := service.CreateCandidate(r.Context(), input)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, candidate)
}

func promoteCandidate(w http.ResponseWriter, r *http.Request, service *Service, workspaceID, candidateID string) {
	expectedRevision, err := strconv.ParseInt(r.URL.Query().Get("expected_revision"), 10, 64)
	if err != nil || expectedRevision < 0 {
		writeError(w, http.StatusBadRequest, "expected_revision must be a non-negative integer")
		return
	}
	head, err := service.PromoteCandidate(r.Context(), workspaceID, candidateID, expectedRevision)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, head)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	return true
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrRevisionConflict),
		errors.Is(err, ErrCandidateBaseStale), errors.Is(err, ErrCandidateFinalized):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
