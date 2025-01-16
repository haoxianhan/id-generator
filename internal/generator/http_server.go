package generator

import (
	"encoding/json"
	"net/http"
)

type IDServer struct {
	generator *SegmentIDGenerator
}

func NewIDServer(generator *SegmentIDGenerator) *IDServer {
	return &IDServer{
		generator: generator,
	}
}

func (s *IDServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	id, err := s.generator.NextID()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(map[string]int64{"id": id})
}
