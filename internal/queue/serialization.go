package queue

import (
	"encoding/json"
	"maps"
	"time"
)

const (
	timestampLayout = time.RFC3339Nano
)

type queueFileDocument struct {
	Status      string             `json:"status"`
	LockedUntil *string            `json:"lockedUntil,omitempty"`
	SplitInto   *string            `json:"splitInto,omitempty"`
	SplitFrom   *string            `json:"splitFrom,omitempty"`
	Jobs        []queueJobDocument `json:"jobs"`
}

type queueJobDocument struct {
	ID          string  `json:"id"`
	Status      string  `json:"status"`
	LockedUntil *string `json:"lockedUntil,omitempty"`
	Payload     Payload `json:"payload"`
}

func encodeQueueFileState(state queueFileState) ([]byte, error) {
	doc := queueFileDocument{
		Status: string(state.Status),
		Jobs:   make([]queueJobDocument, len(state.Jobs)),
	}

	if doc.Status == "" {
		doc.Status = string(fileStatusActive)
	}

	if state.LockedUntil != nil {
		lockedUntil := formatTimestamp(*state.LockedUntil)
		doc.LockedUntil = &lockedUntil
	}

	if state.SplitInto != "" {
		splitInto := state.SplitInto
		doc.SplitInto = &splitInto
	}

	if state.SplitFrom != "" {
		splitFrom := state.SplitFrom
		doc.SplitFrom = &splitFrom
	}

	for i := range state.Jobs {
		doc.Jobs[i] = queueJobDocument{
			ID:      state.Jobs[i].ID,
			Status:  string(state.Jobs[i].Status),
			Payload: maps.Clone(state.Jobs[i].Payload),
		}

		if doc.Jobs[i].Status == "" {
			doc.Jobs[i].Status = string(jobStatusPending)
		}

		if state.Jobs[i].LockedUntil != nil {
			lockedUntil := formatTimestamp(*state.Jobs[i].LockedUntil)
			doc.Jobs[i].LockedUntil = &lockedUntil
		}
	}

	return json.Marshal(doc)
}

func decodeQueueFileState(raw []byte) (queueFileState, error) {
	var doc queueFileDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return queueFileState{}, err
	}

	state := queueFileState{
		Status: fileStatus(doc.Status),
		Jobs:   make([]queueJobState, len(doc.Jobs)),
	}

	if state.Status == "" {
		state.Status = fileStatusActive
	}

	if doc.LockedUntil != nil {
		lockedUntil, err := parseTimestamp(*doc.LockedUntil)
		if err != nil {
			return queueFileState{}, err
		}
		state.LockedUntil = &lockedUntil
	}

	if doc.SplitInto != nil {
		state.SplitInto = *doc.SplitInto
	}

	if doc.SplitFrom != nil {
		state.SplitFrom = *doc.SplitFrom
	}

	for i := range doc.Jobs {
		job := queueJobState{
			ID:      doc.Jobs[i].ID,
			Status:  jobStatus(doc.Jobs[i].Status),
			Payload: maps.Clone(doc.Jobs[i].Payload),
		}

		if job.Status == "" {
			job.Status = jobStatusPending
		}

		if doc.Jobs[i].LockedUntil != nil {
			lockedUntil, err := parseTimestamp(*doc.Jobs[i].LockedUntil)
			if err != nil {
				return queueFileState{}, err
			}
			job.LockedUntil = &lockedUntil
		}

		state.Jobs[i] = job
	}

	return state, nil
}

func formatTimestamp(v time.Time) string {
	return v.UTC().Format(timestampLayout)
}

func parseTimestamp(v string) (time.Time, error) {
	return time.Parse(timestampLayout, v)
}
