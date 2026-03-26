package wal

import "github.com/ZaninAndrea/microdot/internal/db/types"

const WAL_FILE_PREFIX = "wal/"

type record struct {
	StreamLabels types.Labels   `json:"l"`
	Data         types.Document `json:"d"`
}
