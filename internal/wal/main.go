package wal

const WAL_FILE_PREFIX = "wal/"

type Record struct {
	StreamLabels map[string]string `json:"l"`
	Data         map[string]any    `json:"d"`
}
