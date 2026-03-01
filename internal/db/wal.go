package db

type WAL struct{}

func NewWAL(path string) (*WAL, error) {
	return &WAL{}, nil
}

func (w *WAL) AddDocument(streamLabels Labels, data map[string]any) error {
	// TODO: Implement the logic to write the document to the WAL
	return nil
}
