package stream

import "fmt"

const STREAM_FILE_PREFIX = "stream/"

func dataFileName(streamID uint64, fileID uint64) string {
	return fmt.Sprintf("%s%d/%d.data", STREAM_FILE_PREFIX, streamID, fileID)
}

func metadataFileName(streamID uint64, fileID uint64) string {
	return fmt.Sprintf("%s%d/%d.metadata", STREAM_FILE_PREFIX, streamID, fileID)
}
