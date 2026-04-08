package queue

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"maps"
	mrand "math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/ZaninAndrea/microdot/pkg/blob"
)

var (
	MinBackoff = 20 * time.Millisecond
	MaxBackoff = 2 * time.Second
)

type fileStatus string

const (
	fileStatusActive         fileStatus = "active"
	fileStatusPreparingSplit fileStatus = "preparing_split"
	fileStatusSplitting      fileStatus = "splitting"
)

type jobStatus string

const (
	jobStatusPending    jobStatus = "pending"
	jobStatusInProgress jobStatus = "in_progress"
)

type Payload map[string]any

type Item struct {
	fileID  string
	jobID   string
	payload Payload
}

func (i Item) Payload() Payload {
	return maps.Clone(i.payload)
}

type Queue struct {
	bucket      blob.Bucket
	directory   string
	pushRequest chan pushRequest
}

type loadedQueueFile struct {
	ID    string
	ETag  string
	Key   string
	Raw   []byte
	State queueFileState
}

type queueFileState struct {
	Status      fileStatus
	LockedUntil *time.Time
	SplitInto   string
	SplitFrom   string
	Jobs        []queueJobState
}

type queueJobState struct {
	ID          string
	Status      jobStatus
	LockedUntil *time.Time
	Payload     Payload
}

func NewQueue(bucket blob.Bucket, directory string) *Queue {
	q := &Queue{
		bucket:      bucket,
		directory:   normalizeDirectory(directory),
		pushRequest: make(chan pushRequest, max(2, PushBatchSize*2)),
	}

	go q.pushWorker()

	return q
}

func (q *Queue) listFileIDs(ctx context.Context) ([]string, error) {
	ids := make([]string, 0)

	for result := range q.bucket.ListObjects(ctx, q.directory) {
		if result.Err != nil {
			return nil, result.Err
		}

		key := result.Value
		if !strings.HasPrefix(key, q.directory) {
			continue
		}

		relative := strings.TrimPrefix(key, q.directory)
		if relative == "" || strings.Contains(relative, "/") {
			continue
		}
		if !strings.HasSuffix(relative, ".json") {
			continue
		}

		id := strings.TrimSuffix(relative, ".json")
		if id == "" {
			continue
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)
	return ids, nil
}

func (q *Queue) readFile(ctx context.Context, id string) (loadedQueueFile, bool, error) {
	key := q.fileKey(id)

	reader, etag, err := q.bucket.GetObject(ctx, key)
	if err != nil {
		return loadedQueueFile{}, false, err
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return loadedQueueFile{}, false, err
	}

	state, err := decodeQueueFileState(raw)
	if err != nil {
		return loadedQueueFile{}, false, err
	}

	return loadedQueueFile{
		ID:    id,
		ETag:  etag,
		Key:   key,
		Raw:   raw,
		State: state,
	}, true, nil
}

func (q *Queue) replaceFileIfNotChanged(ctx context.Context, loaded loadedQueueFile, next queueFileState) (bool, error) {
	nextRaw, err := encodeQueueFileState(next)
	if err != nil {
		return false, err
	}

	err = q.bucket.PutObjectIfMatch(ctx, loaded.Key, bytes.NewReader(nextRaw), loaded.ETag)
	if err != nil {
		if err == blob.ETAG_CHANGED_ERROR {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (q *Queue) deleteFileIfNotChanged(ctx context.Context, loaded loadedQueueFile) (bool, error) {
	err := q.bucket.DeleteObject(ctx, loaded.Key, &loaded.ETag)
	if err != nil {
		if err == blob.ETAG_CHANGED_ERROR || err == blob.NO_SUCH_KEY_ERROR {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (q *Queue) createFile(ctx context.Context, id string, state queueFileState) error {
	raw, err := encodeQueueFileState(state)
	if err != nil {
		return err
	}

	err = q.bucket.PutObject(ctx, q.fileKey(id), bytes.NewReader(raw), false)
	if err != nil {
		return err
	}

	return nil
}

func (q *Queue) fileKey(id string) string {
	return q.directory + id + ".json"
}

func normalizeDirectory(directory string) string {
	directory = strings.TrimSpace(directory)
	directory = strings.Trim(directory, "/")
	if directory == "" {
		return ""
	}

	return directory + "/"
}

func allJobsPending(jobs []queueJobState) bool {
	for _, job := range jobs {
		if job.Status != jobStatusPending {
			return false
		}
	}

	return true
}

func isLockExpired(lockedUntil *time.Time, now time.Time) bool {
	if lockedUntil == nil {
		return false
	}

	return !lockedUntil.After(now)
}

func cloneQueueFileState(state queueFileState) queueFileState {
	clone := queueFileState{
		Status:      state.Status,
		LockedUntil: cloneTimePtr(state.LockedUntil),
		SplitInto:   state.SplitInto,
		SplitFrom:   state.SplitFrom,
		Jobs:        cloneJobs(state.Jobs),
	}

	return clone
}

func cloneJobs(jobs []queueJobState) []queueJobState {
	if jobs == nil {
		return nil
	}

	clone := make([]queueJobState, len(jobs))
	for i := range jobs {
		clone[i] = queueJobState{
			ID:          jobs[i].ID,
			Status:      jobs[i].Status,
			LockedUntil: cloneTimePtr(jobs[i].LockedUntil),
			Payload:     maps.Clone(jobs[i].Payload),
		}
	}

	return clone
}

func cloneTimePtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}

	copyValue := v.UTC()
	return &copyValue
}

func randomID() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(mrand.IntN(256))
	}

	return hex.EncodeToString(b)
}

func randomIndex(size int) int {
	if size <= 1 {
		return 0
	}

	return mrand.IntN(size)
}
