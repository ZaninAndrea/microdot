package queue

import (
	"context"
	"maps"
	"time"

	"github.com/ZaninAndrea/microdot/pkg/backoff"
)

var (
	PushBatchSize  = 16
	PushBatchDelay = 20 * time.Millisecond
	MaxJobsPerFile = 128
)

type pushRequest struct {
	payload Payload
	done    chan struct{}
}

func (q *Queue) Push(payload Payload) {
	request := pushRequest{
		payload: maps.Clone(payload),
		done:    make(chan struct{}),
	}

	q.pushRequest <- request
	<-request.done
}

func (q *Queue) pushWorker() {
	for {
		first := <-q.pushRequest

		batchLimit := PushBatchSize
		if batchLimit < 1 {
			batchLimit = 1
		}

		delay := PushBatchDelay
		if delay < 0 {
			delay = 0
		}

		batch := []pushRequest{first}
		timer := time.NewTimer(delay)

	collectBatch:
		for len(batch) < batchLimit {
			select {
			case req := <-q.pushRequest:
				batch = append(batch, req)
			case <-timer.C:
				break collectBatch
			}
		}

		payloads := make([]Payload, len(batch))
		for i, req := range batch {
			payloads[i] = req.payload
		}

		q.pushBatch(payloads)

		for _, req := range batch {
			close(req.done)
		}
	}
}

func (q *Queue) pushBatch(payloads []Payload) {
	if len(payloads) == 0 {
		return
	}

	ctx := context.Background()
	bo := backoff.NewExponential(MinBackoff, MaxBackoff)

	for {
		fileIDs, err := q.listFileIDs(ctx)
		if err != nil {
			bo.Wait()
			continue
		}

		if len(fileIDs) == 0 {
			fileID := randomID()
			state := queueFileState{
				Status: fileStatusActive,
				Jobs:   make([]queueJobState, 0, len(payloads)),
			}

			for _, payload := range payloads {
				state.Jobs = append(state.Jobs, newPendingJob(payload))
			}

			if len(state.Jobs) > MaxJobsPerFile {
				state.Status = fileStatusPreparingSplit
			}

			err := q.createFile(ctx, fileID, state)
			if err != nil {
				bo.Wait()
				continue
			}

			return
		}

		start := randomIndex(len(fileIDs))
		now := time.Now().UTC()
		shouldRestart := false
		candidates := make([]loadedQueueFile, 0, len(fileIDs))

		for offset := 0; offset < len(fileIDs); offset++ {
			id := fileIDs[(start+offset)%len(fileIDs)]

			loaded, exists, err := q.readFile(ctx, id)
			if err != nil {
				shouldRestart = true
				break
			} else if !exists {
				continue
			}

			if loaded.State.Status == fileStatusSplitting && isLockExpired(loaded.State.LockedUntil, now) {
				q.recoverSplit(ctx, loaded)
				shouldRestart = true
				break
			}

			if loaded.State.Status == fileStatusPreparingSplit && allJobsPending(loaded.State.Jobs) {
				q.splitPreparingFile(ctx, loaded)
				shouldRestart = true
				break
			}

			if loaded.State.Status == fileStatusActive || loaded.State.Status == fileStatusPreparingSplit {
				candidates = append(candidates, loaded)
			}
		}

		if shouldRestart || len(candidates) == 0 {
			bo.Wait()
			continue
		}

		candidate := pickPushCandidate(candidates)
		next := cloneQueueFileState(candidate.State)
		for _, payload := range payloads {
			next.Jobs = append(next.Jobs, newPendingJob(payload))
		}

		if len(next.Jobs) > MaxJobsPerFile {
			next.Status = fileStatusPreparingSplit
		}

		ok, err := q.replaceFileIfNotChanged(ctx, candidate, next)
		if err != nil || !ok {
			bo.Wait()
			continue
		}

		return
	}
}

func newPendingJob(payload Payload) queueJobState {
	return queueJobState{
		ID:      randomID(),
		Status:  jobStatusPending,
		Payload: maps.Clone(payload),
	}
}

func pickPushCandidate(candidates []loadedQueueFile) loadedQueueFile {
	if len(candidates) == 1 {
		return candidates[0]
	}

	threshold := MaxJobsPerFile / 4
	if threshold > 0 {
		for _, candidate := range candidates {
			if len(candidate.State.Jobs) >= threshold {
				return candidate
			}
		}
	}

	lowest := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.ID < lowest.ID {
			lowest = candidate
		}
	}

	return lowest
}
