package queue

import (
	"context"
	"maps"
	"time"

	"github.com/ZaninAndrea/microdot/pkg/backoff"
)

func (q *Queue) Pull(claimDuration time.Duration) Item {
	ctx := context.Background()
	bo := backoff.NewExponential(MinBackoff, MaxBackoff)

	for {
		fileIDs, err := q.listFileIDs(ctx)
		if err != nil || len(fileIDs) == 0 {
			bo.Wait()
			continue
		}

		start := randomIndex(len(fileIDs))
		now := time.Now().UTC()

		for offset := 0; offset < len(fileIDs); offset++ {
			id := fileIDs[(start+offset)%len(fileIDs)]

			loaded, exists, err := q.readFile(ctx, id)
			if err != nil {
				break
			}
			if !exists {
				continue
			}

			if loaded.State.Status == fileStatusSplitting && isLockExpired(loaded.State.LockedUntil, now) {
				q.recoverSplit(ctx, loaded)
				break
			}

			if loaded.State.Status == fileStatusPreparingSplit && allJobsPending(loaded.State.Jobs) {
				q.splitPreparingFile(ctx, loaded)
				break
			}

			if loaded.State.Status != fileStatusActive {
				continue
			}

			jobIndex := firstClaimableJobIndex(loaded.State.Jobs, now)
			if jobIndex < 0 {
				continue
			}

			next := cloneQueueFileState(loaded.State)
			lockedUntil := time.Now().UTC().Add(claimDuration)
			next.Jobs[jobIndex].Status = jobStatusInProgress
			next.Jobs[jobIndex].LockedUntil = &lockedUntil

			ok, err := q.replaceFileIfNotChanged(ctx, loaded, next)
			if err != nil {
				break
			}
			if !ok {
				break
			}

			return Item{
				fileID:  loaded.ID,
				jobID:   next.Jobs[jobIndex].ID,
				payload: maps.Clone(next.Jobs[jobIndex].Payload),
			}
		}

		bo.Wait()
	}
}

func (q *Queue) ExtendClaim(item Item, duration time.Duration) {
	if item.fileID == "" || item.jobID == "" {
		return
	}

	if duration <= 0 {
		return
	}

	ctx := context.Background()
	bo := backoff.NewExponential(MinBackoff, MaxBackoff)

	for {
		loaded, exists, err := q.readFile(ctx, item.fileID)
		if err != nil {
			bo.Wait()
			continue
		}
		if !exists {
			return
		}

		jobIndex := findJobIndex(loaded.State.Jobs, item.jobID)
		if jobIndex < 0 {
			return
		}

		next := cloneQueueFileState(loaded.State)
		lockedUntil := time.Now().UTC().Add(duration)
		next.Jobs[jobIndex].Status = jobStatusInProgress
		next.Jobs[jobIndex].LockedUntil = &lockedUntil

		ok, err := q.replaceFileIfNotChanged(ctx, loaded, next)
		if err != nil {
			bo.Wait()
			continue
		}
		if !ok {
			bo.Wait()
			continue
		}

		return
	}
}

func (q *Queue) Remove(item Item) {
	if item.fileID == "" || item.jobID == "" {
		return
	}

	ctx := context.Background()
	bo := backoff.NewExponential(MinBackoff, MaxBackoff)

	for {
		loaded, exists, err := q.readFile(ctx, item.fileID)
		if err != nil {
			bo.Wait()
			continue
		}
		if !exists {
			return
		}

		jobIndex := findJobIndex(loaded.State.Jobs, item.jobID)
		if jobIndex < 0 {
			return
		}

		if len(loaded.State.Jobs) == 1 {
			ok, err := q.deleteFileIfNotChanged(ctx, loaded)
			if err != nil {
				bo.Wait()
				continue
			}
			if !ok {
				bo.Wait()
				continue
			}

			return
		}

		next := cloneQueueFileState(loaded.State)
		next.Jobs = append(next.Jobs[:jobIndex], next.Jobs[jobIndex+1:]...)

		ok, err := q.replaceFileIfNotChanged(ctx, loaded, next)
		if err != nil {
			bo.Wait()
			continue
		}
		if !ok {
			bo.Wait()
			continue
		}

		return
	}
}

func findJobIndex(jobs []queueJobState, id string) int {
	for i, job := range jobs {
		if job.ID == id {
			return i
		}
	}

	return -1
}

func firstClaimableJobIndex(jobs []queueJobState, now time.Time) int {
	for i, job := range jobs {
		if job.Status == jobStatusPending {
			return i
		}

		if job.Status == jobStatusInProgress && isLockExpired(job.LockedUntil, now) {
			return i
		}
	}

	return -1
}
