package queue

import (
	"context"
	"time"

	"github.com/ZaninAndrea/microdot/pkg/backoff"
)

var (
	SplitTimeout = 30 * time.Second
)

func (q *Queue) splitPreparingFile(ctx context.Context, original loadedQueueFile) {
	bo := backoff.NewExponential(MinBackoff, MaxBackoff)

	for {
		current, exists, err := q.readFile(ctx, original.ID)
		if err != nil {
			bo.Wait()
			continue
		}
		if !exists {
			return
		}

		if current.State.Status != fileStatusPreparingSplit || !allJobsPending(current.State.Jobs) {
			return
		}

		if len(current.State.Jobs) < 2 {
			return
		}

		splitID := randomID()
		lockUntil := time.Now().UTC().Add(SplitTimeout)

		locked := cloneQueueFileState(current.State)
		locked.Status = fileStatusSplitting
		locked.SplitInto = splitID
		locked.LockedUntil = &lockUntil

		ok, err := q.replaceFileIfNotChanged(ctx, current, locked)
		if err != nil {
			bo.Wait()
			continue
		}
		if !ok {
			bo.Wait()
			continue
		}

		lockedRaw, err := encodeQueueFileState(locked)
		if err != nil {
			return
		}

		q.resumeSplitFromStep3(ctx, loadedQueueFile{
			ID:    current.ID,
			Key:   current.Key,
			Raw:   lockedRaw,
			State: locked,
		})

		return
	}
}

func (q *Queue) recoverSplit(ctx context.Context, encountered loadedQueueFile) {
	now := time.Now().UTC()
	if encountered.State.Status != fileStatusSplitting || !isLockExpired(encountered.State.LockedUntil, now) {
		return
	}

	if encountered.State.SplitInto != "" {
		q.recoverFromOriginal(ctx, encountered)
		return
	}

	if encountered.State.SplitFrom == "" {
		return
	}

	original, exists, err := q.readFile(ctx, encountered.State.SplitFrom)
	if err != nil || !exists {
		return
	}

	if original.State.Status == fileStatusSplitting && isLockExpired(original.State.LockedUntil, now) {
		q.recoverFromOriginal(ctx, original)
		return
	}

	latestTarget, exists, err := q.readFile(ctx, encountered.ID)
	if err != nil || !exists {
		return
	}

	if latestTarget.State.Status == fileStatusSplitting && isLockExpired(latestTarget.State.LockedUntil, now) {
		q.finalizeSplitFile(ctx, latestTarget)
	}
}

func (q *Queue) recoverFromOriginal(ctx context.Context, original loadedQueueFile) {
	if original.State.Status != fileStatusSplitting || original.State.SplitInto == "" {
		return
	}

	locked := cloneQueueFileState(original.State)
	lockedUntil := time.Now().UTC().Add(SplitTimeout)
	locked.LockedUntil = &lockedUntil

	ok, err := q.replaceFileIfNotChanged(ctx, original, locked)
	if err != nil || !ok {
		return
	}

	lockedRaw, err := encodeQueueFileState(locked)
	if err != nil {
		return
	}

	original = loadedQueueFile{
		ID:    original.ID,
		Key:   original.Key,
		Raw:   lockedRaw,
		State: locked,
	}

	target, exists, err := q.readFile(ctx, original.State.SplitInto)
	if err != nil {
		return
	}

	if !exists {
		q.resumeSplitFromStep3(ctx, original)
		return
	}

	q.resumeSplitFromStep4(ctx, original, target)
}

func (q *Queue) resumeSplitFromStep3(ctx context.Context, original loadedQueueFile) {
	if original.State.Status != fileStatusSplitting || original.State.SplitInto == "" {
		return
	}

	if !q.extendSplitLockIfNeeded(ctx, &original, nil) {
		return
	}

	movedCount := len(original.State.Jobs) / 2
	if movedCount == 0 {
		q.releaseOriginalAfterSplit(ctx, original, 0)
		return
	}

	newFileState := queueFileState{
		Status:      fileStatusSplitting,
		SplitFrom:   original.ID,
		LockedUntil: cloneTimePtr(original.State.LockedUntil),
		Jobs:        cloneJobs(original.State.Jobs[:movedCount]),
	}

	err := q.createFile(ctx, original.State.SplitInto, newFileState)
	if err != nil {
		return
	}

	newRaw, err := encodeQueueFileState(newFileState)
	if err != nil {
		return
	}

	newFile := loadedQueueFile{
		ID:    original.State.SplitInto,
		Key:   q.fileKey(original.State.SplitInto),
		Raw:   newRaw,
		State: newFileState,
	}

	if !q.extendSplitLockIfNeeded(ctx, &original, &newFile) {
		return
	}

	if !q.releaseOriginalAfterSplit(ctx, original, movedCount) {
		return
	}

	q.finalizeSplitFile(ctx, newFile)
}

func (q *Queue) resumeSplitFromStep4(ctx context.Context, original loadedQueueFile, newFile loadedQueueFile) {
	if original.State.Status != fileStatusSplitting || newFile.State.Status != fileStatusSplitting {
		return
	}

	if !q.extendSplitLockIfNeeded(ctx, &original, &newFile) {
		return
	}

	movedCount := len(original.State.Jobs) / 2
	if !q.releaseOriginalAfterSplit(ctx, original, movedCount) {
		return
	}

	q.finalizeSplitFile(ctx, newFile)
}

func (q *Queue) extendSplitLockIfNeeded(ctx context.Context, first *loadedQueueFile, second *loadedQueueFile) bool {
	if first.State.LockedUntil == nil {
		return true
	}

	if time.Until(*first.State.LockedUntil) > SplitTimeout/2 {
		return true
	}

	newLockedUntil := time.Now().UTC().Add(SplitTimeout)

	updatedFirst := cloneQueueFileState(first.State)
	updatedFirst.LockedUntil = &newLockedUntil

	ok, err := q.replaceFileIfNotChanged(ctx, *first, updatedFirst)
	if err != nil || !ok {
		return false
	}

	firstRaw, err := encodeQueueFileState(updatedFirst)
	if err != nil {
		return false
	}

	first.Raw = firstRaw
	first.State = updatedFirst

	if second == nil {
		return true
	}

	updatedSecond := cloneQueueFileState(second.State)
	updatedSecond.LockedUntil = &newLockedUntil

	ok, err = q.replaceFileIfNotChanged(ctx, *second, updatedSecond)
	if err != nil || !ok {
		return false
	}

	secondRaw, err := encodeQueueFileState(updatedSecond)
	if err != nil {
		return false
	}

	second.Raw = secondRaw
	second.State = updatedSecond

	return true
}

func (q *Queue) releaseOriginalAfterSplit(ctx context.Context, original loadedQueueFile, movedCount int) bool {
	if movedCount < 0 {
		movedCount = 0
	}
	if movedCount > len(original.State.Jobs) {
		movedCount = len(original.State.Jobs)
	}

	next := cloneQueueFileState(original.State)
	next.Status = fileStatusActive
	next.SplitInto = ""
	next.LockedUntil = nil
	next.Jobs = cloneJobs(original.State.Jobs[movedCount:])

	ok, err := q.replaceFileIfNotChanged(ctx, original, next)
	return err == nil && ok
}

func (q *Queue) finalizeSplitFile(ctx context.Context, splitFile loadedQueueFile) {
	next := cloneQueueFileState(splitFile.State)
	next.Status = fileStatusActive
	next.SplitFrom = ""
	next.LockedUntil = nil

	q.replaceFileIfNotChanged(ctx, splitFile, next)
}
