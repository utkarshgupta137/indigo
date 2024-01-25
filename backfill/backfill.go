package backfill

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

// Job is an interface for a backfill job
type Job interface {
	Repo() string
	State() string
	Rev() string
	SetState(ctx context.Context, state string) error
	SetRev(ctx context.Context, rev string) error
	RetryCount() int

	BufferOps(ctx context.Context, since *string, rev string, ops []*bufferedOp) (bool, error)
	// FlushBufferedOps calls the given callback for each buffered operation
	// Once done it clears the buffer and marks the job as "complete"
	// Allowing the Job interface to abstract away the details of how buffered
	// operations are stored and/or locked
	FlushBufferedOps(ctx context.Context, cb func(kind, rev, path string, rec *[]byte, cid *cid.Cid) error) error

	ClearBufferedOps(ctx context.Context) error
}

// Store is an interface for a backfill store which holds Jobs
type Store interface {
	// BufferOp buffers an operation for a job and returns true if the operation was buffered
	// If the operation was not buffered, it returns false and an error (ErrJobNotFound or ErrJobComplete)
	GetJob(ctx context.Context, repo string) (Job, error)
	GetNextEnqueuedJob(ctx context.Context) (Job, error)
	UpdateRev(ctx context.Context, repo, rev string) error

	EnqueueJob(ctx context.Context, repo string) error
}

// Backfiller is a struct which handles backfilling a repo
type Backfiller struct {
	Name               string
	HandleCreateRecord func(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error
	HandleUpdateRecord func(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error
	HandleDeleteRecord func(ctx context.Context, repo string, rev string, path string) error
	Store              Store

	// Number of backfills to process in parallel
	ParallelBackfills int
	// Number of records to process in parallel for each backfill
	ParallelRecordCreates int
	// Prefix match for records to backfill i.e. app.bsky.feed.app/
	// If empty, all records will be backfilled
	NSIDFilter   string
	CheckoutPath string

	syncLimiter *rate.Limiter

	magicHeaderKey string
	magicHeaderVal string

	stop chan chan struct{}
}

var (
	// StateEnqueued is the state of a backfill job when it is first created
	StateEnqueued = "enqueued"
	// StateInProgress is the state of a backfill job when it is being processed
	StateInProgress = "in_progress"
	// StateComplete is the state of a backfill job when it has been processed
	StateComplete = "complete"
)

// ErrJobComplete is returned when trying to buffer an op for a job that is complete
var ErrJobComplete = errors.New("job is complete")

// ErrJobNotFound is returned when trying to buffer an op for a job that doesn't exist
var ErrJobNotFound = errors.New("job not found")

// ErrEventGap is returned when an event is received with a since that doesn't match the current rev
var ErrEventGap = fmt.Errorf("buffered event revs did not line up")

// ErrAlreadyProcessed is returned when attempting to buffer an event that has already been accounted for (rev older than current)
var ErrAlreadyProcessed = fmt.Errorf("event already accounted for")

var tracer = otel.Tracer("backfiller")

type BackfillOptions struct {
	ParallelBackfills     int
	ParallelRecordCreates int
	NSIDFilter            string
	SyncRequestsPerSecond int
	CheckoutPath          string
}

func DefaultBackfillOptions() *BackfillOptions {
	return &BackfillOptions{
		ParallelBackfills:     10,
		ParallelRecordCreates: 100,
		NSIDFilter:            "",
		SyncRequestsPerSecond: 2,
		CheckoutPath:          "https://bsky.social/xrpc/com.atproto.sync.getRepo",
	}
}

// NewBackfiller creates a new Backfiller
func NewBackfiller(
	name string,
	store Store,
	handleCreate func(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error,
	handleUpdate func(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error,
	handleDelete func(ctx context.Context, repo string, rev string, path string) error,
	opts *BackfillOptions,
) *Backfiller {
	if opts == nil {
		opts = DefaultBackfillOptions()
	}
	return &Backfiller{
		Name:                  name,
		Store:                 store,
		HandleCreateRecord:    handleCreate,
		HandleUpdateRecord:    handleUpdate,
		HandleDeleteRecord:    handleDelete,
		ParallelBackfills:     opts.ParallelBackfills,
		ParallelRecordCreates: opts.ParallelRecordCreates,
		NSIDFilter:            opts.NSIDFilter,
		syncLimiter:           rate.NewLimiter(rate.Limit(opts.SyncRequestsPerSecond), 1),
		CheckoutPath:          opts.CheckoutPath,
		stop:                  make(chan chan struct{}, 1),
	}
}

// Start starts the backfill processor routine
func (b *Backfiller) Start() {
	ctx := context.Background()

	log := slog.With("source", "backfiller", "name", b.Name)
	log.Info("starting backfill processor")

	sem := semaphore.NewWeighted(int64(b.ParallelBackfills))

	for {
		select {
		case stopped := <-b.stop:
			log.Info("stopping backfill processor")
			sem.Acquire(ctx, int64(b.ParallelBackfills))
			close(stopped)
			return
		default:
		}

		// Get the next job
		job, err := b.Store.GetNextEnqueuedJob(ctx)
		if err != nil {
			log.Error("failed to get next enqueued job", "error", err)
			time.Sleep(1 * time.Second)
			continue
		} else if job == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		log := log.With("repo", job.Repo())

		// Mark the backfill as "in progress"
		err = job.SetState(ctx, StateInProgress)
		if err != nil {
			log.Error("failed to set job state", "error", err)
			continue
		}

		sem.Acquire(ctx, 1)
		go func(j Job) {
			defer sem.Release(1)
			newState, err := b.BackfillRepo(ctx, j)
			if err != nil {
				log.Error("failed to backfill repo", "error", err)
			}
			if newState != "" {
				if sserr := j.SetState(ctx, newState); sserr != nil {
					log.Error("failed to set job state", "error", sserr)
				}

				if strings.HasPrefix(newState, "failed") {
					// Clear buffered ops
					if err := j.ClearBufferedOps(ctx); err != nil {
						log.Error("failed to clear buffered ops", "error", err)
					}
				}
			}
			backfillJobsProcessed.WithLabelValues(b.Name).Inc()
		}(job)
	}
}

// Stop stops the backfill processor
func (b *Backfiller) Stop(ctx context.Context) error {
	log := slog.With("source", "backfiller", "name", b.Name)
	log.Info("stopping backfill processor")
	stopped := make(chan struct{})
	b.stop <- stopped
	select {
	case <-stopped:
		log.Info("backfill processor stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FlushBuffer processes buffered operations for a job
func (b *Backfiller) FlushBuffer(ctx context.Context, job Job) int {
	ctx, span := tracer.Start(ctx, "FlushBuffer")
	defer span.End()
	log := slog.With("source", "backfiller_buffer_flush", "repo", job.Repo())

	processed := 0

	repo := job.Repo()

	// Flush buffered operations, clear the buffer, and mark the job as "complete"
	// Clearning and marking are handled by the job interface
	err := job.FlushBufferedOps(ctx, func(kind, rev, path string, rec *[]byte, cid *cid.Cid) error {
		switch repomgr.EventKind(kind) {
		case repomgr.EvtKindCreateRecord:
			err := b.HandleCreateRecord(ctx, repo, rev, path, rec, cid)
			if err != nil {
				log.Error("failed to handle create record", "error", err)
			}
		case repomgr.EvtKindUpdateRecord:
			err := b.HandleUpdateRecord(ctx, repo, rev, path, rec, cid)
			if err != nil {
				log.Error("failed to handle update record", "error", err)
			}
		case repomgr.EvtKindDeleteRecord:
			err := b.HandleDeleteRecord(ctx, repo, rev, path)
			if err != nil {
				log.Error("failed to handle delete record", "error", err)
			}
		}
		backfillOpsBuffered.WithLabelValues(b.Name).Dec()
		processed++
		return nil
	})
	if err != nil {
		log.Error("failed to flush buffered ops", "error", err)
		if errors.Is(err, ErrEventGap) {
			if sserr := job.SetState(ctx, StateEnqueued); sserr != nil {
				log.Error("failed to reset job state after failed buffer flush", "error", sserr)
			}
			// TODO: need to re-queue this job for later
			return processed
		}
	}

	// Mark the job as "complete"
	err = job.SetState(ctx, StateComplete)
	if err != nil {
		log.Error("failed to set job state", "error", err)
	}

	return processed
}

type recordQueueItem struct {
	recordPath string
	nodeCid    cid.Cid
}

type recordResult struct {
	recordPath string
	err        error
}

// BackfillRepo backfills a repo
func (b *Backfiller) BackfillRepo(ctx context.Context, job Job) (string, error) {
	ctx, span := tracer.Start(ctx, "BackfillRepo")
	defer span.End()

	start := time.Now()

	repoDid := job.Repo()

	log := slog.With("source", "backfiller_backfill_repo", "repo", repoDid)
	if job.RetryCount() > 0 {
		log = log.With("retry_count", job.RetryCount())
	}
	log.Info(fmt.Sprintf("processing backfill for %s", repoDid))

	url := fmt.Sprintf("%s?did=%s", b.CheckoutPath, repoDid)

	if job.Rev() != "" {
		url = url + fmt.Sprintf("&since=%s", job.Rev())
	}

	// GET and CAR decode the body
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   600 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		state := fmt.Sprintf("failed (create request: %s)", err.Error())
		return state, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", fmt.Sprintf("atproto-backfill-%s/0.0.1", b.Name))
	if b.magicHeaderKey != "" && b.magicHeaderVal != "" {
		req.Header.Set(b.magicHeaderKey, b.magicHeaderVal)
	}

	b.syncLimiter.Wait(ctx)

	resp, err := client.Do(req)
	if err != nil {
		state := fmt.Sprintf("failed (do request: %s)", err.Error())
		return state, fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		reason := "unknown error"
		if resp.StatusCode == http.StatusBadRequest {
			reason = "repo not found"
		} else {
			reason = resp.Status
		}
		state := fmt.Sprintf("failed (%s)", reason)
		return state, fmt.Errorf("failed to get repo: %s", reason)
	}

	instrumentedReader := instrumentedReader{
		source:  resp.Body,
		counter: backfillBytesProcessed.WithLabelValues(b.Name),
	}

	defer instrumentedReader.Close()

	r, err := repo.ReadRepoFromCar(ctx, instrumentedReader)
	if err != nil {
		state := "failed (couldn't read repo CAR from response body)"
		return state, fmt.Errorf("failed to read repo from car: %w", err)
	}

	numRecords := 0
	numRoutines := b.ParallelRecordCreates
	recordQueue := make(chan recordQueueItem, numRoutines)
	recordResults := make(chan recordResult, numRoutines)

	// Producer routine
	go func() {
		defer close(recordQueue)
		if err := r.ForEach(ctx, b.NSIDFilter, func(recordPath string, nodeCid cid.Cid) error {
			numRecords++
			recordQueue <- recordQueueItem{recordPath: recordPath, nodeCid: nodeCid}
			return nil
		}); err != nil {
			log.Error("failed to iterate records in repo", "err", err)
		}
	}()

	rev := r.SignedCommit().Rev

	// Consumer routines
	wg := sync.WaitGroup{}
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range recordQueue {
				blk, err := r.Blockstore().Get(ctx, item.nodeCid)
				if err != nil {
					recordResults <- recordResult{recordPath: item.recordPath, err: fmt.Errorf("failed to get blocks for record: %w", err)}
					continue
				}

				raw := blk.RawData()

				err = b.HandleCreateRecord(ctx, repoDid, rev, item.recordPath, &raw, &item.nodeCid)
				if err != nil {
					recordResults <- recordResult{recordPath: item.recordPath, err: fmt.Errorf("failed to handle create record: %w", err)}
					continue
				}

				backfillRecordsProcessed.WithLabelValues(b.Name).Inc()
				recordResults <- recordResult{recordPath: item.recordPath, err: err}
			}
		}()
	}

	resultWG := sync.WaitGroup{}
	resultWG.Add(1)
	// Handle results
	go func() {
		defer resultWG.Done()
		for result := range recordResults {
			if result.err != nil {
				log.Error("Error processing record", "record", result.recordPath, "error", result.err)
			}
		}
	}()

	wg.Wait()
	close(recordResults)
	resultWG.Wait()

	if err := job.SetRev(ctx, r.SignedCommit().Rev); err != nil {
		log.Error("failed to update rev after backfilling repo", "err", err)
	}

	// Process buffered operations, marking the job as "complete" when done
	numProcessed := b.FlushBuffer(ctx, job)

	log.Info("backfill complete",
		"buffered_records_processed", numProcessed,
		"records_backfilled", numRecords,
		"duration", time.Since(start),
	)

	return StateComplete, nil
}

const trust = true

func (bf *Backfiller) getRecord(ctx context.Context, r *repo.Repo, op *atproto.SyncSubscribeRepos_RepoOp) (cid.Cid, *[]byte, error) {
	if trust {
		if op.Cid == nil {
			return cid.Undef, nil, fmt.Errorf("op had no cid set")
		}

		c := (cid.Cid)(*op.Cid)
		blk, err := r.Blockstore().Get(ctx, c)
		if err != nil {
			return cid.Undef, nil, err
		}

		raw := blk.RawData()

		return c, &raw, nil
	} else {
		return r.GetRecordBytes(ctx, op.Path)
	}
}

func (bf *Backfiller) HandleEvent(ctx context.Context, evt *atproto.SyncSubscribeRepos_Commit) error {
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		return fmt.Errorf("failed to read event repo: %w", err)
	}

	var ops []*bufferedOp
	for _, op := range evt.Ops {
		switch op.Action {
		case "create", "update":
			cc, rec, err := bf.getRecord(ctx, r, op)
			if err != nil {
				return fmt.Errorf("getting record failed (%s,%s): %w", op.Action, op.Path, err)
			}

			ops = append(ops, &bufferedOp{
				kind: op.Action,
				path: op.Path,
				rec:  rec,
				cid:  &cc,
			})
		case "delete":
			ops = append(ops, &bufferedOp{
				kind: op.Action,
				path: op.Path,
			})
		default:
			return fmt.Errorf("invalid op action: %q", op.Action)
		}
	}

	buffered, err := bf.BufferOps(ctx, evt.Repo, evt.Since, evt.Rev, ops)
	if err != nil {
		if errors.Is(err, ErrAlreadyProcessed) {
			return nil
		}
		return fmt.Errorf("buffer ops failed: %w", err)
	}

	if buffered {
		return nil
	}

	for _, op := range ops {
		switch op.kind {
		case "create":
			if err := bf.HandleCreateRecord(ctx, evt.Repo, evt.Rev, op.path, op.rec, op.cid); err != nil {
				return fmt.Errorf("create record failed: %w", err)
			}
		case "update":
			if err := bf.HandleUpdateRecord(ctx, evt.Repo, evt.Rev, op.path, op.rec, op.cid); err != nil {
				return fmt.Errorf("update record failed: %w", err)
			}
		case "delete":
			if err := bf.HandleDeleteRecord(ctx, evt.Repo, evt.Rev, op.path); err != nil {
				return fmt.Errorf("delete record failed: %w", err)
			}
		}
	}

	if err := bf.Store.UpdateRev(ctx, evt.Repo, evt.Rev); err != nil {
		return fmt.Errorf("failed to update rev: %w", err)
	}

	return nil
}

func (bf *Backfiller) BufferOp(ctx context.Context, repo string, since *string, rev, kind, path string, rec *[]byte, cid *cid.Cid) (bool, error) {
	return bf.BufferOps(ctx, repo, since, rev, []*bufferedOp{{
		path: path,
		kind: kind,
		rec:  rec,
		cid:  cid,
	}})
}

func (bf *Backfiller) BufferOps(ctx context.Context, repo string, since *string, rev string, ops []*bufferedOp) (bool, error) {
	j, err := bf.Store.GetJob(ctx, repo)
	if err != nil {
		if !errors.Is(err, ErrJobNotFound) {
			return false, err
		}
		if qerr := bf.Store.EnqueueJob(ctx, repo); qerr != nil {
			return false, fmt.Errorf("failed to enqueue job for unknown repo: %w", qerr)
		}

		nj, err := bf.Store.GetJob(ctx, repo)
		if err != nil {
			return false, err
		}

		j = nj
	}

	return j.BufferOps(ctx, since, rev, ops)
}

// MaxRetries is the maximum number of times to retry a backfill job
var MaxRetries = 10

func computeExponentialBackoff(attempt int) time.Duration {
	return time.Duration(1<<uint(attempt)) * 10 * time.Second
}
