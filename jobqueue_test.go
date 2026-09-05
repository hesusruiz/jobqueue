package jobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// setupTestDB creates an isolated SQLite database in a temp directory,
// sets WAL mode, busy timeout, and immediate txlock via DSN,
// initializes the schema, and returns the DB and Queue.
func setupTestDB(t *testing.T) (*sql.DB, *Queue) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "jobqueue_test.db")
	dsn := fmt.Sprintf("%s?_busy_timeout=5000&_journal_mode=WAL&_txlock=immediate", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	q := New(db)
	ctx := context.Background()
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	return db, q
}

// enqueueHelper is a convenience helper that opens a transaction and enqueues a job.
func enqueueHelper(
	t *testing.T,
	ctx context.Context,
	q *Queue,
	queue, typ string,
	payload any,
	priority int,
	runAt time.Time,
	maxAttempts int,
) int64 {
	t.Helper()

	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	id, err := q.EnqueueTx(ctx, tx, queue, typ, payload, priority, runAt, maxAttempts)
	if err != nil {
		t.Fatalf("EnqueueTx failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	return id
}

func TestEnsureSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "schema_test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	q := New(db)
	ctx := context.Background()

	// First invocation should create tables and indexes
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("first EnsureSchema failed: %v", err)
	}

	// Verify table exists
	var tableName string
	err = db.QueryRowContext(ctx, `
		SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'
	`).Scan(&tableName)
	if err != nil || tableName != "jobs" {
		t.Fatalf("expected jobs table to exist, got table=%q, err=%v", tableName, err)
	}

	// Verify indexes exist
	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='jobs'
	`)
	if err != nil {
		t.Fatalf("query indexes failed: %v", err)
	}
	defer rows.Close()

	foundIndexes := make(map[string]bool)
	for rows.Next() {
		var idxName string
		if err := rows.Scan(&idxName); err != nil {
			t.Fatalf("scan index name failed: %v", err)
		}
		foundIndexes[idxName] = true
	}
	if !foundIndexes["jobs_ready_idx"] {
		t.Errorf("expected index jobs_ready_idx to exist")
	}
	if !foundIndexes["jobs_lock_token_idx"] {
		t.Errorf("expected index jobs_lock_token_idx to exist")
	}

	// Idempotency: second invocation should succeed without error
	if err := q.EnsureSchema(ctx); err != nil {
		t.Fatalf("second EnsureSchema call should be idempotent, got error: %v", err)
	}
}

func TestEnqueueAndDequeue_Basic(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	q.Now = func() time.Time { return now }

	type emailPayload struct {
		Recipient string `json:"recipient"`
		Subject   string `json:"subject"`
	}

	inputPayload := emailPayload{
		Recipient: "alice@example.com",
		Subject:   "Welcome to the queue",
	}

	jobID := enqueueHelper(t, ctx, q, "emails", "send_welcome", inputPayload, 1, now, 5)
	if jobID <= 0 {
		t.Fatalf("expected positive job ID, got %d", jobID)
	}

	job, err := q.Dequeue(ctx, "emails")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if job == nil {
		t.Fatal("expected job to be returned, got nil")
	}

	if job.ID != jobID {
		t.Errorf("expected ID %d, got %d", jobID, job.ID)
	}
	if job.Queue != "emails" {
		t.Errorf("expected Queue 'emails', got %q", job.Queue)
	}
	if job.Type != "send_welcome" {
		t.Errorf("expected Type 'send_welcome', got %q", job.Type)
	}
	if job.Attempts != 0 {
		t.Errorf("expected Attempts 0, got %d", job.Attempts)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("expected MaxAttempts 5, got %d", job.MaxAttempts)
	}
	if len(job.LockToken) != 32 {
		t.Errorf("expected 32-char hex lock token, got %q (len %d)", job.LockToken, len(job.LockToken))
	}

	var gotPayload emailPayload
	if err := json.Unmarshal(job.PayloadRaw, &gotPayload); err != nil {
		t.Fatalf("failed to unmarshal payload %s: %v", string(job.PayloadRaw), err)
	}
	if gotPayload != inputPayload {
		t.Errorf("payload mismatch: expected %+v, got %+v", inputPayload, gotPayload)
	}
}

func TestDequeue_EmptyOrUnmatchedQueue(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	// Dequeue from completely empty queue
	job, err := q.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("unexpected error on empty queue: %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil job on empty queue, got %+v", job)
	}

	// Enqueue in "queue_a", try to dequeue from "queue_b"
	now := time.Now()
	_ = enqueueHelper(t, ctx, q, "queue_a", "task", "data", 0, now, 3)

	job, err = q.Dequeue(ctx, "queue_b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil job from different queue, got %+v", job)
	}
}

func TestDequeue_LocksJob(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	now := time.Now()
	_ = enqueueHelper(t, ctx, q, "tasks", "task", "data", 0, now, 3)

	// First dequeue should succeed and lock the job
	job1, err := q.Dequeue(ctx, "tasks")
	if err != nil {
		t.Fatalf("first Dequeue failed: %v", err)
	}
	if job1 == nil {
		t.Fatal("expected job1 to be non-nil")
	}

	// Second dequeue immediately should return nil because the only job is locked
	job2, err := q.Dequeue(ctx, "tasks")
	if err != nil {
		t.Fatalf("second Dequeue failed: %v", err)
	}
	if job2 != nil {
		t.Fatalf("expected job2 to be nil since job is locked, got %+v", job2)
	}
}

func TestAck(t *testing.T) {
	db, q := setupTestDB(t)
	ctx := context.Background()

	now := time.Date(2026, 9, 4, 15, 0, 0, 0, time.UTC)
	q.Now = func() time.Time { return now }

	jobID := enqueueHelper(t, ctx, q, "orders", "process", map[string]int{"order_id": 123}, 0, now, 3)

	job, err := q.Dequeue(ctx, "orders")
	if err != nil || job == nil {
		t.Fatalf("Dequeue failed: job=%+v, err=%v", job, err)
	}

	// Ack the job
	if err := q.Ack(ctx, job.LockToken); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// Verify in database that done_at is set
	var doneAt sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT done_at FROM jobs WHERE id = ?", jobID).Scan(&doneAt)
	if err != nil {
		t.Fatalf("query done_at failed: %v", err)
	}
	if !doneAt.Valid || doneAt.Int64 != now.Unix() {
		t.Errorf("expected done_at to be %d, got %v", now.Unix(), doneAt)
	}

	// Verify job cannot be dequeued again
	jobAgain, err := q.Dequeue(ctx, "orders")
	if err != nil {
		t.Fatalf("Dequeue after Ack returned error: %v", err)
	}
	if jobAgain != nil {
		t.Fatalf("expected nil job after Ack, got %+v", jobAgain)
	}
}

func TestRetry_RescheduleUnderMaxAttempts(t *testing.T) {
	db, q := setupTestDB(t)
	ctx := context.Background()

	t0 := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	currentTime := t0
	q.Now = func() time.Time { return currentTime }

	jobID := enqueueHelper(t, ctx, q, "sync", "sync_user", "payload", 0, t0, 3)

	// Attempt 1: Dequeue
	job, err := q.Dequeue(ctx, "sync")
	if err != nil || job == nil {
		t.Fatalf("first Dequeue failed: job=%+v, err=%v", job, err)
	}
	if job.Attempts != 0 {
		t.Errorf("expected initial attempts 0, got %d", job.Attempts)
	}

	// Retry with 30s delay
	delay := 30 * time.Second
	if err := q.Retry(ctx, job.LockToken, delay, "transient error 1"); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}

	// Check DB state directly
	var (
		attempts    int
		lockedAt    sql.NullInt64
		lockToken   sql.NullString
		scheduledAt int64
		doneAt      sql.NullInt64
		jobErr      sql.NullString
	)
	err = db.QueryRowContext(ctx, `
		SELECT attempts, locked_at, lock_token, scheduled_at, done_at, error
		FROM jobs WHERE id = ?
	`, jobID).Scan(&attempts, &lockedAt, &lockToken, &scheduledAt, &doneAt, &jobErr)
	if err != nil {
		t.Fatalf("failed to query job state: %v", err)
	}

	if attempts != 1 {
		t.Errorf("expected attempts=1, got %d", attempts)
	}
	if lockedAt.Valid {
		t.Errorf("expected locked_at to be NULL, got %v", lockedAt)
	}
	if lockToken.Valid {
		t.Errorf("expected lock_token to be NULL, got %v", lockToken)
	}
	expectedScheduled := t0.Unix() + 30
	if scheduledAt != expectedScheduled {
		t.Errorf("expected scheduled_at=%d, got %d", expectedScheduled, scheduledAt)
	}
	if doneAt.Valid {
		t.Errorf("expected done_at to be NULL, got %v", doneAt)
	}

	// At t0 + 10s: job should not be available yet
	currentTime = t0.Add(10 * time.Second)
	j, err := q.Dequeue(ctx, "sync")
	if err != nil {
		t.Fatalf("Dequeue error: %v", err)
	}
	if j != nil {
		t.Fatalf("job should not be ready before delay expires, got %+v", j)
	}

	// At t0 + 30s: job should be ready
	currentTime = t0.Add(30 * time.Second)
	j, err = q.Dequeue(ctx, "sync")
	if err != nil || j == nil {
		t.Fatalf("expected job to be ready after delay, got j=%+v, err=%v", j, err)
	}
	if j.Attempts != 1 {
		t.Errorf("expected Attempts=1 on second dequeue, got %d", j.Attempts)
	}
}

func TestRetry_ExceedMaxAttempts(t *testing.T) {
	db, q := setupTestDB(t)
	ctx := context.Background()

	t0 := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	currentTime := t0
	q.Now = func() time.Time { return currentTime }

	// maxAttempts = 2
	jobID := enqueueHelper(t, ctx, q, "reports", "gen_pdf", "data", 0, t0, 2)

	// Dequeue 1 (attempts=0)
	j1, err := q.Dequeue(ctx, "reports")
	if err != nil || j1 == nil {
		t.Fatalf("first Dequeue failed: j=%+v, err=%v", j1, err)
	}

	// First Retry: attempts becomes 1 (1 < 2, so rescheduled)
	if err := q.Retry(ctx, j1.LockToken, 5*time.Second, "failure 1"); err != nil {
		t.Fatalf("first Retry failed: %v", err)
	}

	currentTime = currentTime.Add(10 * time.Second)

	// Dequeue 2 (attempts=1)
	j2, err := q.Dequeue(ctx, "reports")
	if err != nil || j2 == nil {
		t.Fatalf("second Dequeue failed: j=%+v, err=%v", j2, err)
	}

	// Second Retry: attempts+1 (1+1=2) >= maxAttempts (2) -> marked done with error
	permErr := "permanent failure: bad input"
	if err := q.Retry(ctx, j2.LockToken, 5*time.Second, permErr); err != nil {
		t.Fatalf("second Retry failed: %v", err)
	}

	// Check DB state
	var doneAt sql.NullInt64
	var errMsg sql.NullString
	err = db.QueryRowContext(ctx, "SELECT done_at, error FROM jobs WHERE id = ?", jobID).Scan(&doneAt, &errMsg)
	if err != nil {
		t.Fatalf("failed to query job: %v", err)
	}

	if !doneAt.Valid || doneAt.Int64 != currentTime.Unix() {
		t.Errorf("expected done_at=%d, got %v", currentTime.Unix(), doneAt)
	}
	if !errMsg.Valid || errMsg.String != permErr {
		t.Errorf("expected error message %q, got %v", permErr, errMsg)
	}

	// Dequeue should return nil
	currentTime = currentTime.Add(1 * time.Hour)
	j3, err := q.Dequeue(ctx, "reports")
	if err != nil {
		t.Fatalf("Dequeue error: %v", err)
	}
	if j3 != nil {
		t.Fatalf("expected nil job after max attempts reached, got %+v", j3)
	}
}

func TestPriorityAndFIFOOrder(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	now := time.Now()

	// Priority ASC ordering: lowest priority number comes first
	// Equal priority: lowest ID (FIFO) comes first
	idHighPrio := enqueueHelper(t, ctx, q, "queue", "task_high", "prio 1", 1, now, 3)
	idMedPrio1 := enqueueHelper(t, ctx, q, "queue", "task_med_1", "prio 5", 5, now, 3)
	idMedPrio2 := enqueueHelper(t, ctx, q, "queue", "task_med_2", "prio 5", 5, now, 3)
	idLowPrio := enqueueHelper(t, ctx, q, "queue", "task_low", "prio 10", 10, now, 3)

	expectedOrder := []int64{idHighPrio, idMedPrio1, idMedPrio2, idLowPrio}
	for i, expectedID := range expectedOrder {
		job, err := q.Dequeue(ctx, "queue")
		if err != nil {
			t.Fatalf("dequeue step %d failed: %v", i, err)
		}
		if job == nil {
			t.Fatalf("dequeue step %d: expected job ID %d, got nil", i, expectedID)
		}
		if job.ID != expectedID {
			t.Errorf("dequeue step %d: expected job ID %d, got %d", i, expectedID, job.ID)
		}
	}

	// Next dequeue should be nil
	last, err := q.Dequeue(ctx, "queue")
	if err != nil {
		t.Fatalf("dequeue empty failed: %v", err)
	}
	if last != nil {
		t.Errorf("expected nil job, got %+v", last)
	}
}

func TestScheduledAt_FutureJob(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	t0 := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	currentTime := t0
	q.Now = func() time.Time { return currentTime }

	runAt := t0.Add(1 * time.Hour)
	jobID := enqueueHelper(t, ctx, q, "cron", "hourly_cleanup", "payload", 0, runAt, 3)

	// At t0: not ready
	j, err := q.Dequeue(ctx, "cron")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if j != nil {
		t.Fatalf("expected nil job before scheduled time, got %+v", j)
	}

	// At t0 + 30 minutes: still not ready
	currentTime = t0.Add(30 * time.Minute)
	j, err = q.Dequeue(ctx, "cron")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if j != nil {
		t.Fatalf("expected nil job at 30 min, got %+v", j)
	}

	// At t0 + 1 hour: ready!
	currentTime = runAt
	j, err = q.Dequeue(ctx, "cron")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if j == nil {
		t.Fatal("expected job to be ready at scheduled time, got nil")
	}
	if j.ID != jobID {
		t.Errorf("expected job ID %d, got %d", jobID, j.ID)
	}
}

func TestNilNowFallback(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	// Explicitly set Now to nil to test default time.Now fallback
	q.Now = nil

	jobID := enqueueHelper(t, ctx, q, "fallback", "type", "data", 0, time.Now(), 3)
	if jobID <= 0 {
		t.Fatalf("unexpected job ID: %d", jobID)
	}

	job, err := q.Dequeue(ctx, "fallback")
	if err != nil || job == nil {
		t.Fatalf("Dequeue with q.Now=nil failed: job=%+v, err=%v", job, err)
	}

	// Ack with q.Now=nil
	if err := q.Ack(ctx, job.LockToken); err != nil {
		t.Fatalf("Ack with q.Now=nil failed: %v", err)
	}

	// Test Retry with q.Now=nil
	q.Now = nil
	_ = enqueueHelper(t, ctx, q, "fallback2", "type", "data", 0, time.Now(), 3)
	job2, err := q.Dequeue(ctx, "fallback2")
	if err != nil || job2 == nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if err := q.Retry(ctx, job2.LockToken, 1*time.Second, "retry err"); err != nil {
		t.Fatalf("Retry with q.Now=nil failed: %v", err)
	}
}

func TestEnqueueTx_InvalidPayload(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	// Channel cannot be marshaled to JSON
	unmarshalable := make(chan int)
	_, err = q.EnqueueTx(ctx, tx, "bad", "bad_type", unmarshalable, 0, time.Now(), 3)
	if err == nil {
		t.Fatal("expected error when marshaling channel to JSON, got nil")
	}
}

func TestConcurrentDequeues(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	const numJobs = 30
	const numWorkers = 6

	now := time.Now()
	for i := 0; i < numJobs; i++ {
		enqueueHelper(t, ctx, q, "concurrent_queue", "work", map[string]int{"index": i}, 0, now, 3)
	}

	var mu sync.Mutex
	dequeuedIDs := make(map[int64]int)
	lockTokens := make(map[string]bool)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			for {
				job, err := q.Dequeue(ctx, "concurrent_queue")
				if err != nil {
					t.Errorf("worker Dequeue error: %v", err)
					return
				}
				if job == nil {
					// No more jobs available
					return
				}

				mu.Lock()
				dequeuedIDs[job.ID]++
				if lockTokens[job.LockToken] {
					t.Errorf("duplicate lock token: %s", job.LockToken)
				}
				lockTokens[job.LockToken] = true
				mu.Unlock()

				// Simulate quick processing and Ack
				if err := q.Ack(ctx, job.LockToken); err != nil {
					t.Errorf("worker Ack error: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	if len(dequeuedIDs) != numJobs {
		t.Fatalf("expected %d unique jobs dequeued, got %d", numJobs, len(dequeuedIDs))
	}

	for id, count := range dequeuedIDs {
		if count != 1 {
			t.Errorf("job %d was dequeued %d times (expected 1)", id, count)
		}
	}
}

func TestRetry_NonExistentLockToken(t *testing.T) {
	_, q := setupTestDB(t)
	ctx := context.Background()

	err := q.Retry(ctx, "nonexistent-token-123", 10*time.Second, "some error")
	if err == nil {
		t.Fatal("expected error when retrying non-existent lock token, got nil")
	}
}

func TestContextCanceled(t *testing.T) {
	_, q := setupTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// EnsureSchema with canceled context
	if err := q.EnsureSchema(ctx); err == nil {
		t.Errorf("expected error on EnsureSchema with canceled context, got nil")
	}

	// Dequeue with canceled context
	if _, err := q.Dequeue(ctx, "any"); err == nil {
		t.Errorf("expected error on Dequeue with canceled context, got nil")
	}

	// Retry with canceled context
	if err := q.Retry(ctx, "token", 1*time.Second, "err"); err == nil {
		t.Errorf("expected error on Retry with canceled context, got nil")
	}
}
