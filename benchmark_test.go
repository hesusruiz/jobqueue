package jobqueue

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// setupBenchDB creates an isolated SQLite database in a temp directory,
// sets WAL mode, busy timeout, and immediate txlock via DSN,
// initializes the schema, and returns the DB and Queue for benchmarking.
func setupBenchDB(b *testing.B) (*sql.DB, *Queue) {
	b.Helper()

	dbPath := filepath.Join(b.TempDir(), "jobqueue_bench.db")
	dsn := fmt.Sprintf("%s?_busy_timeout=5000&_journal_mode=WAL&_txlock=immediate", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatalf("failed to open sqlite db: %v", err)
	}

	b.Cleanup(func() {
		_ = db.Close()
	})

	q := New(db)
	ctx := context.Background()
	if err := q.EnsureSchema(ctx); err != nil {
		b.Fatalf("EnsureSchema failed: %v", err)
	}

	return db, q
}

// BenchmarkEnqueue measures performance of enqueuing individual jobs (1 transaction per job).
func BenchmarkEnqueue(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345, "action": "send_notification"}
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := q.DB.BeginTx(ctx, nil)
		if err != nil {
			b.Fatalf("BeginTx failed: %v", err)
		}
		_, err = q.EnqueueTx(ctx, tx, "bench_queue", "notification", payload, 0, now, 3)
		if err != nil {
			b.Fatalf("EnqueueTx failed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkBatchEnqueue measures performance of enqueuing batches of jobs in a single transaction.
func BenchmarkBatchEnqueue(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345, "action": "batch_item"}
	now := time.Now()
	const batchSize = 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := q.DB.BeginTx(ctx, nil)
		if err != nil {
			b.Fatalf("BeginTx failed: %v", err)
		}
		for j := 0; j < batchSize; j++ {
			_, err = q.EnqueueTx(ctx, tx, "batch_queue", "item", payload, 0, now, 3)
			if err != nil {
				b.Fatalf("EnqueueTx failed: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkDequeue measures performance of dequeuing and locking ready jobs.
func BenchmarkDequeue(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345}
	now := time.Now()

	// Pre-enqueue b.N jobs inside one transaction for quick setup
	b.StopTimer()
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		b.Fatalf("BeginTx failed: %v", err)
	}
	for i := 0; i < b.N; i++ {
		_, err = q.EnqueueTx(ctx, tx, "bench_dequeue", "item", payload, 0, now, 3)
		if err != nil {
			b.Fatalf("EnqueueTx failed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		job, err := q.Dequeue(ctx, "bench_dequeue")
		if err != nil {
			b.Fatalf("Dequeue failed at %d: %v", i, err)
		}
		if job == nil {
			b.Fatalf("expected job at iteration %d, got nil", i)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkAck measures performance of acknowledging locked jobs.
func BenchmarkAck(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345}
	now := time.Now()

	// Pre-enqueue and dequeue b.N jobs
	b.StopTimer()
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		b.Fatalf("BeginTx failed: %v", err)
	}
	for i := 0; i < b.N; i++ {
		_, err = q.EnqueueTx(ctx, tx, "bench_ack", "item", payload, 0, now, 3)
		if err != nil {
			b.Fatalf("EnqueueTx failed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}

	tokens := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		job, err := q.Dequeue(ctx, "bench_ack")
		if err != nil || job == nil {
			b.Fatalf("Dequeue failed at %d: %v", i, err)
		}
		tokens[i] = job.LockToken
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := q.Ack(ctx, tokens[i]); err != nil {
			b.Fatalf("Ack failed at %d: %v", i, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkRetry measures performance of retrying and rescheduling a locked job.
func BenchmarkRetry(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345}
	now := time.Now()

	b.StopTimer()
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		b.Fatalf("BeginTx failed: %v", err)
	}
	for i := 0; i < b.N; i++ {
		_, err = q.EnqueueTx(ctx, tx, "bench_retry", "item", payload, 0, now, 1000)
		if err != nil {
			b.Fatalf("EnqueueTx failed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}

	tokens := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		job, err := q.Dequeue(ctx, "bench_retry")
		if err != nil || job == nil {
			b.Fatalf("Dequeue failed at %d: %v", i, err)
		}
		tokens[i] = job.LockToken
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := q.Retry(ctx, tokens[i], 10*time.Second, "transient error"); err != nil {
			b.Fatalf("Retry failed at %d: %v", i, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkFullCycle measures the full single-worker lifecycle: Enqueue -> Dequeue -> Ack.
func BenchmarkFullCycle(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345, "action": "process"}
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := q.DB.BeginTx(ctx, nil)
		if err != nil {
			b.Fatalf("BeginTx failed: %v", err)
		}
		_, err = q.EnqueueTx(ctx, tx, "bench_cycle", "cycle_job", payload, 0, now, 3)
		if err != nil {
			b.Fatalf("EnqueueTx failed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}

		job, err := q.Dequeue(ctx, "bench_cycle")
		if err != nil || job == nil {
			b.Fatalf("Dequeue failed: %v", err)
		}

		if err := q.Ack(ctx, job.LockToken); err != nil {
			b.Fatalf("Ack failed: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

// BenchmarkParallelFullCycle measures throughput across concurrent worker goroutines.
func BenchmarkParallelFullCycle(b *testing.B) {
	_, q := setupBenchDB(b)
	ctx := context.Background()
	payload := map[string]any{"user_id": 12345, "action": "process"}
	now := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := q.DB.BeginTx(ctx, nil)
			if err != nil {
				b.Fatalf("BeginTx failed: %v", err)
			}
			_, err = q.EnqueueTx(ctx, tx, "bench_par", "par_job", payload, 0, now, 3)
			if err != nil {
				b.Fatalf("EnqueueTx failed: %v", err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatalf("Commit failed: %v", err)
			}

			job, err := q.Dequeue(ctx, "bench_par")
			if err != nil {
				b.Fatalf("Dequeue failed: %v", err)
			}
			if job != nil {
				if err := q.Ack(ctx, job.LockToken); err != nil {
					b.Fatalf("Ack failed: %v", err)
				}
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}
