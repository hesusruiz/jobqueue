// Package jobqueue implements a simple SQLite-backed job queue.
package jobqueue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Queue struct {
	DB  *sql.DB
	Now func() time.Time
}

type Job struct {
	ID          int64
	Queue       string
	Type        string
	PayloadRaw  []byte // raw JSON
	Attempts    int
	MaxAttempts int
	LockToken   string
}

// New creates a Queue using an existing *sql.DB.
func New(db *sql.DB) *Queue {
	return &Queue{
		DB:  db,
		Now: time.Now,
	}
}

// EnsureSchema creates the jobs table and indexes if they do not exist.
func (q *Queue) EnsureSchema(ctx context.Context) error {
	stmts := []string{`
CREATE TABLE IF NOT EXISTS jobs (
	id              INTEGER PRIMARY KEY,
	queue           TEXT NOT NULL,
	type            TEXT NOT NULL,
	payload         BLOB NOT NULL,
	priority        INTEGER NOT NULL DEFAULT 0,
	created_at      INTEGER NOT NULL,
	scheduled_at    INTEGER NOT NULL,
	attempts        INTEGER NOT NULL DEFAULT 0,
	max_attempts    INTEGER NOT NULL DEFAULT 25,
	locked_at       INTEGER,
	lock_token      TEXT,
	done_at         INTEGER,
	error           TEXT
);`, `
CREATE INDEX IF NOT EXISTS jobs_ready_idx
    ON jobs (queue, done_at, locked_at, scheduled_at, priority, id);`, `
CREATE INDEX IF NOT EXISTS jobs_lock_token_idx
    ON jobs (lock_token);`,
	}

	for _, s := range stmts {
		if _, err := q.DB.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// EnqueueTx inserts a job inside an existing transaction and returns its ID.
func (q *Queue) EnqueueTx(
	ctx context.Context,
	tx *sql.Tx,
	queue, typ string,
	payload any,
	priority int,
	runAt time.Time,
	maxAttempts int,
) (int64, error) {

	if q.Now == nil {
		q.Now = time.Now
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}

	var id int64
	err = tx.QueryRowContext(ctx, `
        INSERT INTO jobs (
            queue, type, payload,
            priority, created_at, scheduled_at, max_attempts
        ) VALUES (?, ?, jsonb(?), ?, ?, ?, ?)
        RETURNING id
    `,
		queue, typ, payloadJSON,
		priority,
		q.Now().Unix(),
		runAt.Unix(),
		maxAttempts,
	).Scan(&id)

	return id, err
}

// Dequeue locks a single ready job from the given queue.
// Returns nil if no job is available.
func (q *Queue) Dequeue(ctx context.Context, queue string) (*Job, error) {
	if q.Now == nil {
		q.Now = time.Now
	}

	tx, err := q.DB.BeginTx(ctx, nil) // IMMEDIATE is already configured in your DSN
	if err != nil {
		return nil, err
	}

	now := q.Now().Unix()
	token := randomToken() // implement as you like (uuid, random string, etc.)

	res, err := tx.ExecContext(ctx, `
        UPDATE jobs
        SET locked_at = ?, lock_token = ?
        WHERE id = (
            SELECT id
            FROM jobs
            WHERE
                queue = ?
                AND done_at IS NULL
                AND locked_at IS NULL
                AND scheduled_at <= ?
            ORDER BY priority ASC, id ASC
            LIMIT 1
        )
    `, now, token, queue, now)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		tx.Rollback()
		return nil, nil
	}

	var j Job
	err = tx.QueryRowContext(ctx, `
        SELECT id, queue, type, json(payload), attempts, max_attempts
        FROM jobs
        WHERE lock_token = ?
    `, token).Scan(
		&j.ID,
		&j.Queue,
		&j.Type,
		&j.PayloadRaw,
		&j.Attempts,
		&j.MaxAttempts,
	)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	j.LockToken = token

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &j, nil
}

// Ack marks a locked job as done.
func (q *Queue) Ack(ctx context.Context, lockToken string) error {
	_, err := q.DB.ExecContext(ctx, `
        UPDATE jobs
        SET done_at = ?
        WHERE lock_token = ?
    `, q.Now().Unix(), lockToken)
	return err
}

// Retry re-schedules a locked job, incrementing attempts.
// If attempts exceed max_attempts, it marks the job as done with error.
func (q *Queue) Retry(ctx context.Context, lockToken string, delay time.Duration, errMsg string) error {
	if q.Now == nil {
		q.Now = time.Now
	}

	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	var attempts, maxAttempts int
	err = tx.QueryRowContext(ctx, `
        SELECT attempts, max_attempts
        FROM jobs
        WHERE lock_token = ?
    `, lockToken).Scan(&attempts, &maxAttempts)
	if err != nil {
		tx.Rollback()
		return err
	}

	now := q.Now().Unix()

	if attempts+1 >= maxAttempts {
		_, err = tx.ExecContext(ctx, `
            UPDATE jobs
            SET done_at = ?, error = ?
            WHERE lock_token = ?
        `, now, errMsg, lockToken)
	} else {
		_, err = tx.ExecContext(ctx, `
            UPDATE jobs
            SET
                attempts     = attempts + 1,
                locked_at    = NULL,
                lock_token   = NULL,
                scheduled_at = ?
            WHERE lock_token = ?
        `, now+int64(delay.Seconds()), lockToken)
	}

	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func randomToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
