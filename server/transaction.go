// Copyright 2019 Masahiro Sano
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	uuidpkg "github.com/google/uuid"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
)

type databaseReader interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type databaseWriter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Commit() error
	Rollback() error
}

var (
	ErrNotStartedTransaction   = fmt.Errorf("transaction is not started")
	ErrNotAvailableTransaction = fmt.Errorf("transaction is not available")
	ErrInvalidatedTransaction  = fmt.Errorf("transaction is invalidated")
)

type transactionMode int

const (
	txReadOnly       transactionMode = 1
	txReadWrite      transactionMode = 2
	txPartitionedDML transactionMode = 3
)

type TransactionStatus int

const (
	TransactionActive      TransactionStatus = 1
	TransactionInvalidated TransactionStatus = 2
	TransactionCommited    TransactionStatus = 3
	TransactionRollbacked  TransactionStatus = 4
	TransactionAborted     TransactionStatus = 5
)

func (s TransactionStatus) String() string {
	switch s {
	case TransactionActive:
		return "ACTIVE"
	case TransactionInvalidated:
		return "INVALIDATED"
	case TransactionCommited:
		return "COMMITED"
	case TransactionRollbacked:
		return "ROLLBACKED"
	case TransactionAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

type transaction struct {
	id        []byte
	name      string
	session   *session
	single    bool
	mode      transactionMode
	status    int32 // for atomic operation
	createdAt time.Time

	ctx    context.Context
	cancel func()

	mu    sync.RWMutex
	tx    *sql.Tx
	close func(*transaction, *sql.Tx) // database.endTransaction()
}

func (tx *transaction) Context() context.Context {
	return tx.ctx
}

func (tx *transaction) Equals(t *transaction) bool {
	if tx == nil || t == nil {
		return false
	}

	if bytes.Equal(t.ID(), tx.ID()) {
		return true
	}

	return false
}

func (tx *transaction) ID() []byte {
	return tx.id
}

func (tx *transaction) Name() string {
	if tx == nil {
		return "<nil>"
	}
	return tx.name
}

func (tx *transaction) Proto() *spannerpb.Transaction {
	return &spannerpb.Transaction{
		Id: tx.id,
	}
}

func (tx *transaction) Status() TransactionStatus {
	return TransactionStatus(atomic.LoadInt32(&tx.status))
}

func (tx *transaction) ReadWrite() bool {
	return tx.mode == txReadWrite
}

func (tx *transaction) Available() bool {
	return tx.Status() == TransactionActive
}

func (tx *transaction) Invalidated() bool {
	st := tx.Status()
	return st == TransactionInvalidated || st == TransactionAborted
}

func (tx *transaction) SingleUse() bool {
	return tx.single
}

func (tx *transaction) Done(status TransactionStatus) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// skip if the transaction is done already
	if tx.Status() != TransactionActive {
		return
	}

	Debugf("[%s] transaction.Done %s\n", tx.Name(), status)
	atomic.StoreInt32(&tx.status, int32(status))

	if tx.close != nil {
		tx.close(tx, tx.tx)
		tx.close = nil
	}

	// Cancling transaction context is very unstable.
	// Call cancel after explicitly stop the transaction.
	tx.cancel()
}

func (tx *transaction) SetTransaction(dbtx *sql.Tx, closer func(*transaction, *sql.Tx)) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.tx != nil {
		return fmt.Errorf("transaction already started")
	}

	tx.tx = dbtx
	tx.close = closer

	return nil
}

func (tx *transaction) WriteTransaction(fn func(databaseWriter) error) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] transaction.WriteTransaction", tx.Name())()
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.Available() {
		return ErrNotAvailableTransaction
	}

	if tx.tx == nil {
		return ErrNotStartedTransaction
	}

	return fn(tx.tx)
}

func (tx *transaction) ReadTransaction(fn func(context.Context, databaseReader) error) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] transaction.ReadTransaction", tx.Name())()
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.Available() {
		return ErrNotAvailableTransaction
	}

	if tx.tx == nil {
		return ErrNotStartedTransaction
	}

	return fn(tx.ctx, tx.tx)
}

func newTransaction(s *session, mode transactionMode, single bool) *transaction {
	id := uuidpkg.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	return &transaction{
		id:        []byte(id),
		name:      id,
		session:   s,
		single:    single,
		mode:      mode,
		status:    int32(TransactionActive),
		createdAt: time.Now(),

		ctx:    ctx,
		cancel: cancel,
	}
}
