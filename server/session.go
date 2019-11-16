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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	uuidpkg "github.com/google/uuid"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateSessionName(sessionName string) bool {
	parts := strings.Split(sessionName, "/")
	if len(parts) != 8 {
		return false
	}
	if parts[0] != "projects" {
		return false
	}
	if parts[2] != "instances" {
		return false
	}
	if parts[4] != "databases" {
		return false
	}
	if parts[6] != "sessions" {
		return false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" || parts[7] == "" {
		return false
	}
	return true
}

type session struct {
	id        string
	dbName    string
	database  *database
	name      string
	createdAt time.Time
	lastUse   time.Time

	mu                 sync.Mutex
	transactions       map[string]*transaction
	activeTransactions []*transaction
}

func (s *session) Name() string {
	return s.name
}

func (s *session) Proto() *spannerpb.Session {
	ctime, _ := ptypes.TimestampProto(s.createdAt)
	last, _ := ptypes.TimestampProto(s.lastUse)
	return &spannerpb.Session{
		Name:                   s.name,
		CreateTime:             ctime,
		ApproximateLastUseTime: last,
	}
}

func (s *session) createTransaction(txMode transactionMode) (*transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < 3; i++ {
		tx := newTransaction(s, txMode)
		if _, ok := s.transactions[tx.Name()]; ok {
			continue
		}
		s.transactions[tx.Name()] = tx

		// keep only 32 transactions in a session
		s.activeTransactions = append(s.activeTransactions, tx)
		if len(s.activeTransactions) > 32 {
			oldtx := s.activeTransactions[0]
			s.activeTransactions = s.activeTransactions[1:]
			oldtx.Done(TransactionInvalidated)
		}
		return tx, nil
	}

	return nil, fmt.Errorf("failed to create transaction")
}

func (s *session) GetTransaction(id []byte) (*transaction, bool) {
	s.mu.Lock()
	tx, ok := s.transactions[string(id)]
	s.mu.Unlock()
	return tx, ok
}

func (s *session) GetTransactionBySelector(txsel *spannerpb.TransactionSelector) (*transaction, error) {
	switch sel := txsel.GetSelector().(type) {
	case nil:
		// From documents: If none is provided, the default is a
		// temporary read-only transaction with strong concurrency.
		return s.createTransaction(txReadWrite) // ro or rw?
	case *spannerpb.TransactionSelector_SingleUse:
		return s.createTransaction(txReadWrite) // ro or rw?
	case *spannerpb.TransactionSelector_Id:
		tx, ok := s.GetTransaction(sel.Id)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Transaction was started in a different session")
		}
		return tx, nil
	case *spannerpb.TransactionSelector_Begin:
		return s.BeginTransaction(sel.Begin)
	default:
		return nil, fmt.Errorf("unknown transaction selector: %v", sel)
	}
}

func (s *session) BeginTransaction(opt *spannerpb.TransactionOptions) (*transaction, error) {
	var txMode transactionMode
	switch v := opt.GetMode().(type) {
	case *spannerpb.TransactionOptions_ReadWrite_:
		txMode = txReadWrite
	case *spannerpb.TransactionOptions_ReadOnly_:
		txMode = txReadOnly
	case *spannerpb.TransactionOptions_PartitionedDml_:
		txMode = txPartitionedDML
	case nil:
		// TransactionOptions is required
		return nil, status.Errorf(codes.InvalidArgument, "Invalid BeginTransaction request")
	default:
		return nil, status.Errorf(codes.Unknown, "unknown transaction mode: %v", v)
	}

	tx, err := s.createTransaction(txMode)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func newSession(db *database, dbName string) *session {
	id := uuidpkg.New().String()
	return &session{
		id:                 id,
		database:           db,
		dbName:             dbName,
		name:               fmt.Sprintf("%s/sessions/%s", dbName, id),
		createdAt:          time.Now(),
		transactions:       make(map[string]*transaction),
		activeTransactions: make([]*transaction, 0, 8),
	}
}

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
)

type transaction struct {
	id        []byte
	name      string
	session   *session
	mode      transactionMode
	status    TransactionStatus
	createdAt time.Time
}

func (tx *transaction) ID() []byte {
	return tx.id
}

func (tx *transaction) Name() string {
	return tx.name
}

func (tx *transaction) Proto() *spannerpb.Transaction {
	return &spannerpb.Transaction{
		Id: tx.id,
	}
}

func (tx *transaction) Status() TransactionStatus {
	return tx.status
}

func (tx *transaction) Available() bool {
	return tx.status == TransactionActive
}

func (tx *transaction) Done(status TransactionStatus) {
	tx.status = status
}

func newTransaction(s *session, mode transactionMode) *transaction {
	id := uuidpkg.New().String()
	return &transaction{
		id:        []byte(id),
		name:      id,
		session:   s,
		mode:      mode,
		status:    TransactionActive,
		createdAt: time.Now(),
	}
}
