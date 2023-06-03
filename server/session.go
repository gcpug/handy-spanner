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

	uuidpkg "github.com/google/uuid"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	mu           sync.Mutex
	transactions map[string]*transaction
	activeRWTx   []*transaction
}

func (s *session) Name() string {
	return s.name
}

func (s *session) Proto() *spannerpb.Session {
	ctime := timestamppb.New(s.createdAt)
	last := timestamppb.New(s.lastUse)
	return &spannerpb.Session{
		Name:                   s.name,
		CreateTime:             ctime,
		ApproximateLastUseTime: last,
	}
}

func (s *session) createTransaction(txMode transactionMode, single bool) (*transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < 3; i++ {
		tx := newTransaction(s, txMode, single)
		if _, ok := s.transactions[tx.Name()]; ok {
			continue
		}
		s.transactions[tx.Name()] = tx

		if err := s.database.BeginTransaction(tx); err != nil {
			return nil, err // TODO
		}

		// read write transactions are kept only 32 in a session
		// it seems no limitation for read only transactions
		if txMode == txReadWrite {
			s.activeRWTx = append(s.activeRWTx, tx)
			if len(s.activeRWTx) > 32 {
				oldtx := s.activeRWTx[0]
				s.activeRWTx = s.activeRWTx[1:]
				oldtx.Done(TransactionInvalidated)
			}
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

// GetTransactionBySelector returns a transaction by selector from session.
// The second return value means a new transaction is created or not.
// When the selector specifies Begin, true is returned. Otherwise false even if SingleUse option is specified.
func (s *session) GetTransactionBySelector(txsel *spannerpb.TransactionSelector) (*transaction, bool, error) {
	switch sel := txsel.GetSelector().(type) {
	case nil:
		// From documents: If none is provided, the default is a
		// single use transaction with strong concurrency.
		tx, err := s.createTransaction(txReadWrite, true)
		return tx, false, err
	case *spannerpb.TransactionSelector_SingleUse:
		tx, err := s.createTransaction(txReadWrite, true)
		return tx, false, err
	case *spannerpb.TransactionSelector_Id:
		tx, ok := s.GetTransaction(sel.Id)
		if !ok {
			return nil, false, status.Errorf(codes.InvalidArgument, "Transaction was started in a different session")
		}
		return tx, false, nil
	case *spannerpb.TransactionSelector_Begin:
		tx, err := s.BeginTransaction(sel.Begin)
		return tx, true, err
	default:
		return nil, false, fmt.Errorf("unknown transaction selector: %v", sel)
	}
}

// GetTransactionForCommit returns a transaction by selector from session.
// The argument is expected to be spannerpb.isCommitRequest_Transaction. It is not exported so interface{}
// is used instead.
func (s *session) GetTransactionForCommit(txsel interface{}) (*transaction, error) {
	switch v := txsel.(type) {
	case *spannerpb.CommitRequest_TransactionId:
		tx, ok := s.GetTransaction(v.TransactionId)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Transaction was started in a different session")
		}
		return tx, nil
	case *spannerpb.CommitRequest_SingleUseTransaction:
		return s.beginTransaction(v.SingleUseTransaction, true)
	default:
		return nil, status.Errorf(codes.Unknown, "unknown transaction: %v", v)
	}
}

// BeginTransaction creates a new transaction for a session.
func (s *session) BeginTransaction(opt *spannerpb.TransactionOptions) (*transaction, error) {
	return s.beginTransaction(opt, false)
}

func (s *session) beginTransaction(opt *spannerpb.TransactionOptions, single bool) (*transaction, error) {
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

	tx, err := s.createTransaction(txMode, single)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func newSession(db *database, dbName string) *session {
	id := uuidpkg.New().String()
	return &session{
		id:           id,
		database:     db,
		dbName:       dbName,
		name:         fmt.Sprintf("%s/sessions/%s", dbName, id),
		createdAt:    time.Now(),
		transactions: make(map[string]*transaction),
		activeRWTx:   make([]*transaction, 0, 8),
	}
}
