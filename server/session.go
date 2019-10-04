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

func (s *session) CreateTransaction(txMode transactionMode) (*transaction, error) {
	for i := 0; i < 3; i++ {
		tx := newTransaction(s, txMode)
		if _, ok := s.transactions[tx.Name()]; ok {
			continue
		}
		s.transactions[tx.Name()] = tx
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

func newSession(db *database, dbName string) *session {
	id := uuidpkg.New().String()
	return &session{
		id:           id,
		database:     db,
		dbName:       dbName,
		name:         fmt.Sprintf("%s/sessions/%s", dbName, id),
		createdAt:    time.Now(),
		transactions: make(map[string]*transaction),
	}
}

type transactionMode int

const (
	txReadOnly       transactionMode = 1
	txReadWrite      transactionMode = 2
	txPartitionedDML transactionMode = 3
)

type transaction struct {
	id        []byte
	name      string
	session   *session
	mode      transactionMode
	createdAt time.Time

	done bool
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

func newTransaction(s *session, mode transactionMode) *transaction {
	id := uuidpkg.New().String()
	return &transaction{
		id:        []byte(id),
		name:      id,
		session:   s,
		mode:      mode,
		createdAt: time.Now(),
	}
}
