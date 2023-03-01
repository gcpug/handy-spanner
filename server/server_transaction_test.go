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
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

func TestReadAndWriteTransaction_Aborted(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long test")
	}

	ctx := context.Background()

	s := newTestServer()

	preareDBAndSession := func(t *testing.T) *spannerpb.Session {
		session, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		for _, query := range []string{
			`INSERT INTO Simple VALUES(100, "xxx")`,
		} {
			if _, err := db.db.ExecContext(ctx, query); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		return session
	}

	session := preareDBAndSession(t)

	begin := func() *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}
	rollback := func(tx *spannerpb.Transaction) {
		_, _ = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		// ignore error
	}
	readSimple := func(tx *spannerpb.Transaction) error {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		err := s.StreamingRead(&spannerpb.ReadRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Table:   "Simple",
			Columns: []string{"Id"},
			KeySet:  &spannerpb.KeySet{All: true},
		}, fake)
		return err
	}
	readFT := func(tx *spannerpb.Transaction) error {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		err := s.StreamingRead(&spannerpb.ReadRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Table:   "FullTypes",
			Columns: []string{"PKey"},
			KeySet:  &spannerpb.KeySet{All: true},
		}, fake)
		return err
	}
	querySimple := func(tx *spannerpb.Transaction) error {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		err := s.ExecuteStreamingSql(&spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Sql: `SELECT Id FROM Simple`,
		}, fake)
		return err
	}
	queryFT := func(tx *spannerpb.Transaction) error {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		err := s.ExecuteStreamingSql(&spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Sql: `SELECT PKey FROM FullTypes`,
		}, fake)
		return err
	}
	commitSimple := func(tx *spannerpb.Transaction) error {
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{
				{
					Operation: &spannerpb.Mutation_InsertOrUpdate{
						InsertOrUpdate: &spannerpb.Mutation_Write{
							Table:   "Simple",
							Columns: []string{"Id", "Value"},
							Values: []*structpb.ListValue{
								{
									Values: []*structpb.Value{
										makeStringValue("300"),
										makeStringValue("ccc2"),
									},
								},
							},
						},
					},
				},
			},
		})
		return err
	}
	commitFT := func(tx *spannerpb.Transaction) error {
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{
				{
					Operation: &spannerpb.Mutation_InsertOrUpdate{
						InsertOrUpdate: &spannerpb.Mutation_Write{
							Table:   "FullTypes",
							Columns: fullTypesKeys,
							Values: []*structpb.ListValue{
								{
									Values: []*structpb.Value{
										makeStringValue("xxx"),  // PKey STRING(32) NOT NULL,
										makeStringValue("xxx"),  // FTString STRING(32) NOT NULL,
										makeNullValue(),         // FTStringNull STRING(32),
										makeBoolValue(true),     // FTBool BOOL NOT NULL,
										makeNullValue(),         // FTBoolNull BOOL,
										makeStringValue("eHh4"), // FTBytes BYTES(32) NOT NULL,
										makeNullValue(),         // FTBytesNull BYTES(32),
										makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
										makeNullValue(),               // FTTimestampNull TIMESTAMP,
										makeStringValue("100"),        // FTInt INT64 NOT NULL,
										makeNullValue(),               // FTIntNull INT64,
										makeNumberValue(0.5),          // FTFloat FLOAT64 NOT NULL,
										makeNullValue(),               // FTFloatNull FLOAT64,
										makeStringValue("2012-03-04"), // FTDate DATE NOT NULL,
										makeNullValue(),               // FTDateNull DATE,
									},
								},
							},
						},
					},
				},
			},
		})
		return err
	}

	readPattern := []struct {
		name       string
		readSimple func(tx *spannerpb.Transaction) error
		readFT     func(tx *spannerpb.Transaction) error
	}{
		{"Read", readSimple, readFT},
		{"Query", querySimple, queryFT},
	}

	t.Run("ReadLockAborted", func(t *testing.T) {
		for _, tc := range readPattern {
			t.Run(tc.name, func(t *testing.T) {
				tx1 := begin()
				tx2 := begin()
				defer rollback(tx1)
				defer rollback(tx2)
				assertStatusCode(t, tc.readSimple(tx1), codes.OK)
				assertStatusCode(t, commitSimple(tx2), codes.OK)
				assertStatusCode(t, tc.readSimple(tx1), codes.Aborted)
			})
		}
	})

	t.Run("ReadLock_DifferentTable", func(t *testing.T) {
		for _, tc := range readPattern {
			t.Run(tc.name, func(t *testing.T) {
				tx1 := begin()
				tx2 := begin()
				tx3 := begin()
				defer rollback(tx1)
				defer rollback(tx2)
				defer rollback(tx3)
				assertStatusCode(t, tc.readSimple(tx1), codes.OK)
				assertStatusCode(t, tc.readFT(tx2), codes.OK)
				assertStatusCode(t, commitSimple(tx3), codes.OK)
				assertStatusCode(t, tc.readSimple(tx1), codes.Aborted)
				assertStatusCode(t, tc.readFT(tx2), codes.OK)
			})
		}
	})

	t.Run("WriteTest", func(t *testing.T) {
		for _, tc := range readPattern {
			t.Run(tc.name, func(t *testing.T) {
				tx1 := begin()
				tx2 := begin()
				defer rollback(tx1)
				defer rollback(tx2)
				assertStatusCode(t, tc.readSimple(tx1), codes.OK)
				assertStatusCode(t, tc.readFT(tx2), codes.OK)
				assertStatusCode(t, commitSimple(tx1), codes.OK)
				assertStatusCode(t, commitFT(tx2), codes.OK)
			})
		}
	})

	t.Run("ReadAbortedWhileReading", func(t *testing.T) {
		for _, tc := range readPattern {
			t.Run(tc.name, func(t *testing.T) {
				for i := 0; i < 100; i++ {
					tx1 := begin()
					tx2 := begin()
					done := make(chan struct{})
					errCh := make(chan error, 2)

					assertStatusCode(t, tc.readSimple(tx1), codes.OK) // read lock first
					go func() {
						for {
							time.Sleep(100 * time.Microsecond)

							// try to happen aborted while reading
							err := tc.readSimple(tx1)
							code := status.Code(err)
							if code == codes.OK {
								continue
							}
							if code == codes.Aborted {
								close(done)
								return
							}

							errCh <- err
							close(done)
							return
						}
					}()
					time.Sleep(time.Duration(i/10+1) * time.Millisecond)
					assertStatusCode(t, commitSimple(tx2), codes.OK)
					<-done
					select {
					case err := <-errCh:
						if err != nil {
							t.Fatalf("error: %v", err)
						}
					default:
					}
				}
			})
		}
	})
}

func TestReadAndWriteTransaction_AtomicUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newTestServer()

	preareDB := func(t *testing.T) string {
		_, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		return dbName
	}

	createSession := func(t *testing.T, dbName string) *spannerpb.Session {
		session, err := s.CreateSession(context.Background(), &spannerpb.CreateSessionRequest{
			Database: dbName,
		})
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		return session
	}

	begin := func(session *spannerpb.Session) (*spannerpb.Transaction, error) {
		return s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
	}
	read := func(session *spannerpb.Session, tx *spannerpb.Transaction, pKey string, sql bool) (int, error) {
		var txSel *spannerpb.TransactionSelector
		if tx == nil {
			txSel = &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_SingleUse{
					SingleUse: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				},
			}
		} else {
			txSel = &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			}
		}

		fake := NewFakeExecuteStreamingSqlServer(ctx)
		if sql {
			err := s.ExecuteStreamingSql(&spannerpb.ExecuteSqlRequest{
				Session:     session.Name,
				Transaction: txSel,
				Sql:         `SELECT Value FROM Simple WHERE Id = @key`,
				ParamTypes: map[string]*spannerpb.Type{
					"key": &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
				Params: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key": makeStringValue(pKey),
					},
				},
			}, fake)
			if err != nil {
				return 0, err
			}
		} else {
			err := s.StreamingRead(&spannerpb.ReadRequest{
				Session:     session.Name,
				Transaction: txSel,
				Table:       "Simple",
				Columns:     []string{"Value"},
				KeySet: &spannerpb.KeySet{
					Keys: []*structpb.ListValue{
						{
							Values: []*structpb.Value{
								makeStringValue(pKey),
							},
						},
					},
				},
			}, fake)
			if err != nil {
				return 0, err
			}
		}

		var results [][]*structpb.Value
		for _, set := range fake.sets {
			results = append(results, set.Values)
		}

		if len(results) != 1 {
			return 0, fmt.Errorf("results should be 1 record but got %v", len(results))
		}

		v := results[0][0].Kind.(*structpb.Value_StringValue).StringValue
		n, _ := strconv.Atoi(v)
		return n, nil
	}

	commit := func(session *spannerpb.Session, tx *spannerpb.Transaction, pKey string, sql bool, next int) error {
		var mus []*spannerpb.Mutation
		if sql {
			_, err := s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
				Session: session.Name,
				Transaction: &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Id{
						Id: tx.Id,
					},
				},
				Sql: fmt.Sprintf(`UPDATE Simple Set Value = "%d" WHERE Id = %s`, next, pKey),
			})
			if err != nil {
				return err
			}
		} else {
			mus = []*spannerpb.Mutation{
				{
					Operation: &spannerpb.Mutation_Update{
						Update: &spannerpb.Mutation_Write{
							Table:   "Simple",
							Columns: []string{"Id", "Value"},
							Values: []*structpb.ListValue{
								{
									Values: []*structpb.Value{
										makeStringValue(pKey),
										makeStringValue(fmt.Sprint(next)),
									},
								},
							},
						},
					},
				},
			}
		}

		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: mus,
		})

		return err
	}

	dbname := preareDB(t)
	session := createSession(t, dbname)

	initialValue := 100

	table := []struct {
		pKey        string
		concurrency int // should be under 32
		tries       int
	}{
		{pKey: "1000", concurrency: 5, tries: 3},
		{pKey: "1001", concurrency: 10, tries: 10},
		{pKey: "1002", concurrency: 20, tries: 10},
		{pKey: "1003", concurrency: 50, tries: 5},
		// {pKey: "1004", concurrency: 100, tries: 30},
	}

	for _, tc := range table {
		pKey := fmt.Sprint(tc.pKey)
		concurrency := tc.concurrency
		tries := tc.tries
		t.Run(fmt.Sprintf("KEY%s_Con%d_Try%d", pKey, concurrency, tries), func(t *testing.T) {
			_, err := s.Commit(ctx, &spannerpb.CommitRequest{
				Session: session.Name,
				Transaction: &spannerpb.CommitRequest_SingleUseTransaction{
					SingleUseTransaction: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				},
				Mutations: []*spannerpb.Mutation{
					{
						Operation: &spannerpb.Mutation_Insert{
							Insert: &spannerpb.Mutation_Write{
								Table:   "Simple",
								Columns: []string{"Id", "Value"},
								Values: []*structpb.ListValue{
									{
										Values: []*structpb.Value{
											makeStringValue(pKey),
											makeStringValue(fmt.Sprint(initialValue)),
										},
									},
								},
							},
						},
					},
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			errCh := make(chan error, concurrency*tries)
			wg := &sync.WaitGroup{}
			readerDone := make(chan struct{})
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(me int) {
					defer wg.Done()

					// create own session
					session := createSession(t, dbname)

					err := func() error {
						try := 0
						for {
							select {
							case <-ctx.Done():
								return ctx.Err()
							default:
							}

							// begin transaction
							tx, err := begin(session)
							if err != nil {
								return fmt.Errorf("begin: %v", err)
							}

							// read the current value
							useSql := (me % 2) == 0
							n, err := read(session, tx, pKey, useSql)
							if err != nil {
								if code := status.Code(err); code == codes.Aborted {
									continue
								}
								return fmt.Errorf("[%s] read: %v", string(tx.Id), err)
							}
							next := n + 1

							// write +1 value
							if err := commit(session, tx, pKey, useSql, next); err != nil {
								if code := status.Code(err); code == codes.Aborted {
									continue
								}
								return fmt.Errorf("[%s] commit: %v", string(tx.Id), err)
							}

							try++
							if try == tries {
								return nil
							}
						}
					}()
					if err != nil {
						errCh <- err
						cancel()
					}
				}(i)
			}

			go func() {
				// create own session
				session := createSession(t, dbname)
			readLoop:
				for {
					select {
					case <-readerDone:
						break readLoop
					default:
					}
					if _, err := read(session, nil, pKey, false); err != nil {
						if code := status.Code(err); code == codes.Aborted {
							continue
						}
						errCh <- err
						cancel()
						break
					}
				}
			}()

			wg.Wait()

			close(readerDone)

			var errs []error
		L:
			for {
				select {
				case err := <-errCh:
					if err != nil {
						errs = append(errs, err)
					}
					cancel()
				default:
					break L
				}
			}

			if len(errs) != 0 {
				for _, err := range errs {
					t.Errorf("error %v", err)
				}
				t.FailNow()
			}

			n, err := read(session, nil, pKey, false)
			if err != nil {
				t.Fatalf("read error %v", err)
			}

			expected := initialValue + tries*concurrency
			if n != expected {
				t.Errorf("expect n to be %v, but got %v", expected, n)
			}
		})
	}
}
