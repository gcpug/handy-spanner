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
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	structpb "github.com/golang/protobuf/ptypes/struct"
	uuidpkg "github.com/google/uuid"
	sqlite "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Database interface {
	ApplyDDL(ctx context.Context, ddl ast.DDL) error

	Read(ctx context.Context, tx *transaction, tbl, index string, cols []string, keyset *KeySet, limit int64) (RowIterator, error)
	Query(ctx context.Context, tx *transaction, query *ast.QueryStatement, params map[string]Value) (RowIterator, error)
	Execute(ctx context.Context, tx *transaction, dml ast.DML, params map[string]Value) (int64, error)

	Insert(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error
	Update(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error
	Replace(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error
	InsertOrUpdate(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error
	Delete(ctx context.Context, tx *transaction, table string, keyset *KeySet) error

	BeginTransaction(tx *transaction) error
	Commit(tx *transaction) error
	Rollback(tx *transaction) error

	Close() error
}

type barrier struct {
	locked int32
	condMu *sync.Mutex
	cond   *sync.Cond
}

func newBarrier() *barrier {
	mu := new(sync.Mutex)
	cond := sync.NewCond(mu)
	return &barrier{
		locked: 0,
		condMu: mu,
		cond:   cond,
	}
}

// TryAcquire get a lock and blocks all other transactions enter critical section.
// When fail to get a lock, returns false. Please use Wait to wait the lock is released and
// try TryAcquire again.
func (l *barrier) TryAcquire() bool {
	now := atomic.LoadInt32(&l.locked)
	if now == 1 {
		return false
	}
	ok := atomic.CompareAndSwapInt32(&l.locked, now, 1)
	return ok
}

// Release releases the lock and notifies to transactions that wait releasing th lock.
func (l *barrier) Release() {
	atomic.StoreInt32(&l.locked, 0)
	l.cond.Broadcast()
}

// Wait waits until locked is released.
// This does not ensure only one transaction enters critical section.
// This makes sure one the lock is acquired by someone, other transactions are blocked until released.
func (l *barrier) Wait() {
	now := atomic.LoadInt32(&l.locked)
	if now == 0 {
		return
	}

	l.condMu.Lock()
	l.cond.Wait()
	l.condMu.Unlock()
}

func (l *barrier) Released() bool {
	now := atomic.LoadInt32(&l.locked)
	return now == 0
}

var _ Database = (*database)(nil)

type database struct {
	db *sql.DB

	ctx    context.Context
	cancel func()

	// schema level lock
	schemaMu sync.RWMutex
	tables   map[string]*Table

	// transactions
	transactions   map[string]*transaction
	transactionsMu sync.Mutex
	tablesInUse    map[string]*tableTransaction
	tablesInUseMu  sync.RWMutex

	// writeBarrier blocks other transactions try to write or commit
	writeBarrier       *barrier
	writeTransaction   *transaction
	writeTransactionMu sync.RWMutex
}

type tableTransaction struct {
	use                 sync.RWMutex
	lockHolder          *transaction
	transactionsInUse   map[string]*transaction
	transactionsInUseMu sync.Mutex
}

func (tt *tableTransaction) Dump() {
	if tt.lockHolder != nil {
		fmt.Printf("lock holder: %s status=%v\n", tt.lockHolder.Name(), tt.lockHolder.Status())
	} else {
		fmt.Printf("lock holder: <nil>\n")
	}
	fmt.Printf("transactions in use\n")
	for _, tx := range tt.transactionsInUse {
		fmt.Printf(" - %s status=%v\n", tx.Name(), tx.Status())
	}
}

func (tt *tableTransaction) Use(tx *transaction) {
	if IsDebug() {
		defer DebugStartEnd("[%s] tableTransaction.Use", tx.Name())()
	}

	// skip if the transction already holds the table lock
	tt.transactionsInUseMu.Lock()
	if tx.Equals(tt.lockHolder) {
		tt.transactionsInUseMu.Unlock()
		return
	}
	tt.transactionsInUseMu.Unlock()

	// try to get read lock
	tt.use.RLock()
	defer tt.use.RUnlock()

	tt.transactionsInUseMu.Lock()
	defer tt.transactionsInUseMu.Unlock()

	tt.transactionsInUse[tx.Name()] = tx
}

func (tt *tableTransaction) Lock(tx *transaction) {
	if IsDebug() {
		defer DebugStartEnd("[%s] tableTransaction.Lock", tx.Name())()
	}

	// skip if the transction already holds the table lock
	tt.transactionsInUseMu.Lock()
	if tx.Equals(tt.lockHolder) {
		tt.transactionsInUseMu.Unlock()
		return
	}
	tt.transactionsInUseMu.Unlock()

	// try to get write lock
	tt.use.Lock()

	var uses []*transaction
	func() {
		tt.transactionsInUseMu.Lock()
		defer tt.transactionsInUseMu.Unlock()

		if !tx.Available() {
			Debugf("[%s] transaction NOT AVAILABE in Lock: %v\n", tx.Name(), tx.Status())
			panic(fmt.Sprintf("[%s] transaction NOT AVAILABE in Lock: %v\n", tx.Name(), tx.Status()))
		}

		tt.lockHolder = tx

		for _, tx := range tt.transactionsInUse {
			uses = append(uses, tx)
		}
	}()

	// Abort all ransactions which hold read lock for the table
	for _, tt := range uses {
		if !tx.Equals(tt) {
			tt.Done(TransactionAborted)
		}
	}
}

func (tt *tableTransaction) Release(tx *transaction) {
	if IsDebug() {
		defer DebugStartEnd("[%s] tableTransaction.Release", tx.Name())()
	}

	tt.transactionsInUseMu.Lock()
	defer tt.transactionsInUseMu.Unlock()

	delete(tt.transactionsInUse, tx.Name())

	if !tx.Equals(tt.lockHolder) {
		return
	}

	tt.lockHolder = nil
	tt.use.Unlock()
}

func newTableTransaction() *tableTransaction {
	return &tableTransaction{
		transactionsInUse: make(map[string]*transaction),
	}
}

func newDatabase() *database {
	uuid := uuidpkg.New().String()
	db, err := sql.Open("sqlite3_spanner", fmt.Sprintf("file:%s.db?cache=shared&mode=memory&_foreign_keys=true", uuid))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// keep at least 1 active connection to keep database
	go func(conn *sql.Conn) {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}

			newConn, err := db.Conn(ctx)
			if err != nil {
				continue
			}
			conn.Close()
			conn = newConn
		}
	}(conn)

	return &database{
		tables:      make(map[string]*Table),
		tablesInUse: make(map[string]*tableTransaction),
		db:          db,

		ctx:    ctx,
		cancel: cancel,

		transactions: make(map[string]*transaction),

		writeBarrier: newBarrier(),
	}
}

// waitUntilReadable marks database is used for read.
// If database is locked for write by other transaction, this function blocks until the lock is released.
func (d *database) waitUntilReadable(ctx context.Context, tx *transaction) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] database.waitUntilReadable", tx.Name())()
	}

	// Skip if the transaction already holds write lock
	d.writeTransactionMu.RLock()
	curTx := d.writeTransaction
	d.writeTransactionMu.RUnlock()
	if tx.Equals(curTx) {
		return nil
	}

	if d.writeBarrier.Released() {
		return nil
	}

	ch := make(chan struct{}, 0)
	go func() {
		d.writeBarrier.Wait()
		close(ch)
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}

	if !tx.Available() {
		return ErrNotAvailableTransaction
	}

	return nil
}

// waitUntilWritable locks database for write.
// This function does not ensure all other transactions don't have read lock.
// Once locked, other transactions cannot newly get read or write lock.
//
// This function blocks until the lock is acquired. Break the block when the context
// is done while waiting the lock.
func (d *database) waitUntilWritable(ctx context.Context, tx *transaction) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] database.waitUntilWritable", tx.Name())()
	}

	// Skip if the transaction already holds write lock
	d.writeTransactionMu.RLock()
	curTx := d.writeTransaction
	d.writeTransactionMu.RUnlock()
	if tx.Equals(curTx) {
		return nil
	}

	ch := make(chan struct{}, 0)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if !tx.Available() {
				close(ch)
				return
			}

			// Try to get lock
			ok := d.writeBarrier.TryAcquire()
			if ok {
				// save the holder of write lock
				d.writeTransactionMu.Lock()
				d.writeTransaction = tx
				d.writeTransactionMu.Unlock()
				close(ch)
				return
			}

			// Return if the lock is acrequired by own transaction
			d.writeTransactionMu.RLock()
			curTx := d.writeTransaction
			d.writeTransactionMu.RUnlock()
			if curTx != nil && bytes.Equal(curTx.ID(), tx.ID()) {
				close(ch)
				return
			}

			// Wait until the lock is released
			d.writeBarrier.Wait()
		}
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}

	if !tx.Available() {
		d.releaseWriteLock(tx)
		return ErrNotAvailableTransaction
	}

	return nil
}

// releasewriteLock releases write lock for database
// This function can be called by any transactions but transactions don't hold write lock are ignored.
func (d *database) releaseWriteLock(tx *transaction) {
	if IsDebug() {
		defer DebugStartEnd("[%s] database.releaseWriteLock", tx.Name())()
	}

	d.writeTransactionMu.RLock()
	curTx := d.writeTransaction
	d.writeTransactionMu.RUnlock()

	if !curTx.Equals(tx) {
		return
	}

	d.writeTransactionMu.Lock()
	defer d.writeTransactionMu.Unlock()

	curTx = d.writeTransaction
	if curTx.Equals(tx) {
		d.writeTransaction = nil
		d.writeBarrier.Release()
	}
}

// useTable marks a table is used for read by the transaction.
// If the table is locked by other transaction, this function blocks until the lock is released.
func (d *database) useTable(tbl string, tx *transaction) (*Table, error) {
	d.schemaMu.RLock()
	defer d.schemaMu.RUnlock()

	d.tablesInUseMu.Lock()
	defer d.tablesInUseMu.Unlock()

	table, ok := d.tables[tbl]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Table not found: %s", tbl)
	}

	use, ok := d.tablesInUse[tbl]
	if !ok {
		use = newTableTransaction()
		d.tablesInUse[tbl] = use
	}

	use.Use(tx)

	if !tx.Available() {
		return nil, ErrNotAvailableTransaction
	}

	return table, nil
}

// useTableExclusive locks a table for write.
// Other transactions cannot read or write the table and wait until the lock is released.
// This function blocks until lock is acquired.
//
// When a transaction got the lock to a table, other transactions reading the table are
// aborted immediately.
func (d *database) useTableExclusive(tbl string, tx *transaction) (*Table, error) {
	table, ok := d.tables[tbl]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Table not found: %s", tbl)
	}

	var tt *tableTransaction
	func() {
		d.tablesInUseMu.Lock()
		defer d.tablesInUseMu.Unlock()

		use, ok := d.tablesInUse[tbl]
		if !ok {
			use = newTableTransaction()
			d.tablesInUse[tbl] = use
		}

		tt = use
	}()

	tt.Lock(tx)

	return table, nil
}

func (d *database) ApplyDDL(ctx context.Context, ddl ast.DDL) error {
	d.schemaMu.Lock()
	defer d.schemaMu.Unlock()

	switch val := ddl.(type) {
	case *ast.CreateTable:
		if err := d.CreateTable(ctx, val); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		return nil

	case *ast.CreateIndex:
		if err := d.CreateIndex(ctx, val); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		return nil

	case *ast.DropTable:
		return status.Errorf(codes.Unimplemented, "Drop Table is not supported yet")

	case *ast.DropIndex:
		return status.Errorf(codes.Unimplemented, "Drop Index is not supported yet")

	case *ast.AlterTable:
		return status.Errorf(codes.Unimplemented, "Alter Table is not supported yet")

	default:
		return status.Errorf(codes.Unknown, "unknown DDL statement: %v", val)
	}
}

func (d *database) Read(ctx context.Context, tx *transaction, tbl, idx string, cols []string, keyset *KeySet, limit int64) (RowIterator, error) {
	if keyset == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StreamingRead request")
	}
	if tbl == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StreamingRead request")
	}
	if len(cols) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StreamingRead request")
	}

	if err := d.waitUntilReadable(ctx, tx); err != nil {
		return nil, err
	}

	table, err := d.useTable(tbl, tx)
	if err != nil {
		return nil, err
	}

	index, ok := table.TableIndex(idx)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Index not found on table %s: %s", tbl, idx)
	}

	columns, err := table.getColumnsByName(cols)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	// Check the index table has the specified columns
	for _, column := range columns {
		if !index.HasColumn(column.Name()) {
			return nil, status.Errorf(codes.Unimplemented, "Reading non-indexed columns using an index is not supported. Consider adding %s to the index using a STORING clause.", column.Name())
		}
	}

	resultItems := make([]ResultItem, len(cols))
	for i := range columns {
		resultItems[i] = createResultItemFromColumn(columns[i])
	}

	indexColumnsName := strings.Join(index.IndexColumnNames(), ", ")
	indexColumns := index.IndexColumns()
	indexColumnDirs := index.IndexColumnDirections()
	colName := strings.Join(cols, ", ")

	var args []interface{}

	whereClause, whereArgs, err := buildWhereClauseFromKeySet(keyset, indexColumnsName, indexColumns)
	if err != nil {
		return nil, err
	}
	args = append(args, whereArgs...)

	orderByItems := make([]string, len(indexColumns))
	for i := range indexColumns {
		orderByItems[i] = fmt.Sprintf("%s %s", indexColumns[i].Name(), indexColumnDirs[i])
	}
	orderByClause := strings.Join(orderByItems, ", ")

	query := fmt.Sprintf(`SELECT %s FROM %s %s ORDER BY %s`, colName, table.Name, whereClause, orderByClause)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	var sqlRows *sql.Rows
	err = tx.ReadTransaction(func(ctx context.Context, dbtx databaseReader) error {
		r, err := dbtx.QueryContext(ctx, query, args...)
		if err != nil {
			return status.Errorf(codes.Internal, "query failed: %v, query: %v", err, query)
		}
		sqlRows = r
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &rows{rows: sqlRows, resultItems: resultItems, transaction: tx}, nil
}

func (d *database) Query(ctx context.Context, tx *transaction, stmt *ast.QueryStatement, params map[string]Value) (RowIterator, error) {
	if err := d.waitUntilReadable(ctx, tx); err != nil {
		return nil, err
	}

	query, args, resultItems, err := BuildQuery(d, tx, stmt.Query, params, false)
	if err != nil {
		return nil, err
	}

	var sqlRows *sql.Rows
	err = tx.ReadTransaction(func(ctx context.Context, dbtx databaseReader) error {
		r, err := dbtx.QueryContext(ctx, query, args...)
		if err != nil {
			return status.Errorf(codes.Internal, "query failed: %v, query: %v", err, query)
		}
		sqlRows = r
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &rows{rows: sqlRows, resultItems: resultItems, transaction: tx}, nil
}

func (d *database) Execute(ctx context.Context, tx *transaction, dml ast.DML, params map[string]Value) (int64, error) {
	if err := d.waitUntilWritable(ctx, tx); err != nil {
		return 0, err
	}

	if !tx.Available() {
		return 0, ErrNotAvailableTransaction
	}

	if tx.Status() == TransactionAborted {
		return 0, status.Errorf(codes.Aborted, "transaction aborted")
	}

	query, args, err := BuildDML(d, tx, dml, params)
	if err != nil {
		return 0, err
	}

	var affectedRows int64
	err = tx.WriteTransaction(func(dbtx databaseWriter) error {
		r, err := dbtx.ExecContext(ctx, query, args...)
		if err != nil {
			if sqliteErr, ok := err.(sqlite.Error); ok {
				// This error should not be happend.
				// This error means a tx tries to write a table which another tx holds read-lock
				// to the table, or a tx tries to write a table which another tx holds global wite-lock
				//
				// It better to be panic but return Aborted to expect the client retries.
				if sqliteErr.Code == sqlite.ErrLocked {
					return status.Errorf(codes.Aborted, "transaction is aborted: database is locked")
				}
			}

			return status.Errorf(codes.Internal, "failed to write into sqlite: %v", err)
		}

		affectedRows, err = r.RowsAffected()
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return affectedRows, nil
}

func (d *database) write(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue,
	nonNullCheck bool,
	affectedRowsCheck bool,
	buildQueryFn func(*Table, []*Column) string,
	buildArgsFn func(*Table, []*Column, []interface{}) []interface{},
) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] database.write", tx.Name())()
	}

	if err := d.waitUntilWritable(ctx, tx); err != nil {
		return err
	}

	if !tx.Available() {
		return ErrNotAvailableTransaction
	}

	if tx.Status() == TransactionAborted {
		return status.Errorf(codes.Aborted, "transaction aborted")
	}

	table, err := d.useTableExclusive(tbl, tx)
	if err != nil {
		return err
	}

	if IsDebug() {
		defer DebugStartEnd("[%s] database.write after write lock", tx.Name())()
	}

	primaryKey := table.primaryKey

	// Check columns are defined in the table
	columns, err := table.getColumnsByName(cols)
	if err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	// Ensure multiple values are not specified
	usedColumns := make(map[string]struct{}, len(cols))
	for _, c := range columns {
		n := c.Name()
		if _, ok := usedColumns[n]; ok {
			return status.Errorf(codes.InvalidArgument, "Multiple values for column %s", n)
		}
		usedColumns[n] = struct{}{}
	}

	// Check all primary keys are specified
	for _, colName := range primaryKey.IndexColumnNames() {
		if _, ok := usedColumns[colName]; !ok {
			return status.Errorf(codes.FailedPrecondition, "%s must not be NULL in table %s.", colName, tbl)
		}
	}

	// Check not nullable columns are specified for Insert/Replace
	if nonNullCheck {
		if exist, nonNullables := table.NonNullableColumnsExist(cols); exist {
			columns := strings.Join(nonNullables, ", ")
			return status.Errorf(codes.FailedPrecondition,
				"A new row in table %s does not specify a non-null value for these NOT NULL columns: %s",
				tbl, columns,
			)
		}
	}

	if len(values) == 0 {
		return nil
	}

	query := buildQueryFn(table, columns)
	if query == "" {
		return nil
	}

	err = tx.WriteTransaction(func(dbtx databaseWriter) error {
		for _, vs := range values {
			if len(vs.Values) != len(cols) {
				return status.Error(codes.InvalidArgument, "Mutation has mismatched number of columns and values.")
			}

			data := make([]interface{}, 0, len(cols))
			for i, v := range vs.Values {
				col := columns[i]

				vv, err := spannerValue2DatabaseValue(v, *col)
				if err != nil {
					return status.Errorf(codes.InvalidArgument, "%v", err)
				}

				if !col.nullable && vv == nil {
					return status.Errorf(codes.FailedPrecondition,
						"%s must not be NULL in table %s.",
						col.Name(), tbl,
					)
				}

				data = append(data, vv)
			}

			args := buildArgsFn(table, columns, data)

			r, err := dbtx.ExecContext(ctx, query, args...)
			if err != nil {
				if sqliteErr, ok := err.(sqlite.Error); ok {
					msg := sqliteErr.Error()
					switch sqliteErr.ExtendedCode {
					case sqlite.ErrConstraintPrimaryKey:
						return status.Errorf(codes.AlreadyExists, "Row %v in table %s already exists", data, tbl)
					case sqlite.ErrConstraintUnique:
						if n := strings.Index(msg, ": "); n > 0 {
							msg = msg[n+2:]
						}
						return status.Errorf(codes.AlreadyExists,
							"Unique index violation at index key [%v]. It conflicts with row %v in table %s",
							msg, data, tbl,
						)
					}

					// This error should not be happend.
					// This error means a tx tries to write a table which another tx holds read-lock
					// to the table, or a tx tries to write a table which another tx holds global wite-lock
					//
					// It better to be panic but return Aborted to expect the client retries.
					if sqliteErr.Code == sqlite.ErrLocked {
						return status.Errorf(codes.Aborted, "transaction is aborted: database is locked")
					}
				}

				return status.Errorf(codes.Internal, "failed to write into sqlite: %v", err)
			}

			if affectedRowsCheck {
				// Check rows are updated
				// When the row does not exist, sqlite returns success.
				// But spanner should return NotFound
				n, err := r.RowsAffected()
				if err != nil {
					return status.Errorf(codes.Internal, "failed to get RowsAffected: %v", err)
				}
				if n == 0 {
					return status.Errorf(codes.NotFound, "Row %v in table %s is missing. Row cannot be updated.", data, tbl)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *database) Insert(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		columnName := strings.Join(cols, ", ")
		placeholder := "?"
		if len(cols) > 1 {
			placeholder += strings.Repeat(", ?", len(cols)-1)
		}
		return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tbl, columnName, placeholder)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		return data
	}

	return d.write(ctx, tx, tbl, cols, values, true, false, buildQueryFn, buildArgsFn)
}

func (d *database) Update(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		assigns := make([]string, 0, len(cols))
		for _, c := range columns {
			if c.isPrimaryKey {
				continue
			}
			assigns = append(assigns, fmt.Sprintf("%s = ?", c.Name()))
		}

		// If no columns to be updated exist, it should be no-op.
		if len(assigns) == 0 {
			return ""
		}

		setClause := strings.Join(assigns, ", ")

		pKeysNames := table.primaryKey.IndexColumnNames()
		pkeysAssign := make([]string, len(pKeysNames))
		for i, col := range pKeysNames {
			pkeysAssign[i] = fmt.Sprintf("%s = ?", col)
		}
		whereClause := strings.Join(pkeysAssign, " AND ")

		return fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, tbl, setClause, whereClause)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		numPKeys := len(table.primaryKey.IndexColumns())
		values := make([]interface{}, 0, len(cols))
		pkeyValues := make([]interface{}, numPKeys)

		for i, column := range columns {
			if column.isPrimaryKey {
				pkeyValues[column.primaryKeyPos-1] = data[i]
			} else {
				values = append(values, data[i])
			}
		}

		// First N(size=columns-pkeys) values are for SET clause
		// Last M(size=pkeys) values are for WHERE clause
		values = append(values, pkeyValues...)

		return values
	}

	return d.write(ctx, tx, tbl, cols, values, false, true, buildQueryFn, buildArgsFn)
}

func (d *database) Replace(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		columnName := strings.Join(cols, ", ")
		placeholder := "?"
		if len(cols) > 1 {
			placeholder += strings.Repeat(", ?", len(cols)-1)
		}
		return fmt.Sprintf(`REPLACE INTO %s (%s) VALUES (%s)`, tbl, columnName, placeholder)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		return data
	}

	return d.write(ctx, tx, tbl, cols, values, true, false, buildQueryFn, buildArgsFn)
}

func (d *database) InsertOrUpdate(ctx context.Context, tx *transaction, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		assigns := make([]string, 0, len(cols))
		for _, c := range columns {
			if c.isPrimaryKey {
				continue
			}
			assigns = append(assigns, fmt.Sprintf("%s = ?", c.Name()))
		}
		setClause := strings.Join(assigns, ", ")

		pkeysNamesSlice := table.primaryKey.IndexColumnNames()
		pkeysNames := strings.Join(pkeysNamesSlice, ", ")

		columnName := strings.Join(cols, ", ")
		placeholder := "?"
		if len(cols) > 1 {
			placeholder += strings.Repeat(", ?", len(cols)-1)
		}

		return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
			tbl, columnName, placeholder, pkeysNames, setClause)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		setValues := make([]interface{}, 0, len(cols))

		for i, column := range columns {
			if !column.isPrimaryKey {
				setValues = append(setValues, data[i])
			}
		}

		// First N(size=columns) values are for VALUES clause
		// Last M(size=columns-pkeys) values are for SET clause
		data = append(data, setValues...)
		return data
	}

	return d.write(ctx, tx, tbl, cols, values, false, false, buildQueryFn, buildArgsFn)
}

func (d *database) Delete(ctx context.Context, tx *transaction, tbl string, keyset *KeySet) error {
	if err := d.waitUntilWritable(ctx, tx); err != nil {
		return err
	}

	table, err := d.useTableExclusive(tbl, tx)
	if err != nil {
		return err
	}

	index := table.primaryKey

	indexColumnsName := strings.Join(index.IndexColumnNames(), ", ")
	indexColumns := index.IndexColumns()

	whereClause, args, err := buildWhereClauseFromKeySet(keyset, indexColumnsName, indexColumns)
	if err != nil {
		return err
	}

	err = tx.WriteTransaction(func(dbtx databaseWriter) error {
		query := fmt.Sprintf("DELETE FROM %s %s", tbl, whereClause)
		if _, err := dbtx.ExecContext(ctx, query, args...); err != nil {
			return status.Errorf(codes.Internal, "failed to delete: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *database) BeginTransaction(tx *transaction) error {
	if tx == nil {
		return fmt.Errorf("invalid transaction: nil")
	}

	dbtx, err := d.db.BeginTx(tx.Context(), nil)
	if err != nil {
		return err
	}

	d.transactionsMu.Lock()
	defer d.transactionsMu.Unlock()

	if err := tx.SetTransaction(dbtx, d.endTransaction); err != nil {
		dbtx.Rollback()
		return err
	}

	d.transactions[tx.Name()] = tx

	return nil
}

func (d *database) endTransaction(tx *transaction, dbtx *sql.Tx) {
	if tx == nil {
		return
	}

	if IsDebug() {
		defer DebugStartEnd("[%s] database.endTransaction", tx.Name())()
	}

	// always try to do rollback to make sure the transaction finished
	if dbtx != nil {
		var lastErr error
		for i := 0; i < 3; i++ {
			err := dbtx.Rollback()
			if err == nil || err == sql.ErrTxDone {
				lastErr = nil
				break
			}

			lastErr = err
			time.Sleep(time.Millisecond)
		}

		if lastErr != nil {
			panic(fmt.Sprintf("endTransaction err: %T %v", lastErr, lastErr))
		}
	}

	d.transactionsMu.Lock()
	defer d.transactionsMu.Unlock()

	delete(d.transactions, tx.Name())

	for _, tt := range d.tablesInUse {
		tt.Release(tx)
	}
	d.releaseWriteLock(tx)

	return
}

func (d *database) Commit(tx *transaction) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] database.Commit", tx.Name())()
	}

	d.writeTransactionMu.RLock()
	defer d.writeTransactionMu.RUnlock()

	err := tx.WriteTransaction(func(dbtx databaseWriter) error {
		if err := dbtx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *database) Rollback(tx *transaction) error {
	if IsDebug() {
		defer DebugStartEnd("[%s] database.Rollback", tx.Name())()
	}

	d.writeTransactionMu.RLock()
	defer d.writeTransactionMu.RUnlock()

	err := tx.WriteTransaction(func(dbtx databaseWriter) error {
		if err := dbtx.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *database) CreateTable(ctx context.Context, stmt *ast.CreateTable) error {
	if _, ok := db.tables[stmt.Name.Name]; ok {
		return fmt.Errorf("duplicated table: %v", stmt.Name.Name)
	}

	t, err := createTableFromAST(stmt)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "CreateTable failed: %v", err)
	}
	db.tables[stmt.Name.Name] = t
	db.tablesInUse[stmt.Name.Name] = newTableTransaction()

	var columnDefs []string
	for _, col := range t.columns {
		s := fmt.Sprintf("  %s %s", col.Name(), col.dbDataType)
		if !col.nullable {
			// Array data type is not supported for now
			// so values for array data type are handled as null always
			if !col.isArray {
				s += " NOT NULL"
			}
		}
		columnDefs = append(columnDefs, s)
	}
	columnDefsQuery := strings.Join(columnDefs, ",\n")
	primaryKeysQuery := strings.Join(t.primaryKey.IndexColumnNames(), ", ")
	var foreignKeyConstraint string


	if stmt.Cluster != nil{
		parentTableName := stmt.Cluster.TableName.Name
		parentStmt, ok :=  db.tables[parentTableName]
		if !ok {
			return fmt.Errorf("could not find parent table for interleaving: %v", stmt.Name.Name)
		}

		columns := strings.Join(parentStmt.primaryKey.IndexColumnNames(), ",")

		foreignKeyConstraint = fmt.Sprintf(",\n FOREIGN KEY(%s) REFERENCES %s(%s) %s", columns, parentTableName, columns, stmt.Cluster.OnDelete)
	}

	query := fmt.Sprintf("CREATE TABLE `%s` (\n%s,\n  PRIMARY KEY (%s)%s\n)", t.Name, columnDefsQuery, primaryKeysQuery, foreignKeyConstraint)
	if _, err := db.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table for %s: %v", t.Name, err)
	}

	return nil
}

func (db *database) CreateIndex(ctx context.Context, stmt *ast.CreateIndex) error {
	table, ok := db.tables[stmt.TableName.Name]
	if !ok {
		return fmt.Errorf("table does not exist: %v", stmt.Name.Name)
	}

	index, err := table.createIndex(stmt)
	if err != nil {
		return err
	}

	idxType := "INDEX"
	if index.unique {
		idxType = "UNIQUE INDEX"
	}
	columnsName := strings.Join(index.IndexColumnNames(), ", ")

	query := fmt.Sprintf("CREATE %s `%s` ON %s (%s)", idxType, index.Name(), table.Name, columnsName)
	if _, err := db.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create index for %s: %v", index.Name(), err)
	}

	return nil
}

func (d *database) Close() error {
	d.cancel()

	if d.db == nil {
		return nil
	}

	d.schemaMu.Lock()
	defer d.schemaMu.Unlock()

	if err := d.db.Close(); err != nil {
		return err
	}

	d.db = nil

	return nil
}
