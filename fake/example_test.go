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

package fake_test

import (
	"context"
	"log"
	"strings"

	"cloud.google.com/go/spanner"
	admindatabasev1 "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/gcpug/handy-spanner/fake"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func ExampleSpannerClient() {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"

	// Run fake server
	srv, conn, err := fake.Run()
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()
	defer conn.Close()

	// Prepare spanner client
	client, err := spanner.NewClient(ctx, dbName, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("failed to connect fake spanner server: %v", err)
	}

	// Use the client
	_, _ = client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Test",
			[]string{"ColA", "ColB"},
			[]interface{}{"foo", 100},
		),
	})

	// output:
}

func ExampleAdminClient() {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"
	stmts := []string{
		`CREATE TABLE Table1 ( Id STRING(MAX) NOT NULL ) PRIMARY KEY (Id)`,
		`CREATE TABLE Table2 ( Id TIMESTAMP NOT NULL ) PRIMARY KEY (Id)`,
	}

	// Run fake server
	srv, conn, err := fake.Run()
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()

	// Prepare spanner client
	adminclient, err := admindatabasev1.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("failed to connect fake spanner server: %v", err)
	}

	// Use the client
	_, err = adminclient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbName,
		Statements: stmts,
	})

	// output:
}

func ExampleApplyDDL() {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"
	schema := `
CREATE TABLE Table1 ( Id STRING(MAX) NOT NULL ) PRIMARY KEY (Id);
CREATE TABLE Table2 ( Id TIMESTAMP NOT NULL ) PRIMARY KEY (Id);
`

	// Run fake server
	srv, _, err := fake.Run()
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()

	err = srv.ParseAndApplyDDL(ctx, dbName, strings.NewReader(schema))
	if err != nil {
		log.Fatal(err)
	}

	// output:
}
