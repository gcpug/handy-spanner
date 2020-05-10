// Copyright 2020 Masahiro Sano
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

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newSpannerDatabaseNotFoundError(name string) error {
	detail := &errdetails.ResourceInfo{
		ResourceType: "type.googleapis.com/google.spanner.admin.database.v1.Database",
		ResourceName: name,
		Description:  "Database does not exist.",
	}
	st, err := status.Newf(codes.NotFound, "Database not found: %s", name).WithDetails(detail)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to build status error")
	}

	return st.Err()
}

func newSpannerSessionNotFoundError(name string) error {
	detail := &errdetails.ResourceInfo{
		ResourceType: "type.googleapis.com/google.spanner.v1.Session",
		ResourceName: name,
		Description:  "Session does not exist.",
	}
	st, err := status.Newf(codes.NotFound, "Session not found: %s", name).WithDetails(detail)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to build status error")
	}

	return st.Err()
}

func newSpannerTableNotFoundError(name string) error {
	detail := &errdetails.ResourceInfo{
		ResourceType: "spanner.googleapis.com/Table",
		ResourceName: name,
		Description:  "Table not found",
	}
	st, err := status.Newf(codes.NotFound, "Table not found: %s", name).WithDetails(detail)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to build status error")
	}

	return st.Err()
}

func newSpannerColumnNotFoundError(table, name string) error {
	detail := &errdetails.ResourceInfo{
		ResourceType: "spanner.googleapis.com/Column",
		ResourceName: name,
		// no description in real spanner
	}
	st, err := status.Newf(codes.NotFound, "Column not found in table %s: %s", table, name).WithDetails(detail)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to build status error")
	}

	return st.Err()
}

func newSpannerIndexnNotFoundError(table, name string) error {
	detail := &errdetails.ResourceInfo{
		ResourceType: "spanner.googleapis.com/Index",
		ResourceName: name,
		Description:  fmt.Sprintf("Index not found on table %s", table),
	}
	st, err := status.Newf(codes.NotFound, "Index not found on table %s: %s", table, name).WithDetails(detail)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to build status error")
	}

	return st.Err()
}
