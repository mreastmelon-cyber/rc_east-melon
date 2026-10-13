package migrations

import _ "embed"

// Initial is the reviewed V1 MySQL schema.
//
//go:embed 001_initial.sql
var Initial string
