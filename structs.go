// Copyright 2013 Belogik. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goes

import (
	"net/url"
)

// Represents a Connection object to elasticsearch
type Connection struct {
	// The host to connect to
	Host string

	// The port to use
	Port string
}

// Represents a Request to elasticsearch
type Request struct {
	// Which connection will be used
	Conn *Connection

	// A search query
	Query interface{}

	// Which index to search into
	IndexList []string

	// Which type to search into
	TypeList []string

	// HTTP Method to user (GET, POST ...)
	method string

	// Which api keyword (_search, _bulk, etc) to use
	api string

	// Bulk data
	bulkData []byte

	// A list of extra URL arguments
	ExtraArgs url.Values

	// Used for the id field when indexing a document
	id string
}

// Represents a Response from elasticsearch
type Response struct {
	Ok           bool
	Acknowledged bool
	Error        string
	Status       uint64
	Took         uint64
	TimedOut     bool  `json:"timed_out"`
	Shards       Shard `json:"_shards"`
	Hits         Hits
	Index        string `json:"_index"`
	Id           string `json:"_id"`
	Type         string `json:"_type"`
	Version      int    `json:"_version"`
	Found        bool

	// Used by the _stats API
	All All `json:"_all"`

	// Used by the _bulk API
	Items []map[string]Item `json:"items,omitempty"`

	// Used by the GET API
	Exists bool
	Source map[string]interface{} `json:"_source"`
	Fields map[string]interface{} `json:"fields"`

	// Used by the _status API
	Indices map[string]IndexStatus
}

// Represents a document to send to elasticsearch
type Document struct {
	// XXX : interface as we can support nil values
	Index       interface{}
	Type        string
	Id          interface{}
	BulkCommand string
	Fields      map[string]interface{}
}

// Represents the "items" field in a _bulk response
type Item struct {
	Ok      bool   `json:"ok"`
	Type    string `json:"_type"`
	Id      string `json:"_id"`
	Index   string `json:"_index"`
	Version int    `json:"_version"`
}

// Represents the "_all" field when calling the _stats API
// This is minimal but this is what I only need
type All struct {
	Indices   map[string]StatIndex   `json:"indices"`
	Primaries map[string]StatPrimary `json:"primaries"`
}

type StatIndex struct {
	Primaries map[string]StatPrimary `json:"primaries"`
}

type StatPrimary struct {
	// primary/docs:
	Count   int
	Deleted int
}

// Represents the "shard" struct as returned by elasticsearch
type Shard struct {
	Total      uint64
	Successful uint64
	Failed     uint64
}

// Represent a hit returned by a search
type Hit struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Id     string                 `json:"_id"`
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
	Fields map[string]interface{} `json:"fields"`
}

// Represent the hits structure as returned by elasticsearch
type Hits struct {
	Total uint64
	// max_score may contain the "null" value
	MaxScore interface{} `json:"max_score"`
	Hits     []Hit
}

type SearchError struct {
	Msg        string
	StatusCode uint64
}

// Represent the status for a given index for the _status command
type IndexStatus struct {
	// XXX : problem, int will be marshaled to a float64 which seems logical
	// XXX : is it better to use strings even for int values or to keep
	// XXX : interfaces and deal with float64 ?
	Index map[string]interface{}

	Translog map[string]uint64
	Docs     map[string]uint64
	Merges   map[string]interface{}
	Refresh  map[string]interface{}
	Flush    map[string]interface{}

	// TODO: add shards support later, we do not need it for the moment
}
