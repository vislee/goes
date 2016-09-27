// Copyright 2013 Belogik. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package goes provides an API to access Elasticsearch.
package goes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

const (
	// BulkCommandIndex specifies a bulk doc should be indexed
	BulkCommandIndex = "index"
	// BulkCommandDelete specifies a bulk doc should be deleted
	BulkCommandDelete = "delete"
)

func (err *SearchError) Error() string {
	return fmt.Sprintf("[%d] %s", err.StatusCode, err.Msg)
}

// NewClient initiates a new client for an elasticsearch server
//
// This function is pretty useless for now but might be useful in a near future
// if wee need more features like connection pooling or load balancing.
func NewClient(host string, port string) *Client {
	return &Client{host, port, http.DefaultClient}
}

// WithHTTPClient sets the http.Client to be used with the connection. Returns the original client.
func (c *Client) WithHTTPClient(cl *http.Client) *Client {
	c.Client = cl
	return c
}

// CreateIndex creates a new index represented by a name and a mapping
func (c *Client) CreateIndex(name string, mapping interface{}) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     mapping,
		IndexList: []string{name},
		Method:    "PUT",
	}

	return r.Run()
}

// DeleteIndex deletes an index represented by a name
func (c *Client) DeleteIndex(name string) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: []string{name},
		Method:    "DELETE",
	}

	return r.Run()
}

// RefreshIndex refreshes an index represented by a name
func (c *Client) RefreshIndex(name string) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: []string{name},
		Method:    "POST",
		API:       "_refresh",
	}

	return r.Run()
}

// UpdateIndexSettings updates settings for existing index represented by a name and a settings
// as described here: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
func (c *Client) UpdateIndexSettings(name string, settings interface{}) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     settings,
		IndexList: []string{name},
		Method:    "PUT",
		API:       "_settings",
	}

	return r.Run()
}

// Optimize an index represented by a name, extra args are also allowed please check:
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-optimize.html#indices-optimize
func (c *Client) Optimize(indexList []string, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: indexList,
		ExtraArgs: extraArgs,
		Method:    "POST",
		API:       "_optimize",
	}

	return r.Run()
}

// Stats fetches statistics (_stats) for the current elasticsearch server
func (c *Client) Stats(indexList []string, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: indexList,
		ExtraArgs: extraArgs,
		Method:    "GET",
		API:       "_stats",
	}

	return r.Run()
}

// IndexStatus fetches the status (_status) for the indices defined in
// indexList. Use _all in indexList to get stats for all indices
func (c *Client) IndexStatus(indexList []string) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: indexList,
		Method:    "GET",
		API:       "_status",
	}

	return r.Run()
}

// BulkSend bulk adds multiple documents in bulk mode
func (c *Client) BulkSend(documents []Document) (*Response, error) {
	// We do not generate a traditional JSON here (often a one liner)
	// Elasticsearch expects one line of JSON per line (EOL = \n)
	// plus an extra \n at the very end of the document
	//
	// More informations about the Bulk JSON format for Elasticsearch:
	//
	// - http://www.elasticsearch.org/guide/reference/api/bulk.html
	//
	// This is quite annoying for us as we can not use the simple JSON
	// Marshaler available in Run().
	//
	// We have to generate this special JSON by ourselves which leads to
	// the code below.
	//
	// I know it is unreadable I must find an elegant way to fix this.

	// len(documents) * 2 : action + optional_sources
	// + 1 : room for the trailing \n
	bulkData := make([][]byte, len(documents)*2+1)
	i := 0

	for _, doc := range documents {
		action, err := json.Marshal(map[string]interface{}{
			doc.BulkCommand: map[string]interface{}{
				"_index": doc.Index,
				"_type":  doc.Type,
				"_id":    doc.ID,
			},
		})

		if err != nil {
			return &Response{}, err
		}

		bulkData[i] = action
		i++

		if doc.Fields != nil {
			if docFields, ok := doc.Fields.(map[string]interface{}); ok {
				if len(docFields) == 0 {
					continue
				}
			} else {
				typeOfFields := reflect.TypeOf(doc.Fields)
				if typeOfFields.Kind() == reflect.Ptr {
					typeOfFields = typeOfFields.Elem()
				}
				if typeOfFields.Kind() != reflect.Struct {
					return &Response{}, fmt.Errorf("Document fields not in struct or map[string]interface{} format")
				}
				if typeOfFields.NumField() == 0 {
					continue
				}
			}

			sources, err := json.Marshal(doc.Fields)
			if err != nil {
				return &Response{}, err
			}

			bulkData[i] = sources
			i++
		}
	}

	// forces an extra trailing \n absolutely necessary for elasticsearch
	bulkData[len(bulkData)-1] = []byte(nil)

	r := Request{
		Conn:     c,
		Method:   "POST",
		API:      "_bulk",
		BulkData: bytes.Join(bulkData, []byte("\n")),
	}

	return r.Run()
}

// Search executes a search query against an index
func (c *Client) Search(query interface{}, indexList []string, typeList []string, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     query,
		IndexList: indexList,
		TypeList:  typeList,
		Method:    "POST",
		API:       "_search",
		ExtraArgs: extraArgs,
	}

	return r.Run()
}

// Count executes a count query against an index, use the Count field in the response for the result
func (c *Client) Count(query interface{}, indexList []string, typeList []string, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     query,
		IndexList: indexList,
		TypeList:  typeList,
		Method:    "POST",
		API:       "_count",
		ExtraArgs: extraArgs,
	}

	return r.Run()
}

//Query runs a query against an index using the provided http method.
//This method can be used to execute a delete by query, just pass in "DELETE"
//for the HTTP method.
func (c *Client) Query(query interface{}, indexList []string, typeList []string, httpMethod string, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     query,
		IndexList: indexList,
		TypeList:  typeList,
		Method:    httpMethod,
		API:       "_query",
		ExtraArgs: extraArgs,
	}

	return r.Run()
}

// Scan starts scroll over an index
func (c *Client) Scan(query interface{}, indexList []string, typeList []string, timeout string, size int) (*Response, error) {
	v := url.Values{}
	v.Add("search_type", "scan")
	v.Add("scroll", timeout)
	v.Add("size", strconv.Itoa(size))

	r := Request{
		Conn:      c,
		Query:     query,
		IndexList: indexList,
		TypeList:  typeList,
		Method:    "POST",
		API:       "_search",
		ExtraArgs: v,
	}

	return r.Run()
}

// Scroll fetches data by scroll id
func (c *Client) Scroll(scrollID string, timeout string) (*Response, error) {
	v := url.Values{}
	v.Add("scroll", timeout)

	r := Request{
		Conn:      c,
		Method:    "POST",
		API:       "_search/scroll",
		ExtraArgs: v,
		Body:      []byte(scrollID),
	}

	return r.Run()
}

// Get a typed document by its id
func (c *Client) Get(index string, documentType string, id string, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: []string{index},
		Method:    "GET",
		API:       documentType + "/" + id,
		ExtraArgs: extraArgs,
	}

	return r.Run()
}

// Index indexes a Document
// The extraArgs is a list of url.Values that you can send to elasticsearch as
// URL arguments, for example, to control routing, ttl, version, op_type, etc.
func (c *Client) Index(d Document, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     d.Fields,
		IndexList: []string{d.Index.(string)},
		TypeList:  []string{d.Type},
		ExtraArgs: extraArgs,
		Method:    "POST",
	}

	if d.ID != nil {
		r.Method = "PUT"
		r.ID = d.ID.(string)
	}

	return r.Run()
}

// Delete deletes a Document d
// The extraArgs is a list of url.Values that you can send to elasticsearch as
// URL arguments, for example, to control routing.
func (c *Client) Delete(d Document, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		IndexList: []string{d.Index.(string)},
		TypeList:  []string{d.Type},
		ExtraArgs: extraArgs,
		Method:    "DELETE",
		ID:        d.ID.(string),
	}

	return r.Run()
}

// Run executes an elasticsearch Request. It converts data to Json, sends the
// request and returns the Response obtained
func (req *Request) Run() (*Response, error) {
	body, statusCode, err := req.run()
	esResp := &Response{Status: statusCode}

	if err != nil {
		return esResp, err
	}

	if req.Method != "HEAD" {
		err = json.Unmarshal(body, &esResp)
		if err != nil {
			return esResp, err
		}
		err = json.Unmarshal(body, &esResp.Raw)
		if err != nil {
			return esResp, err
		}
	}

	if req.API == "_bulk" && esResp.Errors {
		for _, item := range esResp.Items {
			for _, i := range item {
				if i.Error != "" {
					return esResp, &SearchError{i.Error, i.Status}
				}
			}
		}
		return esResp, &SearchError{Msg: "Unknown error while bulk indexing"}
	}

	if esResp.Error != "" {
		return esResp, &SearchError{esResp.Error, esResp.Status}
	}

	return esResp, nil
}

func (req *Request) run() ([]byte, uint64, error) {
	postData := []byte{}

	// XXX : refactor this
	if len(req.Body) > 0 {
		postData = req.Body
	} else if req.API == "_bulk" {
		postData = req.BulkData
	} else {
		b, err := json.Marshal(req.Query)
		if err != nil {
			return nil, 0, err
		}
		postData = b
	}

	reader := bytes.NewReader(postData)

	newReq, err := http.NewRequest(req.Method, req.URL(), reader)
	if err != nil {
		return nil, 0, err
	}

	if req.Method == "POST" || req.Method == "PUT" {
		newReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := req.Conn.Client.Do(newReq)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, uint64(resp.StatusCode), err
	}

	if resp.StatusCode > 201 && resp.StatusCode < 400 {
		return nil, uint64(resp.StatusCode), errors.New(string(body))
	}

	return body, uint64(resp.StatusCode), nil
}

// URL builds a Request for a URL
func (req *Request) URL() string {
	path := "/" + strings.Join(req.IndexList, ",")

	if len(req.TypeList) > 0 {
		path += "/" + strings.Join(req.TypeList, ",")
	}

	// XXX : for indexing documents using the normal (non bulk) API
	if len(req.ID) > 0 {
		path += "/" + req.ID
	}

	path += "/" + req.API

	u := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%s", req.Conn.Host, req.Conn.Port),
		Path:     path,
		RawQuery: req.ExtraArgs.Encode(),
	}

	return u.String()
}

// Buckets returns list of buckets in aggregation
func (a Aggregation) Buckets() []Bucket {
	result := []Bucket{}
	if buckets, ok := a["buckets"]; ok {
		for _, bucket := range buckets.([]interface{}) {
			result = append(result, bucket.(map[string]interface{}))
		}
	}

	return result
}

// Key returns key for aggregation bucket
func (b Bucket) Key() interface{} {
	return b["key"]
}

// DocCount returns count of documents in this bucket
func (b Bucket) DocCount() uint64 {
	return uint64(b["doc_count"].(float64))
}

// Aggregation returns aggregation by name from bucket
func (b Bucket) Aggregation(name string) Aggregation {
	if agg, ok := b[name]; ok {
		return agg.(map[string]interface{})
	}
	return Aggregation{}
}

// PutMapping registers a specific mapping for one or more types in one or more indexes
func (c *Client) PutMapping(typeName string, mapping interface{}, indexes []string) (*Response, error) {

	r := Request{
		Conn:      c,
		Query:     mapping,
		IndexList: indexes,
		Method:    "PUT",
		API:       "_mappings/" + typeName,
	}

	return r.Run()
}

// GetMapping returns the mappings for the specified types
func (c *Client) GetMapping(types []string, indexes []string) (*Response, error) {

	r := Request{
		Conn:      c,
		IndexList: indexes,
		Method:    "GET",
		API:       "_mapping/" + strings.Join(types, ","),
	}

	return r.Run()
}

// IndicesExist checks whether index (or indices) exist on the server
func (c *Client) IndicesExist(indexes []string) (bool, error) {

	r := Request{
		Conn:      c,
		IndexList: indexes,
		Method:    "HEAD",
	}

	resp, err := r.Run()

	return resp.Status == 200, err
}

// Update updates the specified document using the _update endpoint
func (c *Client) Update(d Document, query interface{}, extraArgs url.Values) (*Response, error) {
	r := Request{
		Conn:      c,
		Query:     query,
		IndexList: []string{d.Index.(string)},
		TypeList:  []string{d.Type},
		ExtraArgs: extraArgs,
		Method:    "POST",
		API:       "_update",
	}

	if d.ID != nil {
		r.ID = d.ID.(string)
	}

	return r.Run()
}

// DeleteMapping deletes a mapping along with all data in the type
func (c *Client) DeleteMapping(typeName string, indexes []string) (*Response, error) {

	r := Request{
		Conn:      c,
		IndexList: indexes,
		Method:    "DELETE",
		API:       "_mappings/" + typeName,
	}

	return r.Run()
}

func (c *Client) modifyAlias(action string, alias string, indexes []string) (*Response, error) {
	command := map[string]interface{}{
		"actions": make([]map[string]interface{}, 1),
	}

	for _, index := range indexes {
		command["actions"] = append(command["actions"].([]map[string]interface{}), map[string]interface{}{
			action: map[string]interface{}{
				"index": index,
				"alias": alias,
			},
		})
	}

	r := Request{
		Conn:   c,
		Query:  command,
		Method: "POST",
		API:    "_aliases",
	}

	return r.Run()
}

// AddAlias creates an alias to one or more indexes
func (c *Client) AddAlias(alias string, indexes []string) (*Response, error) {
	return c.modifyAlias("add", alias, indexes)
}

// RemoveAlias removes an alias to one or more indexes
func (c *Client) RemoveAlias(alias string, indexes []string) (*Response, error) {
	return c.modifyAlias("remove", alias, indexes)
}

// AliasExists checks whether alias is defined on the server
func (c *Client) AliasExists(alias string) (bool, error) {

	r := Request{
		Conn:   c,
		Method: "HEAD",
		API:    "_alias/" + alias,
	}

	resp, err := r.Run()

	return resp.Status == 200, err
}
