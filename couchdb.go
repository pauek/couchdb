package couchdb

import (
	"errors"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var Url string
var client *http.Client

func init() {
	Url = "http://localhost:5984"
	client = &http.Client{}
}

// UUIDs

const hex = "0123456789abcdef"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandString(length int) string {
	var str = make([]byte, length)
	for i := 0; i < length; i++ {
		str[i] = hex[rand.Intn(16)]
	}
	return fmt.Sprintf("%s", str)
}

func NewUUID() string {
	return RandString(32)
}

// Database

type Database struct {
	dbname string
}

func marshal(v interface{}, preamble map[string]string) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "{")
	for key, value := range preamble {
		if value != "" {
			fmt.Fprintf(&b, `"%s":"%s",`, key, value)
		}
	}
	json, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(&b, "%s", json[1:]) // includes '}'
	return b.Bytes(), nil
}

func (D *Database) url(path string) string {
	return fmt.Sprintf("%s/%s/%s", Url, D.dbname, path)
}

func (D *Database) Rev(id string) (rev string, err error) {
	rev, err = "", nil
	req, err := http.NewRequest("HEAD", D.url(id), nil)
	if err != nil {
		err = fmt.Errorf("Rev: cannot create request: %s\n", err)
		return
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		err = fmt.Errorf("Rev: http.client error: %s\n", err)
		return
	case resp.StatusCode == 404:
		err = nil // not found is not an error
		return
	case resp.StatusCode != 200:
		err = fmt.Errorf("Rev: HTTP status = '%s'\n", resp.Status)
		return
	}
	rev = resp.Header.Get("Etag")
	if rev == "" {
		err = fmt.Errorf("Rev: Header 'Etag' not found\n")
	}
	rev = strings.Replace(rev, `"`, ``, -1)
	return
}

var NotFound = errors.New("ID not found in database")

func (D *Database) Get(id string, v interface{}) (rev string, err error) {
	rev = ""
	req, err := http.NewRequest("GET", D.url(id), nil)
	if err != nil {
		err = fmt.Errorf("Get: cannot create request: %s\n", err)
		return
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		err = fmt.Errorf("Get: http.client error: %s\n", err)
		return
	case resp.StatusCode == 404:
		err = NotFound
		return
	case resp.StatusCode != 200:
		err = fmt.Errorf("Get: HTTP status = '%s'\n", resp.Status)
		return
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("Get: cannot read response body: %s\n", err)
		return
	}
	if err = json.Unmarshal(data, v); err != nil {
		err = fmt.Errorf("Get: json.Unmarshal error: %s\n", err)
		return
	}
	rev = resp.Header.Get("Etag")
	return
}

func (D *Database) Put(id string, v interface{}) error {
	return D.put(id, "", v)
}

func (D *Database) Update(id, rev string, v interface{}) error {
	return D.put(id, rev, v)
}

func (D *Database) PutOrUpdate(id string, v interface{}) error {
	rev, err := D.Rev(id)
	if err != nil {
		return fmt.Errorf("PutOrUpdate: %s\n", err)
	}
	if rev == "" {
		return D.Put(id, v)
	}
	return D.Update(id, rev, v)
}

func (D *Database) put(id, rev string, v interface{}) error {
	// TODO: Detect that 'v' really is db.Obj
	preamble := map[string]string{
		"_id":  id,
		"_rev": rev,
	}
	json, err := marshal(v, preamble)
	if err != nil {
		return fmt.Errorf("Put: json.Marshal error: %s\n", err)
	}
	b := bytes.NewBuffer(json)
	req, err := http.NewRequest("PUT", D.url(id), b)
	if err != nil {
		return fmt.Errorf("Put: cannot create request: %s\n", err)
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		return fmt.Errorf("Put: http.client error: %s\n", err)
	case resp.StatusCode != 201:
		// body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Put: HTTP status = '%s'\n", resp.Status)
	}
	return nil
}

func (D *Database) Delete(id, rev string) error {
	req, err := http.NewRequest("DELETE", D.url(id), nil)
	req.Header.Set("If-Match", rev)
	if err != nil {
		return fmt.Errorf("Delete: cannot create request: %s\n", err)
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		return fmt.Errorf("Delete: http.client error: %s\n", err)
	case resp.StatusCode == 404:
		return nil
	case resp.StatusCode != 200:
		return fmt.Errorf("Delete: HTTP status = '%s'\n", resp.Status)
	}
	return nil
}

// Database Functions

func GetDB(dbname string) (db *Database, err error) {
	db = nil
	url := fmt.Sprintf("%s/%s/", Url, dbname)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		err = fmt.Errorf("Get: cannot create request: %s\n", err)
		return
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		err = fmt.Errorf("Get: http.client error: %s\n", err)
		return
	case resp.StatusCode == 404:
		err = fmt.Errorf("Get: database '%s' doesn't exist\n", dbname)
		return
	case resp.StatusCode != 200:
		err = fmt.Errorf("Get: HTTP status = '%s'\n", resp.Status)
		return
	}
	return &Database{dbname}, nil
}

func CreateDB(dbname string) (db *Database, err error) {
	db = nil
	url := fmt.Sprintf("%s/%s/", Url, dbname)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		err = fmt.Errorf("Get: cannot create request: %s\n", err)
		return
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		err = fmt.Errorf("Create: http.client error: %s\n", err)
		return
	case resp.StatusCode != 201:
		err = fmt.Errorf("Create: HTTP status = '%s'\n", resp.Status)
		return
	}
	return &Database{dbname}, nil
}

func GetOrCreateDB(dbname string) (db *Database, err error) {
	db, err = GetDB(dbname)
	if db == nil {
		db, err = CreateDB(dbname)
	}
	return
}

func DeleteDB(db *Database) (err error) {
	req, err := http.NewRequest("DELETE", db.url(""), nil)
	if err != nil {
		err = fmt.Errorf("Get: cannot create request: %s\n", err)
		return
	}
	resp, err := client.Do(req)
	switch {
	case err != nil:
		err = fmt.Errorf("Create: http.client error: %s\n", err)
		return
	case resp.StatusCode == 404:
		return
	case resp.StatusCode != 200:
		err = fmt.Errorf("Create: HTTP status = '%s'\n", resp.Status)
		return
	}
	return
}
