package berkeleydb

// #cgo LDFLAGS: -ldb
// #include <db.h>
// #include <stdlib.h>
// #include "bdb.h"
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

const version string = "0.0.4"

// Flags for opening a database or environment.
const (
	DbCreate   = C.DB_CREATE
	DbExcl     = C.DB_EXCL
	DbRdOnly   = C.DB_RDONLY
	DbTruncate = C.DB_TRUNCATE

	// DbInitMpool is used in environment only.
	DbInitMpool = C.DB_INIT_MPOOL
)

// Database types.
const (
	DbBtree   = C.DB_BTREE
	DbHash    = C.DB_HASH
	DbRecno   = C.DB_RECNO
	DbQueue   = C.DB_QUEUE
	DbUnknown = C.DB_UNKNOWN
)

type CursorMode int

// Cursor.Get/Cursor.GetString modes.
const (
	DbNext  CursorMode = C.DB_NEXT
	DbPrev  CursorMode = C.DB_PREV
	DbFirst CursorMode = C.DB_FIRST
	DbLast  CursorMode = C.DB_LAST
)

// Db is the structure that holds the database connection.
type Db struct {
	db *C.DB
}

// Cursor holds the current cursor position.
type Cursor struct {
	dbc *C.DBC
}

// Errno is the error number.
type Errno int

// NewDB initialises a new bdb connection.
func NewDB() (*Db, error) {
	var db *C.DB
	if ret := C.db_create(&db, nil, 0); ret > 0 {
		return nil, createError(ret)
	}
	return &Db{db}, nil
}

// NewDBInEnvironment initialises a new bdb connection in an environment.
func NewDBInEnvironment(env *Environment) (*Db, error) {
	var db *C.DB
	if ret := C.db_create(&db, env.environ, 0); ret > 0 {
		return nil, createError(ret)
	}
	return &Db{db}, nil
}

// OpenWithTxn opens the database in transaction mode (transactions are not yet supported by all
// functions).
func (handle *Db) OpenWithTxn(filename string, txn *C.DB_TXN, dbtype C.DBTYPE, flags C.u_int32_t) error {
	db := handle.db
	file := C.CString(filename)
	defer C.free(unsafe.Pointer(file))

	ret := C.go_db_open(db, txn, file, nil, dbtype, flags, 0)
	return createError(ret)
}

// Open a database file.
func (handle *Db) Open(filename string, dbtype C.DBTYPE, flags C.u_int32_t) error {
	file := C.CString(filename)
	defer C.free(unsafe.Pointer(file))

	ret := C.go_db_open(handle.db, nil, file, nil, dbtype, flags, 0)
	return createError(ret)
}

// Close the database file.
func (handle *Db) Close() error {
	ret := C.go_db_close(handle.db, 0)
	return createError(ret)
}

// Flags returns the flags of the database connection.
func (handle *Db) Flags() (C.u_int32_t, error) {
	var flags C.u_int32_t
	ret := C.go_db_get_open_flags(handle.db, &flags)
	return flags, createError(ret)
}

// Remove the database.
func (handle *Db) Remove(filename string) error {
	file := C.CString(filename)
	defer C.free(unsafe.Pointer(file))

	ret := C.go_db_remove(handle.db, file)
	return createError(ret)
}

// Rename the database filename.
func (handle *Db) Rename(oldName, newName string) error {
	o := C.CString(oldName)
	defer C.free(unsafe.Pointer(o))
	n := C.CString(newName)
	defer C.free(unsafe.Pointer(n))

	ret := C.go_db_rename(handle.db, o, n)
	return createError(ret)
}

// Put a key/value pair into the database.
func (handle *Db) Put(key, value string) error {
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	v := C.CString(value)
	defer C.free(unsafe.Pointer(v))

	ret := C.go_db_put_string(handle.db, k, v, 0)
	return createError(ret)
}

// Get a value from the database by key.
func (handle *Db) Get(key string) (string, error) {
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	v := C.CString("")
	defer C.free(unsafe.Pointer(v))

	ret := C.go_db_get_string(handle.db, k, &v)
	return C.GoString(v), createError(ret)
}

// Delete a value from the database by key.
func (handle *Db) Delete(key string) error {
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))

	ret := C.go_db_del_string(handle.db, k)
	return createError(ret)
}

// Cursor returns a handle for the database cursor.
func (handle *Db) Cursor() (*Cursor, error) {
	var dbc *C.DBC
	if err := C.go_db_cursor(handle.db, &dbc); err > 0 {
		return nil, createError(err)
	}
	return &Cursor{dbc}, nil
}

// Get moves the cursor based on the mode and returns the key/value pair.
func (cursor *Cursor) Get(mode CursorMode) (key, value []byte, err error) {
	var k, v C.DBT
	ret := C.go_cursor_get(cursor.dbc, &k, &v, C.int(mode))
	key = C.GoBytes(unsafe.Pointer(k.data), C.int(k.size))
	value = C.GoBytes(unsafe.Pointer(v.data), C.int(v.size))
	err = createError(ret)
	return
}

// GetString moves the cursor based on the mode and returns the string key/value pair.
func (cursor *Cursor) GetString(mode CursorMode) (key, value string, err error) {
	k := C.CString("")
	defer C.free(unsafe.Pointer(k))
	v := C.CString("")
	defer C.free(unsafe.Pointer(v))

	ret := C.go_cursor_get_string(cursor.dbc, &k, &v, C.int(mode))
	return C.GoString(k), C.GoString(v), createError(ret)
}

// UTILITY FUNCTIONS

// Version returns the version of the database and binding.
func Version() string {
	libVersion := C.GoString(C.db_version(nil, nil, nil))
	return fmt.Sprintf("%v (Go bindings v%s)", libVersion, version)
}

// DBError contains the database Error.
type DBError struct {
	Code    int
	Message string
}

func createError(code C.int) error {
	if code == 0 {
		return nil
	}
	msg := C.db_strerror(code)
	e := DBError{int(code), C.GoString(msg)}
	return errors.New(e.Error())
}

// Error return the string representation of the error.
func (e *DBError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}
