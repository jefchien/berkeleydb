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

const version string = "0.0.6"

var (
	// ErrDatabaseClosed blocks all operations after Db.Close, Db.Rename,
	// or Db.Remove has been called.
	ErrDatabaseClosed = errors.New("database has been closed")
	// ErrCursorClosed blocks all operations after Cursor.Close.
	ErrCursorClosed = errors.New("cursor has been closed")
	// ErrOpened prevents Db.Open and variants from being
	// called multiple times.
	ErrOpened = errors.New("database already opened")
)

// Flags for opening a database or environment.
const (
	// DbCreate is used to create the database.
	DbCreate = C.DB_CREATE
	// DbExcl is used to return an error if the database already exists.
	// Only meaningful when specified with the DB_CREATE flag.
	DbExcl = C.DB_EXCL
	// DbRdOnly is used to open the database for reading only.
	// Any attempt to modify items in the database will fail.
	DbRdOnly = C.DB_RDONLY
	// DbTruncate is used to physically truncate the underlying file,
	// discarding all previous databases it might have held.
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
	db     *C.DB
	opened bool
}

// Cursor holds the current cursor position.
type Cursor struct {
	dbc *C.DBC
}

// NewDB initialises a new bdb connection.
func NewDB() (*Db, error) {
	var db *C.DB
	if ret := C.db_create(&db, nil, 0); ret != 0 {
		return nil, createError(ret)
	}
	return &Db{db: db}, nil
}

// NewDBInEnvironment initialises a new bdb connection in an environment.
func NewDBInEnvironment(env *Environment) (*Db, error) {
	var db *C.DB
	if ret := C.db_create(&db, env.environ, 0); ret != 0 {
		return nil, createError(ret)
	}
	return &Db{db: db}, nil
}

// Open a database file.
func (handle *Db) Open(filename string, dbtype C.DBTYPE, flags C.u_int32_t) error {
	return handle.OpenWithMode(filename, dbtype, flags, 0)
}

// OpenWithMode a database file with specific fs.FileMode. The mode is only used
// with DbCreate. If mode is 0, Berkeley DB will use a default mode of readable
// and writable by owner and readable by group (0640).
func (handle *Db) OpenWithMode(filename string, dbtype C.DBTYPE, flags C.u_int32_t, mode int) error {
	if handle.db == nil {
		return ErrDatabaseClosed
	}
	if handle.opened {
		return ErrOpened
	}
	file := C.CString(filename)
	defer C.free(unsafe.Pointer(file))

	ret := C.go_db_open(handle.db, nil, file, nil, dbtype, flags, C.int(mode))
	handle.opened = true
	return createError(ret)
}

// Close the database file. Operations will return
// ErrDatabaseClosed after.
func (handle *Db) Close() error {
	if handle.db == nil {
		return ErrDatabaseClosed
	}
	ret := C.go_db_close(handle.db, 0)
	handle.db = nil
	return createError(ret)
}

// Flags returns the flags of the database connection.
func (handle *Db) Flags() (C.u_int32_t, error) {
	if handle.db == nil {
		return 0, ErrDatabaseClosed
	}
	var flags C.u_int32_t
	ret := C.go_db_get_open_flags(handle.db, &flags)
	return flags, createError(ret)
}

// Remove the database. Must be done before calling
// Open. Operations will return ErrDatabaseClosed after.
func (handle *Db) Remove(filename string) error {
	if handle.db == nil {
		return ErrDatabaseClosed
	}
	file := C.CString(filename)
	defer C.free(unsafe.Pointer(file))

	ret := C.go_db_remove(handle.db, file)
	handle.db = nil
	return createError(ret)
}

// Rename the database filename. Must be done before
// calling Open. Operations will return ErrDatabaseClosed
// after.
func (handle *Db) Rename(oldName, newName string) error {
	if handle.db == nil {
		return ErrDatabaseClosed
	}
	o := C.CString(oldName)
	defer C.free(unsafe.Pointer(o))
	n := C.CString(newName)
	defer C.free(unsafe.Pointer(n))

	ret := C.go_db_rename(handle.db, o, n)
	handle.db = nil
	return createError(ret)
}

// Put a key/value pair into the database.
func (handle *Db) Put(key, value string) error {
	if handle.db == nil {
		return ErrDatabaseClosed
	}
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	v := C.CString(value)
	defer C.free(unsafe.Pointer(v))

	ret := C.go_db_put_string(handle.db, k, v, 0)
	return createError(ret)
}

// Get a value from the database by key.
func (handle *Db) Get(key string) (string, error) {
	if handle.db == nil {
		return "", ErrDatabaseClosed
	}
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	v := C.CString("")
	defer C.free(unsafe.Pointer(v))

	ret := C.go_db_get_string(handle.db, k, &v)
	return C.GoString(v), createError(ret)
}

// Delete a value from the database by key.
func (handle *Db) Delete(key string) error {
	if handle.db == nil {
		return ErrDatabaseClosed
	}
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))

	ret := C.go_db_del_string(handle.db, k)
	return createError(ret)
}

// Cursor returns a handle for the database cursor.
func (handle *Db) Cursor() (*Cursor, error) {
	if handle.db == nil {
		return nil, ErrDatabaseClosed
	}
	var dbc *C.DBC
	if err := C.go_db_cursor(handle.db, &dbc); err != 0 {
		return nil, createError(err)
	}
	return &Cursor{dbc}, nil
}

// Get moves the cursor based on the mode and returns the key/value pair.
// Returns a DB_NOTFOUND error if there are no additional records in
// the database.
func (cursor *Cursor) Get(mode CursorMode) (key, value []byte, err error) {
	if cursor.dbc == nil {
		return nil, nil, ErrCursorClosed
	}
	var k, v C.DBT
	ret := C.go_cursor_get(cursor.dbc, &k, &v, C.int(mode))
	key = C.GoBytes(unsafe.Pointer(k.data), C.int(k.size))
	value = C.GoBytes(unsafe.Pointer(v.data), C.int(v.size))
	err = createError(ret)
	return
}

// GetString is a convenience function that calls Get and converts
// the key/value pair into strings.
func (cursor *Cursor) GetString(mode CursorMode) (string, string, error) {
	k, v, err := cursor.Get(mode)
	return string(k), string(v), err
}

// Close closes the database cursor. Cursor operations will return
// ErrCursorClosed after.
func (cursor *Cursor) Close() error {
	if cursor.dbc == nil {
		return ErrCursorClosed
	}
	ret := C.go_cursor_close(cursor.dbc)
	cursor.dbc = nil
	return createError(ret)
}

// UTILITY FUNCTIONS

// Version returns the version of the database and binding.
func Version() string {
	libVersion := C.GoString(C.db_version(nil, nil, nil))
	return fmt.Sprintf("%v (Go bindings v%s)", libVersion, version)
}

// DBError contains the database error.
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
