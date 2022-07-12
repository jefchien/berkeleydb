package berkeleydb_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jefchien/berkeleydb"
)

const (
	testFilename  = "test_db.db"
	testKey       = "key"
	testValue     = "value"
	testNoSuchKey = "no_such_key"
)

func TestNewDB(t *testing.T) {
	db, err := berkeleydb.NewDB()
	require.NoError(t, err)
	require.NotNil(t, db)
}

func TestVersion(t *testing.T) {
	require.NotNil(t, berkeleydb.Version())
}

func TestOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), testFilename)
	db, err := berkeleydb.NewDB()
	require.NoError(t, err)
	err = db.Open(path, berkeleydb.DbBtree, berkeleydb.DbCreate)
	require.NoError(t, err)
	flags, err := db.Flags()
	require.NoError(t, err)
	require.Equal(t, berkeleydb.DbCreate, int(flags))
	err = db.Open("", berkeleydb.DbQueue, berkeleydb.DbCreate|berkeleydb.DbExcl)
	require.ErrorIs(t, err, berkeleydb.ErrOpened)
	require.NoError(t, db.Close())
}

func TestClose(t *testing.T) {
	db, err := setup(t)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	var errs []error
	errs = append(errs, db.Close())
	errs = append(errs, db.Rename("", ""))
	errs = append(errs, db.Remove(""))
	errs = append(errs, db.Put("", ""))
	errs = append(errs, db.Delete(""))
	err = db.Open("", berkeleydb.DbUnknown, berkeleydb.DbRdOnly)
	errs = append(errs, err)
	_, err = db.Flags()
	errs = append(errs, err)
	_, err = db.Get("")
	errs = append(errs, err)
	_, err = db.Cursor()
	errs = append(errs, err)

	for _, err = range errs {
		require.ErrorIs(t, err, berkeleydb.ErrDatabaseClosed)
	}
}

func TestPut(t *testing.T) {
	db, err := setup(t)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.Put(testKey, testValue))
}

func TestGet(t *testing.T) {
	db, err := setup(t)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.Put(testKey, testValue))

	val, err := db.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, testValue, val)

	val, err = db.Get(testNoSuchKey)
	require.Error(t, err)
}

func TestDelete(t *testing.T) {
	db, err := setup(t)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.Put(testKey, testValue))
	require.NoError(t, db.Delete(testKey))
	require.Error(t, db.Delete(testNoSuchKey))
}

func TestRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), testFilename)
	db, err := setupWith(t, path)
	require.NoError(t, err)
	// cannot call remove after open has been called
	require.Error(t, db.Remove(path))
	// cannot access db handle after remove
	require.ErrorIs(t, db.Close(), berkeleydb.ErrDatabaseClosed)

	db, err = berkeleydb.NewDB()
	require.NoError(t, err)
	require.NoError(t, db.Remove(path))
}

func TestRename(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, testFilename)
	db, err := setupWith(t, oldPath)
	require.NoError(t, err)
	newPath := filepath.Join(dir, "foo_"+testFilename)
	// cannot call remove after open has been called
	require.Error(t, db.Rename(oldPath, newPath))
	// cannot access db handle after rename
	require.ErrorIs(t, db.Close(), berkeleydb.ErrDatabaseClosed)

	db, err = berkeleydb.NewDB()
	require.NoError(t, err)
	require.NoError(t, db.Rename(oldPath, newPath))
	_, err = os.Stat(oldPath)
	require.Error(t, err)
	require.ErrorIs(t, err, fs.ErrNotExist)
	_, err = os.Stat(newPath)
	require.NoError(t, err)
}

func TestCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), testFilename)
	db, err := berkeleydb.NewDB()
	require.NoError(t, err)
	err = db.Open(path, berkeleydb.DbHash, berkeleydb.DbCreate)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	expected := map[string]string{
		testKey:             testValue,
		testValue:           testKey,
		testKey + testValue: testFilename,
	}
	for k, v := range expected {
		require.NoError(t, db.Put(k, v))
	}

	cursor, err := db.Cursor()
	require.NoError(t, err)

	actual := make(map[string]string)
	for {
		var k, v string
		k, v, err = cursor.GetString(berkeleydb.DbNext)
		if err != nil {
			break
		}
		actual[k] = v
	}
	require.NoError(t, cursor.Close())
	require.Equal(t, len(expected), len(actual))
	require.Equal(t, expected, actual)
	// cannot close again
	require.ErrorIs(t, cursor.Close(), berkeleydb.ErrCursorClosed)
	_, _, err = cursor.GetString(berkeleydb.DbPrev)
	require.ErrorIs(t, err, berkeleydb.ErrCursorClosed)

	cursor, err = db.Cursor()
	require.NoError(t, err)
	_, _, err = cursor.GetString(berkeleydb.DbFirst)
	require.NoError(t, err)
	_, _, err = cursor.GetString(berkeleydb.DbLast)
	require.NoError(t, err)
	require.NoError(t, cursor.Close())
}

func TestOpenWithMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), testFilename)
	testCases := []struct {
		mode int
		want string
	}{
		{0, "0640"},
		{0644, "0644"},
		{0600, "0600"},
		{0755, "0755"},
	}
	for _, testCase := range testCases {
		db, err := berkeleydb.NewDB()
		require.NoError(t, err)
		err = db.OpenWithMode(path, berkeleydb.DbRecno, berkeleydb.DbCreate|berkeleydb.DbTruncate, testCase.mode)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		info, err := os.Lstat(path)
		require.NoError(t, err)
		require.Equal(t, testCase.want, fmt.Sprintf("%04o", info.Mode().Perm()))
		require.NoError(t, os.Remove(path))
	}
}

func setup(t *testing.T) (*berkeleydb.Db, error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), testFilename)
	return setupWith(t, path)
}

func setupWith(t *testing.T, filename string) (*berkeleydb.Db, error) {
	t.Helper()

	db, err := berkeleydb.NewDB()
	if err != nil {
		return nil, err
	}
	err = db.Open(filename, berkeleydb.DbHash, berkeleydb.DbCreate)
	if err != nil {
		return nil, err
	}
	return db, nil
}
