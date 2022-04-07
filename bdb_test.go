package berkeleydb_test

import (
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

func TestOpen(t *testing.T) {
	db, err := berkeleydb.NewDB()
	require.NoError(t, err)
	err = db.Open(testFilename, berkeleydb.DbBtree, berkeleydb.DbCreate)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	flags, err := db.Flags()
	require.NoError(t, err)
	require.Equal(t, berkeleydb.DbCreate, int(flags))
}

func openDB() (*berkeleydb.Db, error) {
	db, err := berkeleydb.NewDB()
	if err != nil {
		return nil, err
	}
	err = db.Open(testFilename, berkeleydb.DbBtree, berkeleydb.DbCreate)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TestPut(t *testing.T) {
	db, err := openDB()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.Put(testKey, testValue))
}

func TestGet(t *testing.T) {
	db, err := openDB()
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
	db, err := openDB()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.Put(testKey, testValue))
	require.NoError(t, db.Delete(testKey))
	require.Error(t, db.Delete(testNoSuchKey))
}

func TestRemove(t *testing.T) {
	db, err := berkeleydb.NewDB()
	require.NoError(t, err)
	require.NoError(t, db.Remove(testFilename))
}

func TestRename(t *testing.T) {
	db, err := berkeleydb.NewDB()
	require.NoError(t, err)
	require.NoError(t, db.Open(testFilename, berkeleydb.DbHash, berkeleydb.DbCreate))
	require.NoError(t, db.Close())

	db, err = berkeleydb.NewDB()
	require.NoError(t, err)

	newName := "foo_" + testFilename
	require.NoError(t, db.Rename(testFilename, newName))

	db, err = berkeleydb.NewDB()
	require.NoError(t, db.Remove(newName))
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
		testKey:   testValue,
		testValue: testKey,
	}
	for k, v := range expected {
		require.NoError(t, db.Put(k, v))
	}

	cursor, err := db.Cursor()
	require.NoError(t, err)

	actual := make(map[string]string)
	for {
		k, v, err := cursor.GetString(berkeleydb.DbNext)
		if err != nil {
			break
		}
		actual[k] = v
	}
	require.Equal(t, len(expected), len(actual))
	require.Equal(t, expected, actual)
}
