package berkeleydb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jefchien/berkeleydb"
)

func TestNewEnvironment(t *testing.T) {
	env, err := berkeleydb.NewEnvironment()
	require.NoError(t, err)
	require.NotNil(t, env)
}

func TestOpenEnvironment(t *testing.T) {
	env, err := berkeleydb.NewEnvironment()
	require.NoError(t, err)
	err = env.Open(t.TempDir(), berkeleydb.DbCreate|berkeleydb.DbInitMpool, 0)
	require.NoError(t, err)
	require.NoError(t, env.Close())
}

func TestOpenDBInEnvironment(t *testing.T) {
	testDir := t.TempDir()
	env, err := berkeleydb.NewEnvironment()
	require.NoError(t, err)
	err = env.Open(testDir, berkeleydb.DbCreate|berkeleydb.DbInitMpool, 0755)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, env.Close())
	})

	// Now create, open, and close a DB
	db, err := berkeleydb.NewDBInEnvironment(env)
	require.NoError(t, err)

	err = db.Open(testFilename, berkeleydb.DbBtree, berkeleydb.DbCreate)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	// Test that the DB file was actually created.
	_, err = os.Stat(filepath.Join(testDir, testFilename))
	require.NoError(t, err)
}
