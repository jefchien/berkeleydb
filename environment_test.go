package berkeleydb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jefchien/berkeleydb"
)

func TestNewEnvironment(t *testing.T) {
	testDir := t.TempDir()
	_, err := os.Stat(testDir)
	if err != nil && os.IsNotExist(err) {
		e := os.Mkdir(testDir, os.ModeDir|os.ModePerm)
		if e != nil {
			t.Fatalf("Failed to create directory: %v", e)
		}
	}

	_, err = berkeleydb.NewEnvironment()

	if err != nil {
		t.Errorf("Expected environment, got %v", err)
	}

}

func TestOpenEnvironment(t *testing.T) {
	env, _ := berkeleydb.NewEnvironment()
	err := env.Open(t.TempDir(), berkeleydb.DbCreate|berkeleydb.DbInitMpool, 0)
	if err != nil {
		t.Errorf("Expected to open DB, got %v", err)
	}

	err = env.Close()
	if err != nil {
		t.Errorf("Expected to close DB, got %v", err)
	}
}

func TestOpenDBInEnvironment(t *testing.T) {
	testDir := t.TempDir()
	env, _ := berkeleydb.NewEnvironment()
	err := env.Open(testDir, berkeleydb.DbCreate|berkeleydb.DbInitMpool, 0755)
	if err != nil {
		t.Errorf("Expected to open DB, got %v", err)
		return
	}

	// Now create, open, and close a DB
	db, err := berkeleydb.NewDBInEnvironment(env)
	if err != nil {
		t.Errorf("Expected to create new DB: %v", err)
	}

	err = db.Open(TestFilename, berkeleydb.DbBtree, berkeleydb.DbCreate)
	if err != nil {
		t.Errorf("Expected to open DB, got %v", err)
	}

	// Test that the DB file was actually created.
	_, err = os.Stat(filepath.Join(testDir, TestFilename))
	if err != nil {
		t.Errorf("Expected to stat .db, got %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("Expected to close the DB, got %v", err)
	}

	err = env.Close()
	if err != nil {
		t.Errorf("Expected to close DB, got %v", err)
	}
}
