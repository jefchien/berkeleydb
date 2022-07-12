// Copyright 2013 Matt Butcher
// MIT License
// This file contains a number of wrapper functions that make it
// possible for Go to call the method-style functions on BerkeleyDB
// structs.

#include <db.h>
#include <string.h>

int go_db_open(DB* dbp, DB_TXN* txn_id, char* filename, char* dbname,
               DBTYPE type, u_int32_t flags, int mode) {
  return dbp->open(dbp, txn_id, filename, dbname, type, flags, mode);
}

int go_db_close(DB* dbp, u_int32_t flags) {
  if (dbp == NULL) return 0;

  return dbp->close(dbp, flags);
}

int go_db_get_open_flags(DB* dbp, u_int32_t* open_flags) {
  return dbp->get_open_flags(dbp, open_flags);
}

int go_db_remove(DB* dbp, char* filename) {
  return dbp->remove(dbp, filename, NULL, 0);
}

int go_db_rename(DB* dbp, char* filename, char* new_name) {
  return dbp->rename(dbp, filename, NULL, new_name, 0);
}

int go_env_open(DB_ENV* env, char* dirname, u_int32_t flags, u_int32_t mode) {
  return env->open(env, dirname, flags, mode);
}

int go_env_close(DB_ENV* env, u_int32_t flags) {
  return env->close(env, flags);
}

int go_db_put_string(DB* dbp, char* key, char* value, u_int32_t flags) {
  // Create two DBT records and initialize them.
  DBT k, v;
  memset(&k, 0, sizeof(k));
  memset(&v, 0, sizeof(v));

  // Store the two strings in the DBT records
  k.data = key;
  k.size = strlen(key);

  v.data = value;
  v.size = strlen(value);

  // Put the record into the database.
  return dbp->put(dbp, NULL, &k, &v, flags);
}

int go_db_get_string(DB* dbp, char* key, char** value) {
  // Create two DBT records and initialize them.
  DBT k, v;
  memset(&k, 0, sizeof(k));
  memset(&v, 0, sizeof(v));

  k.data = key;
  k.size = strlen(key);

  // Allocates memory for returned value data.
  v.flags = DB_DBT_MALLOC;
  *value = NULL;

  int ret = dbp->get(dbp, NULL, &k, &v, 0);
  if (ret == 0) {
    *value = (char*)v.data;
  }
  return ret;
}

int go_db_del_string(DB* dbp, char* key) {
  DBT k;
  memset(&k, 0, sizeof(k));

  k.data = key;
  k.size = strlen(key);

  return dbp->del(dbp, NULL, &k, 0);
}

int go_db_cursor(DB* dbp, DBC** dbcp) {
  return dbp->cursor(dbp, NULL, dbcp, 0);
}

int go_cursor_get(DBC* dbcp, DBT* key, DBT* value, int mode) {
  return dbcp->c_get(dbcp, key, value, mode);
}

int go_cursor_close(DBC* dbcp) {
  return dbcp->c_close(dbcp);
}
