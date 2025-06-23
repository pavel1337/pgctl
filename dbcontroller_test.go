// postgresctl/dbcontroller_test.go
package postgresctl

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var (
	// testDB       = "test_db"
	// testUser     = "test_user"
	// testPassword = "test_password"
	pc = PostgresConn{
		Host:     "localhost",
		Port:     55432,
		Username: "postgres",
		Password: "password",
		Database: "postgres",
	}
)

func testDB() string {
	return fmt.Sprintf("test_db_%d", rand.Intn(1000000))
}

func testUser() string {
	return fmt.Sprintf("test_user_%d", rand.Intn(1000000))
}

func testPassword() string {
	return fmt.Sprintf("test_password_%d", rand.Intn(1000000))
}

func generateTestNames() []string {
	return []string{"test1", "test2", "test3", "test4_test", "test5_test"}
}

func createTestController() *PostgresController {
	c, err := NewPostgresController(pc)
	if err != nil {
		panic(err)
	}
	return c
}

func TestPostgresController_CreateDatabase(t *testing.T) {
	testDB := testDB()

	c := createTestController()
	defer c.Close()

	err := c.CreateDatabase(testDB)
	assert.NoError(t, err)

	err = c.CreateDatabase(testDB)
	assert.Error(t, err)
	assert.Equal(t, ErrDBExists, err)

	err = c.CreateDatabase("")
	assert.Error(t, err)

	for _, name := range baseDBs {
		err = c.CreateDatabase(name)
		assert.Error(t, err)
	}

	err = c.DeleteDatabase(testDB)
	assert.NoError(t, err)
}

func TestPostgresController_DeleteDatabase(t *testing.T) {
	testDB := testDB()

	c := createTestController()
	defer c.Close()

	err := c.DeleteDatabase(testDB)
	assert.Error(t, err)

	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)

	err = c.DeleteDatabase(testDB)
	assert.NoError(t, err)

	err = c.DeleteDatabase("")
	assert.Error(t, err)

	for _, name := range baseDBs {
		err = c.DeleteDatabase(name)
		assert.Error(t, err)
	}
}

func TestPostgresController_ListDatabases(t *testing.T) {
	c := createTestController()
	defer c.Close()

	dbs, err := c.ListDatabases()
	assert.NoError(t, err)

	for _, name := range generateTestNames() {
		err = c.CreateDatabase(name)
		assert.NoError(t, err)
	}

	dbs, err = c.ListDatabases()
	assert.NoError(t, err)

	// We can't assert exact equality because there might be other databases
	// So we just check that our test databases are included
	for _, name := range generateTestNames() {
		assert.Contains(t, dbs, name)
	}

	for _, name := range generateTestNames() {
		err = c.DeleteDatabase(name)
		assert.NoError(t, err)
	}
}

func TestPostgresController_DatabaseExists(t *testing.T) {
	testDB := testDB()

	c := createTestController()
	defer c.Close()

	exists, err := c.DatabaseExists(testDB)
	assert.NoError(t, err)
	assert.False(t, exists)

	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)

	exists, err = c.DatabaseExists(testDB)
	assert.NoError(t, err)
	assert.True(t, exists)

	err = c.DeleteDatabase(testDB)
	assert.NoError(t, err)
}

func TestPostgresController_Size(t *testing.T) {
	testDB := testDB()

	c := createTestController()
	defer c.Close()

	_, err := c.Size(testDB)
	assert.Error(t, err)

	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)
	defer c.DeleteDatabase(testDB)

	size, err := c.Size(testDB)
	assert.NoError(t, err)
	assert.True(t, size > 0)
}

func TestPostgresController_Tables(t *testing.T) {
	testDB := testDB()

	c := createTestController()
	defer c.Close()

	_, err := c.Tables(testDB)
	assert.Error(t, err)

	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)
	defer c.DeleteDatabase(testDB)

	tables, err := c.Tables(testDB)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tables))

	// Connect to the test database and create a table
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://postgres:password@localhost:55432/%s?sslmode=disable", testDB))
	assert.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(255))")
	assert.NoError(t, err)

	tables, err = c.Tables(testDB)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tables))
	assert.Contains(t, tables, "test")
}

func randomString(n int) string {
	if n <= 0 {
		return ""
	}

	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestPostgresController_TransferDatabaseOwnership(t *testing.T) {
	dbName := testDB()
	userName := testUser()
	password := testPassword()

	c := createTestController()
	defer c.Close()

	// Create DB
	err := c.CreateDatabase(dbName)
	assert.NoError(t, err)
	defer c.DeleteDatabase(dbName)

	// Create user
	err = c.CreateUserWithMaxConn(userName, password, 5)
	assert.NoError(t, err)
	defer c.DeleteUser(userName)

	// Transfer ownership
	err = c.TransferDatabaseOwnership(dbName, userName)
	assert.NoError(t, err)

	// Verify via query
	var owner string
	err = c.db.QueryRow(`SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_database WHERE datname = $1`, dbName).Scan(&owner)
	assert.NoError(t, err)
	assert.Equal(t, userName, owner)
}

func TestPostgresController_TransferPublicSchemaOwnership(t *testing.T) {
	dbName := testDB()
	userName := testUser()
	password := testPassword()

	c := createTestController()
	defer c.Close()

	// Create DB
	err := c.CreateDatabase(dbName)
	assert.NoError(t, err)
	defer c.DeleteDatabase(dbName)

	// Create user
	err = c.CreateUserWithMaxConn(userName, password, 5)
	assert.NoError(t, err)
	defer c.DeleteUser(userName)

	// Transfer schema ownership
	err = c.TransferPublicSchemaOwnership(dbName, userName)
	assert.NoError(t, err)

	// Check ownership in connected DB
	perDbConn := pc
	perDbConn.Database = dbName
	db, err := sql.Open("postgres", perDbConn.connStr())
	assert.NoError(t, err)
	defer db.Close()

	var schemaOwner string
	err = db.QueryRow(`SELECT schema_owner FROM information_schema.schemata WHERE schema_name = 'public'`).Scan(&schemaOwner)
	assert.NoError(t, err)
	assert.Equal(t, userName, schemaOwner)
}
