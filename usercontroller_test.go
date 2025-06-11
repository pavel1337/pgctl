// postgresctl/usercontroller_test.go
package postgresctl

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestPostgresController_CreateUser(t *testing.T) {
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	err := c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)

	// Verify user can connect
	err = openPostgres(testUser, testPassword, "postgres")
	assert.NoError(t, err)

	err = c.CreateUser(testUser, testPassword)
	assert.Error(t, err)
	assert.Equal(t, ErrUserExists, err)

	err = c.CreateUser("", testPassword)
	assert.Error(t, err)

	err = c.CreateUser(testUser, "")
	assert.Error(t, err)

	for _, name := range baseUsers {
		err = c.CreateUser(name, testPassword)
		assert.Error(t, err)
	}

	err = c.DeleteUser(testUser)
	assert.NoError(t, err)
}

func TestPostgresController_UpdateUserPassword(t *testing.T) {
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	err := c.UpdateUserPassword(testUser, testPassword)
	assert.Error(t, err)
	assert.Equal(t, ErrUserDoesNotExist, err)

	err = c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	err = openPostgres(testUser, testPassword, "postgres")
	assert.NoError(t, err)

	newPassword := "new_password"
	err = c.UpdateUserPassword(testUser, newPassword)
	assert.NoError(t, err)

	err = openPostgres(testUser, newPassword, "postgres")
	assert.NoError(t, err)

	err = c.UpdateUserPassword("", testPassword)
	assert.Error(t, err)

	err = c.UpdateUserPassword(testUser, "")
	assert.Error(t, err)

	for _, name := range baseUsers {
		err = c.UpdateUserPassword(name, testPassword)
		assert.Error(t, err)
	}
}

func TestPostgresController_DeleteUser(t *testing.T) {
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	// Deleting a non-existent user should error
	err := c.DeleteUser(testUser)
	assert.Error(t, err)

	// Create user
	err = c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)

	_, err = c.db.Exec(fmt.Sprintf(`GRANT CREATE ON SCHEMA public TO "%s"`, testUser))
	assert.NoError(t, err)

	// Connect as the new user and create an object (e.g., a table)
	err = openPostgres(testUser, testPassword, "postgres") // use existing DB for object creation
	assert.NoError(t, err)

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@localhost:55432/postgres?sslmode=disable", testUser, testPassword))
	assert.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_owned_table (id INT);`)
	assert.NoError(t, err)
	db.Close()

	// Now delete the user (should succeed because DROP OWNED is called)
	err = c.DeleteUser(testUser)
	assert.NoError(t, err)

	// Verify the user is actually gone (can't connect)
	err = openPostgres(testUser, testPassword, "")
	assert.Error(t, err)

	// Invalid username should error
	err = c.DeleteUser("")
	assert.Error(t, err)

	// Attempting to delete base users (e.g., postgres) should fail
	for _, name := range baseUsers {
		err = c.DeleteUser(name)
		assert.Error(t, err)
	}
}

func TestPostgresController_ListUsers(t *testing.T) {
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	names, err := c.ListUsers()
	assert.NoError(t, err)

	err = c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	namesAfter, err := c.ListUsers()
	assert.NoError(t, err)
	assert.Equal(t, len(names)+1, len(namesAfter))
}

func openPostgres(username, password, database string) error {
	connStr := fmt.Sprintf("postgres://%s:%s@localhost:55432/%s?sslmode=disable",
		username, password, database)

	// fmt.Println(connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error creating database connection: %s", err)
	}
	defer db.Close()

	return db.Ping()
}

func TestPostgresController_UserExists(t *testing.T) {
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	_, err := c.UserExists("")
	assert.Error(t, err)

	exists, err := c.UserExists(testUser)
	assert.NoError(t, err)
	assert.False(t, exists)

	err = c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	exists, err = c.UserExists(testUser)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestPostgresController_UsersMaxConnHandling(t *testing.T) {
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	err := c.CreateUserWithMaxConn(testUser, testPassword, 1)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	err = openPostgres(testUser, testPassword, "postgres")
	assert.NoError(t, err)

	err = c.CreateUserWithMaxConn(testUser, testPassword, 1)
	assert.Error(t, err)

	maxConn, err := c.GetUserMaxConn(testUser)
	assert.NoError(t, err)
	assert.Equal(t, 1, maxConn)

	err = c.UpdateUserMaxConn(testUser, 2)
	assert.NoError(t, err)

	maxConn, err = c.GetUserMaxConn(testUser)
	assert.NoError(t, err)
	assert.Equal(t, 2, maxConn)

	// Test unlimited connections (-1 in PostgreSQL)
	err = c.UpdateUserMaxConn(testUser, -1)
	assert.NoError(t, err)

	maxConn, err = c.GetUserMaxConn(testUser)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxConn)
}
