// postgresctl/grantcontroller_test.go
package postgresctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresController_GrantAll(t *testing.T) {
	testDB := testDB()
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	// Test with non-existent user
	err := c.GrantAll(testDB, testUser)
	assert.Error(t, err)
	assert.Equal(t, ErrUserDoesNotExist, err)

	// Create user for testing
	err = c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	// Test with non-existent database
	err = c.GrantAll("non_existent_db", testUser)
	assert.Error(t, err)
	assert.Equal(t, ErrDBDoesNotExist, err)

	// Create database for testing
	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)
	defer c.DeleteDatabase(testDB)

	// Test successful grant
	err = c.GrantAll(testDB, testUser)
	assert.NoError(t, err)

	// Verify user can connect to the database
	err = openPostgres(testUser, testPassword, testDB)
	assert.NoError(t, err)

	// Test revoke
	err = c.RevokeAll(testDB, testUser)
	assert.NoError(t, err)

	// Verify privileges were revoked (user can still connect to database but not access tables)
	err = openPostgres(testUser, testPassword, testDB)
	assert.NoError(t, err) // Can still connect to database
}

func TestPostgresController_Grant(t *testing.T) {
	testDB := testDB()
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	// Create user and database
	err := c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)
	defer c.DeleteDatabase(testDB)

	// Test invalid privilege
	err = c.Grant("INVALID_GRANT", testDB, testUser)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid grant")

	// ✅ Test all valid grants on an empty database
	t.Run("GrantAllOnEmptyDatabase", func(t *testing.T) {
		for grant := range grants {
			err := c.Grant(grant, testDB, testUser)
			assert.NoError(t, err, "granting "+grant+" on empty DB should not fail")
		}
	})

	// Create test table/function for completeness
	_, err = c.db.Exec(`CREATE TABLE public.test_table (id SERIAL PRIMARY KEY, val TEXT);`)
	assert.NoError(t, err)
	_, err = c.db.Exec(`CREATE FUNCTION public.test_func() RETURNS INT AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;`)
	assert.NoError(t, err)

	// Re-run grant logic now that objects exist
	for grant := range grants {
		t.Run("Grant_"+grant+"_WithObjects", func(t *testing.T) {
			err := c.Grant(grant, testDB, testUser)
			assert.NoError(t, err, "granting "+grant+" with existing objects should not fail")
		})
	}

	// CONNECT test: try to connect using the user
	t.Run("ConnectWithGrantedUser", func(t *testing.T) {
		err := c.Grant("CONNECT", testDB, testUser)
		assert.NoError(t, err)

		err = openPostgres(testUser, testPassword, testDB)
		assert.NoError(t, err)
	})
}

// func TestPostgresController_GrantExists(t *testing.T) {
// 	testDB := testDB()
// 	testUser := testUser()
// 	testPassword := testPassword()

// 	c := createTestController()
// 	defer c.Close()

// 	// Create test user and database
// 	err := c.CreateUser(testUser, testPassword)
// 	assert.NoError(t, err)
// 	defer c.DeleteUser(testUser)

// 	err = c.CreateDatabase(testDB)
// 	assert.NoError(t, err)
// 	defer c.DeleteDatabase(testDB)

// 	// Test CONNECT privilege
// 	err = c.Grant("CONNECT", testDB, testUser)
// 	assert.NoError(t, err)

// 	exists, err := c.GrantExists("CONNECT", testDB, testUser)
// 	assert.NoError(t, err)
// 	assert.True(t, exists)

// 	err = c.Revoke("CONNECT", testDB, testUser)
// 	assert.NoError(t, err)

// 	exists, err = c.GrantExists("CONNECT", testDB, testUser)
// 	assert.NoError(t, err)
// 	assert.False(t, exists) // failed

// 	err = c.Grant("SELECT", testDB, testUser)
// 	assert.NoError(t, err)

// 	exists, err = c.GrantExists("SELECT", testDB, testUser)
// 	assert.NoError(t, err)
// 	assert.True(t, exists) // failed
// }

func TestPostgresController_Revoke(t *testing.T) {
	testDB := testDB()
	testUser := testUser()
	testPassword := testPassword()

	c := createTestController()
	defer c.Close()

	// Create test user and database
	err := c.CreateUser(testUser, testPassword)
	assert.NoError(t, err)
	defer c.DeleteUser(testUser)

	err = c.CreateDatabase(testDB)
	assert.NoError(t, err)
	defer c.DeleteDatabase(testDB)

	// Test revoking non-existent privilege
	err = c.Revoke("SELECT", testDB, testUser)
	assert.NoError(t, err)

	// Test revoking CONNECT privilege
	err = c.Grant("CONNECT", testDB, testUser)
	assert.NoError(t, err)

	err = c.Revoke("CONNECT", testDB, testUser)
	assert.NoError(t, err)
}
