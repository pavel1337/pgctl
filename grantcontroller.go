// postgresctl/grantcontroller.go
package postgresctl

import (
	"fmt"
	"strings"
)

type GrantController interface {
	Grant(grantName, dbName, username string) error
	// Limitation in PostgreSQL: we can't check if a specific privilege exists for a user on a database
	// GrantExists(grantName, dbName, username string) (bool, error)
	GrantAll(dbName, username string) error
	RevokeAll(dbName, username string) error
	Revoke(grantName, dbName, username string) error
}

var _ GrantController = &PostgresController{}

var (
	ErrInvalidGrant = fmt.Errorf("invalid grant")
)

// Standard PostgreSQL privileges we want to support
var grants = map[string]string{
	"SELECT":     "SELECT",
	"INSERT":     "INSERT",
	"UPDATE":     "UPDATE",
	"DELETE":     "DELETE",
	"TRUNCATE":   "TRUNCATE",
	"REFERENCES": "REFERENCES",
	"TRIGGER":    "TRIGGER",
	"CONNECT":    "CONNECT",
	"TEMPORARY":  "TEMPORARY",
	"EXECUTE":    "EXECUTE",
	"USAGE":      "USAGE",
	"CREATE":     "CREATE",
}

func (c *PostgresController) GrantAll(dbName, username string) error {
	if err := validateDBName(dbName); err != nil {
		return fmt.Errorf("error validating database name: %w", err)
	}
	if err := validateUsername(username); err != nil {
		return fmt.Errorf("error validating username: %w", err)
	}

	// Check existence
	if exists, err := c.UserExists(username); err != nil {
		return fmt.Errorf("error checking user: %w", err)
	} else if !exists {
		return ErrUserDoesNotExist
	}
	if exists, err := c.DatabaseExists(dbName); err != nil {
		return fmt.Errorf("error checking database: %w", err)
	} else if !exists {
		return ErrDBDoesNotExist
	}

	// Grant database and schema access
	if _, err := c.db.Exec(`GRANT CONNECT ON DATABASE "` + dbName + `" TO "` + username + `"`); err != nil {
		return fmt.Errorf("grant CONNECT failed: %w", err)
	}
	if _, err := c.db.Exec(`GRANT USAGE, CREATE ON SCHEMA public TO "` + username + `"`); err != nil {
		return fmt.Errorf("grant SCHEMA privileges failed: %w", err)
	}

	// Grant all privileges on existing objects
	if _, err := c.db.Exec(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "` + username + `"`); err != nil {
		return fmt.Errorf("grant TABLES failed: %w", err)
	}
	if _, err := c.db.Exec(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "` + username + `"`); err != nil {
		return fmt.Errorf("grant SEQUENCES failed: %w", err)
	}

	// Grant future access via default privileges
	if _, err := c.db.Exec(`
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
		GRANT ALL PRIVILEGES ON TABLES TO "` + username + `"`); err != nil {
		return fmt.Errorf("default privileges for TABLES failed: %w", err)
	}
	if _, err := c.db.Exec(`
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
		GRANT ALL PRIVILEGES ON SEQUENCES TO "` + username + `"`); err != nil {
		return fmt.Errorf("default privileges for SEQUENCES failed: %w", err)
	}

	return nil
}

func (c *PostgresController) RevokeAll(dbName, username string) error {
	if err := validateDBName(dbName); err != nil {
		return fmt.Errorf("error validating database name: %w", err)
	}
	if err := validateUsername(username); err != nil {
		return fmt.Errorf("error validating username: %w", err)
	}

	// Revoke CONNECT
	if _, err := c.db.Exec(`REVOKE CONNECT ON DATABASE "` + dbName + `" FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking CONNECT: %w", err)
	}

	// Revoke existing object privileges
	if _, err := c.db.Exec(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking TABLE privileges: %w", err)
	}
	if _, err := c.db.Exec(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking SEQUENCE privileges: %w", err)
	}

	// Revoke schema-level privileges
	if _, err := c.db.Exec(`REVOKE USAGE, CREATE ON SCHEMA public FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking schema privileges: %w", err)
	}

	// Revoke default privileges for future tables/sequences
	if _, err := c.db.Exec(`
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
		REVOKE ALL PRIVILEGES ON TABLES FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking default TABLE privileges: %w", err)
	}
	if _, err := c.db.Exec(`
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
		REVOKE ALL PRIVILEGES ON SEQUENCES FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking default SEQUENCE privileges: %w", err)
	}

	return nil
}

func (c *PostgresController) Grant(grantName, dbName, username string) error {
	if err := validateDBName(dbName); err != nil {
		return fmt.Errorf("error validating database name: %w", err)
	}
	if err := validateUsername(username); err != nil {
		return fmt.Errorf("error validating username: %w", err)
	}
	grantName = strings.ToUpper(grantName)
	if err := validateGrant(grantName); err != nil {
		return fmt.Errorf("error validating grant: %w", err)
	}

	switch grantName {
	case "CONNECT", "TEMPORARY":
		_, err := c.db.Exec(`GRANT ` + grantName + ` ON DATABASE "` + dbName + `" TO "` + username + `"`)
		return err

	case "USAGE":
		_, err := c.db.Exec(`GRANT USAGE ON SCHEMA public TO "` + username + `"`)
		if err != nil {
			return fmt.Errorf("error granting schema USAGE: %w", err)
		}
		_, err = c.db.Exec(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO "` + username + `"`)
		return err

	case "CREATE":
		_, err := c.db.Exec(`GRANT CREATE ON SCHEMA public TO "` + username + `"`)
		if err != nil {
			return fmt.Errorf("error granting schema CREATE: %w", err)
		}
		// Optional: add default privileges for created objects if needed

	case "EXECUTE":
		_, err := c.db.Exec(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO "` + username + `"`)
		if err != nil {
			return fmt.Errorf("error granting EXECUTE: %w", err)
		}
		_, err = c.db.Exec(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO "` + username + `"`)
		return err

	default:
		_, err := c.db.Exec(`GRANT ` + grantName + ` ON ALL TABLES IN SCHEMA public TO "` + username + `"`)
		if err != nil {
			return fmt.Errorf("error granting table privileges: %w", err)
		}
		_, err = c.db.Exec(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ` + grantName + ` ON TABLES TO "` + username + `"`)
		return err
	}

	return nil
}

func (c *PostgresController) Revoke(grantName, dbName, username string) error {
	if err := validateDBName(dbName); err != nil {
		return fmt.Errorf("error validating database name: %w", err)
	}
	if err := validateUsername(username); err != nil {
		return fmt.Errorf("error validating username: %w", err)
	}

	grantName = strings.ToUpper(grantName)
	if err := validateGrant(grantName); err != nil {
		return fmt.Errorf("error validating grant: %w", err)
	}

	if grantName == "CONNECT" {
		_, err := c.db.Exec(`REVOKE CONNECT ON DATABASE "` + dbName + `" FROM "` + username + `"`)
		if err != nil {
			return fmt.Errorf("error revoking CONNECT privilege: %w", err)
		}
		return nil
	}

	// Revoke from all tables
	if _, err := c.db.Exec(`REVOKE ` + grantName + ` ON ALL TABLES IN SCHEMA public FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking table privileges: %w", err)
	}

	// Revoke default privileges
	if _, err := c.db.Exec(`
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
		REVOKE ` + grantName + ` ON TABLES FROM "` + username + `"`); err != nil {
		return fmt.Errorf("error revoking default table privileges: %w", err)
	}

	// Revoke schema-level privilege if applicable
	if grantName == "USAGE" || grantName == "CREATE" {
		if _, err := c.db.Exec(`REVOKE ` + grantName + ` ON SCHEMA public FROM "` + username + `"`); err != nil {
			return fmt.Errorf("error revoking schema privilege: %w", err)
		}
	}

	return nil
}

// // GrantExists returns true if the given privilege exists for the given database and user
// func (c *PostgresController) GrantExists(grantName, dbName, username string) (bool, error) {
// 	err := validateDBName(dbName)
// 	if err != nil {
// 		return false, fmt.Errorf("error validating database name: %w", err)
// 	}

// 	err = validateUsername(username)
// 	if err != nil {
// 		return false, fmt.Errorf("error validating username: %w", err)
// 	}

// 	grantName = strings.ToUpper(grantName)
// 	err = validateGrant(grantName)
// 	if err != nil {
// 		return false, fmt.Errorf("error validating grant: %w", err)
// 	}

// 	if grantName == "CONNECT" {
// 		var hasPrivilege bool
// 		err = c.db.QueryRow(`
// 			SELECT has_database_privilege($1, $2, 'CONNECT')
// 		`, username, dbName).Scan(&hasPrivilege)
// 		if err != nil {
// 			return false, fmt.Errorf("error checking CONNECT privilege: %w", err)
// 		}
// 		return hasPrivilege, nil
// 	}

// 	// Check privileges on tables in the public schema
// 	var count int
// 	err = c.db.QueryRow(`
// 		SELECT COUNT(*)
// 		FROM information_schema.role_table_grants
// 		WHERE grantee = $1
// 		AND table_catalog = $2
// 		AND privilege_type = $3
// 	`, username, dbName, grantName).Scan(&count)
// 	if err != nil {
// 		return false, fmt.Errorf("error checking if grant exists: %w", err)
// 	}

// 	return count > 0, nil
// }

// validateGrant checks if the given privilege is valid
func validateGrant(grantName string) error {
	if grantName == "" {
		return ErrInvalidGrant
	}

	if _, ok := grants[grantName]; !ok {
		return ErrInvalidGrant
	}

	return nil
}
