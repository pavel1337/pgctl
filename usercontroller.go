// postgresctl/usercontroller.go
package postgresctl

import (
	"database/sql"
	"fmt"
	"strings"
)

type UserController interface {
	CreateUser(username, password string) error
	UpdateUserPassword(username, password string) error
	DeleteUser(username string) error
	ListUsers() ([]string, error)
	UserExists(username string) (bool, error)
	CreateUserWithMaxConn(username, password string, maxConn int) error
	UpdateUserMaxConn(username string, maxConn int) error
	GetUserMaxConn(username string) (int, error)
}

var _ UserController = &PostgresController{}

var baseUsers = []string{"postgres"}

var (
	ErrUserExists       = fmt.Errorf("user exists")
	ErrUserDoesNotExist = fmt.Errorf("user does not exist")
)

func (c *PostgresController) CreateUser(username, password string) error {
	err := validateUsername(username)
	if err != nil {
		return err
	}

	err = validatePassword(password)
	if err != nil {
		return err
	}

	_, err = c.db.Exec("CREATE ROLE \"" + username + "\" WITH LOGIN PASSWORD '" + password + "'")
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return ErrUserExists
		}
		return fmt.Errorf("error creating user: %w", err)
	}
	return nil
}

func (c *PostgresController) CreateUserWithMaxConn(username, password string, maxConn int) error {
	err := validateUsername(username)
	if err != nil {
		return err
	}

	err = validatePassword(password)
	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf(
		"CREATE ROLE \"%s\" WITH LOGIN PASSWORD '%s' CONNECTION LIMIT %d",
		username, password, maxConn))
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return ErrUserExists
		}
		return fmt.Errorf("error creating user: %w", err)
	}
	return nil
}

func (c *PostgresController) GetUserMaxConn(username string) (int, error) {
	err := validateUsername(username)
	if err != nil {
		return 0, err
	}

	var maxConn sql.NullInt32
	err = c.db.QueryRow(`
		SELECT rolconnlimit
		FROM pg_roles
		WHERE rolname = $1
	`, username).Scan(&maxConn)

	if err != nil {
		return 0, fmt.Errorf("error getting user max connections: %w", err)
	}

	if !maxConn.Valid {
		return -1, nil // -1 means unlimited in PostgreSQL
	}

	return int(maxConn.Int32), nil
}

func (c *PostgresController) UpdateUserMaxConn(username string, maxConn int) error {
	err := validateUsername(username)
	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf(
		"ALTER ROLE \"%s\" WITH CONNECTION LIMIT %d",
		username, maxConn))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return ErrUserDoesNotExist
		}
		return fmt.Errorf("error updating user max connections: %w", err)
	}
	return nil
}

func (c *PostgresController) UpdateUserPassword(username, password string) error {
	err := validateUsername(username)
	if err != nil {
		return err
	}

	err = validatePassword(password)
	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf(
		"ALTER ROLE \"%s\" WITH PASSWORD '%s'",
		username, password))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return ErrUserDoesNotExist
		}
		return fmt.Errorf("error updating user password: %w", err)
	}
	return nil
}

func (c *PostgresController) DeleteUser(username string) error {
	err := validateUsername(username)
	if err != nil {
		return err
	}

	// First, terminate all connections for the user
	_, err = c.db.Exec(fmt.Sprintf(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE usename = '%s'
	`, username))
	if err != nil {
		return fmt.Errorf("error terminating user connections: %w", err)
	}

	_, err = c.db.Exec(fmt.Sprintf("DROP ROLE \"%s\"", username))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return ErrUserDoesNotExist // define this as needed
		}
		return fmt.Errorf("error deleting user: %w", err)
	}
	return nil
}

func (c *PostgresController) ListUsers() ([]string, error) {
	rows, err := c.db.Query(`
		SELECT rolname
		FROM pg_roles
		WHERE rolcanlogin = true
		AND rolname NOT LIKE 'pg_%'
	`)
	if err != nil {
		return nil, fmt.Errorf("error listing users: %w", err)
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var user string
		err = rows.Scan(&user)
		if err != nil {
			return nil, fmt.Errorf("error scanning user: %w", err)
		}
		users = append(users, user)
	}

	return filterUsers(users), nil
}

func (c *PostgresController) UserExists(username string) (bool, error) {
	err := validateUsername(username)
	if err != nil {
		return false, err
	}

	var exists bool
	err = c.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM pg_roles
			WHERE rolname = $1
		)`, username).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking if user exists: %w", err)
	}
	return exists, nil
}

func filterUsers(users []string) []string {
	var filtered []string
	for _, user := range users {
		if !contains(baseUsers, user) {
			filtered = append(filtered, user)
		}
	}
	return filtered
}

func validateUsername(username string) error {
	if username == "" {
		return fmt.Errorf("username cannot be empty")
	}

	if contains(baseUsers, username) {
		return fmt.Errorf("username %s is reserved", username)
	}
	return nil
}

func validatePassword(password string) error {
	if password == "" {
		return fmt.Errorf("password cannot be empty")
	}
	return nil
}
