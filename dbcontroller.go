// postgresctl/dbcontroller.go
package postgresctl

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

type DBController interface {
	CreateDatabase(dbName string) error
	DeleteDatabase(dbName string) error
	ListDatabases() ([]string, error)
	DatabaseExists(dbName string) (bool, error)
	Size(dbName string) (int, error)
	Tables(dbName string) ([]string, error)
	TransferDatabaseOwnership(dbName, newOwner string) error
	TransferPublicSchemaOwnership(dbName, newOwner string) error
}

var _ DBController = &PostgresController{}

var baseDBs = []string{"postgres", "template0", "template1"}

var (
	ErrDBExists       = fmt.Errorf("database exists")
	ErrDBDoesNotExist = fmt.Errorf("database does not exist")
)

type PostgresController struct {
	db *sql.DB
	pc PostgresConn
}

type PostgresConn struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	SSLMode  string
}

// connStr returns the connection string from the postgressConn struct
func (pc PostgresConn) connStr() string {
	if pc.SSLMode == "" {
		pc.SSLMode = "disable"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		pc.Username, pc.Password, pc.Host, pc.Port, pc.Database, pc.SSLMode)
}

type Option func(*PostgresController)

func WithBadUsernames(usernames []string) Option {
	return func(c *PostgresController) {
		baseUsers = append(baseUsers, usernames...)
	}
}

func NewPostgresController(conn PostgresConn, opts ...Option) (*PostgresController, error) {
	// build connection string
	connStr := conn.connStr()
	// create database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error creating database connection: %s", err)
	}

	c := &PostgresController{db: db, pc: conn}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

func (c *PostgresController) Close() error {
	return c.db.Close()
}

func (c *PostgresController) CreateDatabase(dbName string) error {
	err := validateDBName(dbName)
	if err != nil {
		return err
	}

	_, err = c.db.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", dbName))
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return ErrDBExists
		}
	}
	return err
}

func (c *PostgresController) DeleteDatabase(dbName string) error {
	err := validateDBName(dbName)
	if err != nil {
		return err
	}

	// First, disconnect all users from the database
	_, err = c.db.Exec(fmt.Sprintf(`
		SELECT pg_terminate_backend(pg_stat_activity.pid)
		FROM pg_stat_activity
		WHERE pg_stat_activity.datname = '%s'
		AND pid <> pg_backend_pid()`, dbName))
	if err != nil {
		return fmt.Errorf("error terminating connections: %w", err)
	}

	_, err = c.db.Exec(fmt.Sprintf("DROP DATABASE \"%s\"", dbName))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return ErrDBDoesNotExist
		}
		return fmt.Errorf("error dropping database: %w", err)
	}

	return err
}

func (c *PostgresController) ListDatabases() ([]string, error) {
	rows, err := c.db.Query(`
		SELECT datname FROM pg_database
		WHERE datistemplate = false
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return nil, err
		}
		databases = append(databases, database)
	}

	return filterBaseDatabases(databases), nil
}

func (c *PostgresController) DatabaseExists(dbName string) (bool, error) {
	err := validateDBName(dbName)
	if err != nil {
		return false, err
	}

	var exists bool
	err = c.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM pg_database
			WHERE datname = $1
		)`, dbName).Scan(&exists)
	if err != nil {
		return exists, err
	}

	return exists, nil
}

func (c *PostgresController) Size(dbName string) (int, error) {
	err := validateDBName(dbName)
	if err != nil {
		return 0, err
	}

	var size int
	err = c.db.QueryRow(`
		SELECT pg_database_size($1)
	`, dbName).Scan(&size)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (c *PostgresController) Tables(dbName string) ([]string, error) {
	err := validateDBName(dbName)
	if err != nil {
		return nil, err
	}

	perDbConn := c.pc
	perDbConn.Database = dbName
	newConnStr := perDbConn.connStr()

	db, err := sql.Open("postgres", newConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database %s: %w", dbName, err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func (c *PostgresController) TransferDatabaseOwnership(dbName, newOwner string) error {
	_, err := c.db.Exec(`ALTER DATABASE "` + dbName + `" OWNER TO "` + newOwner + `"`)
	return err
}

func (c *PostgresController) TransferPublicSchemaOwnership(dbName, newOwner string) error {
	perDbConn := c.pc
	perDbConn.Database = dbName
	db, err := sql.Open("postgres", perDbConn.connStr())
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", dbName, err)
	}
	defer db.Close()

	_, err = db.Exec(`ALTER SCHEMA public OWNER TO "` + newOwner + `"`)
	return err
}

func filterBaseDatabases(dbs []string) []string {
	var filtered []string
	for _, db := range dbs {
		if !contains(baseDBs, db) {
			filtered = append(filtered, db)
		}
	}
	return filtered
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func validateDBName(dbName string) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	if contains(baseDBs, dbName) {
		return fmt.Errorf("%v is a disallowed database name", dbName)
	}
	return nil
}
