package migrations

import (
	"context"
	"io/fs"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestMigrate_AppliesUnappliedMigrationAndRecordsVersion(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectExec("CREATE TABLE IF NOT EXISTS outbox_schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock_.ExpectQuery("SELECT 1 FROM outbox_schema_migrations WHERE version = \\?").
		WithArgs("0001_init.sql").
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock_.ExpectExec("CREATE TABLE IF NOT EXISTS outbox_events").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock_.ExpectExec("CREATE TABLE IF NOT EXISTS outbox_deadletters").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock_.ExpectExec("INSERT INTO outbox_schema_migrations \\(version\\) VALUES \\(\\?\\)").
		WithArgs("0001_init.sql").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = Migrate(context.Background(), db)
	assert.NoError(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestMigrate_SkipsAlreadyAppliedMigration(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectExec("CREATE TABLE IF NOT EXISTS outbox_schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock_.ExpectQuery("SELECT 1 FROM outbox_schema_migrations WHERE version = \\?").
		WithArgs("0001_init.sql").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	err = Migrate(context.Background(), db)
	assert.NoError(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestFS_ContainsInitMigration(t *testing.T) {
	contents, err := fs.ReadFile(FS(), "0001_init.sql")
	assert.NoError(t, err)
	assert.Contains(t, string(contents), "CREATE TABLE IF NOT EXISTS outbox_events")
	assert.Contains(t, string(contents), "CREATE TABLE IF NOT EXISTS outbox_deadletters")
}
