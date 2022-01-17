package datastore

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"argc.in/archivo/internal/model"
)

func NewSQLiteStore(path string) (WebStore, error) {
	dsn := fmt.Sprintf("file:%s?_journal=WAL", path)
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	i := &sqliteImpl{conn: conn}

	if err := i.initialize(); err != nil {
		conn.Close()
		return nil, err
	}

	return i, nil
}

type sqliteImpl struct {
	conn *sql.DB
}

func (i *sqliteImpl) Close() error {
	return i.conn.Close()
}

func (i *sqliteImpl) initialize() error {
	query := `CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
				url TEXT,
				title TEXT
			  );`

	if _, err := i.conn.ExecContext(context.Background(), query); err != nil {
		return err
	}

	query = `CREATE VIRTUAL TABLE IF NOT EXISTS fts
             USING fts5(pageid, content);`

	if _, err := i.conn.ExecContext(context.Background(), query); err != nil {
		return err
	}

	return nil
}

func (i *sqliteImpl) Save(ctx context.Context, p *model.Page) (err error) {
	// Is it required to split them up?
	query := `BEGIN TRANSACTION;
              INSERT INTO pages (url, title) VALUES(?, ?);
              INSERT INTO fts VALUES(last_insert_rowid(), ?);
              COMMIT TRANSACTION;`

	if _, err := i.conn.ExecContext(ctx, query, p.URL, p.Title, p.HTMLContent); err != nil {
		return err
	}

	return nil
}

func (i *sqliteImpl) Get(ctx context.Context, p *model.Page) error {
	query := `SELECT
                pages.url,
                pages.title,
                fts.content,
              FROM pages
              JOIN fts ON pages.id = fts.pageid
              WHERE pages.id = ?`

	if err := i.conn.QueryRowContext(ctx, query, p.ID).Scan(&p.URL, &p.Title, &p.HTMLContent); err != nil {
		return err
	}

	return nil
}

func (i *sqliteImpl) Search(ctx context.Context, search string) ([]model.Page, error) {
	query := `SELECT
                pages.id,
                pages.url,
                pages.title,
                snippet(fts, -1, "[ ", " ]", "...", 10)
              FROM pages
              JOIN fts ON pages.id = fts.pageid
              WHERE fts MATCH ?;`

	rows, err := i.conn.QueryContext(ctx, query, search)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pages []model.Page

	for rows.Next() {
		var p model.Page
		if err := rows.Scan(&p.ID, &p.URL, &p.Title, &p.HTMLContent); err != nil {
			return nil, err
		}

		pages = append(pages, p)
	}

	return pages, nil
}
