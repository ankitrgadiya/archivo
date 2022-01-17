package datastore

import (
	"context"
	"io"

	"argc.in/archivo/internal/model"
)

type WebStore interface {
	io.Closer
	Save(ctx context.Context, p *model.Page) error
	Get(ctx context.Context, p *model.Page) error
	Search(ctx context.Context, query string) ([]model.Page, error)
}
