package db

import (
	"path"

	"github.com/lindend/distdb/internal/lsmtree"
)

type Collection struct {
	lsmt *lsmtree.LsmTree
}

func NewCollection(rootDir string, name string) (*Collection, error) {
	tree, err := lsmtree.NewLsmTree(path.Join(rootDir, name))
	if err != nil {
		return nil, err
	}

	return &Collection{
		lsmt: tree,
	}, nil
}
