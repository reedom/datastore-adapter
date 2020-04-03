package datastoreadapter

import (
	"context"
	"io/ioutil"

	"cloud.google.com/go/datastore"
	"github.com/casbin/casbin/v2/model"
)

type CasbinModelConf struct {
	Text string `datastore:"text,noindex"`
}

// SaveModel loads a casbin model definition from the specified file and store it to a datastore entity.
func SaveModel(db *datastore.Client, path string) error {
	return SaveModelWithConfig(db, path, Config{Kind: casbinKind, Namespace: ""})
}

// SaveModel loads a casbin model definition from the specified file and store it to a datastore entity.
func SaveModelWithConfig(db *datastore.Client, path string, config Config) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	text := string(b)

	// Validate the specified config.
	if _, err = model.NewModelFromString(text); err != nil {
		return err
	}

	kind := casbinKind
	if config.Kind != "" {
		kind = config.Kind
	}
	namespace := config.Namespace

	ctx := context.Background()
	_, err = db.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		key := datastore.NameKey(kind, "conf", nil)
		key.Namespace = namespace

		m := CasbinModelConf{text}
		_, err := tx.Put(key, &m)
		return err
	})
	return err
}

// LoadModel loads a casbin model definition from a datastore entity.
func LoadModel(db *datastore.Client) (model.Model, error) {
	return LoadModelWithConfig(db, Config{Kind: casbinKind, Namespace: ""})
}

// LoadModel loads a casbin model definition from a datastore entity.
func LoadModelWithConfig(db *datastore.Client, config Config) (model.Model, error) {
	kind := casbinKind
	if config.Kind != "" {
		kind = config.Kind
	}
	namespace := config.Namespace

	key := datastore.NameKey(kind, "conf", nil)
	key.Namespace = namespace

	ctx := context.Background()
	var conf CasbinModelConf
	if err := db.Get(ctx, key, &conf); err != nil {
		return nil, err
	}

	return model.NewModelFromString(conf.Text)
}
