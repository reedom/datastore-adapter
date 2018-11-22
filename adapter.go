package datastoreadapter

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/casbin/casbin/model"
	"github.com/casbin/casbin/persist"
	"github.com/pkg/errors"
	"runtime"
)

const casbinKind = "casbin"

// CasbinRule represents a rule in Casbin.
type CasbinRule struct {
	PType string `datastore:"p_type"`
	V0    string `datastore:"v0"`
	V1    string `datastore:"v1"`
	V2    string `datastore:"v2"`
	V3    string `datastore:"v3"`
	V4    string `datastore:"v4"`
	V5    string `datastore:"v5"`
}

// adapter represents the GCP datastore adapter for policy storage.
type adapter struct {
	db *datastore.Client
}

// finalizer is the destructor for adapter.
func finalizer(a *adapter) {
	a.close()
}

func (a *adapter) close() {
	a.db.Close()
}

// NewAdapter is the constructor for Adapter. A valid datastore client must be provided.
func NewAdapter(db *datastore.Client) persist.Adapter {
	a := &adapter{db}

	// Call the destructor when the object is released.
	runtime.SetFinalizer(a, finalizer)

	return a
}

func (a *adapter) LoadPolicy(model model.Model) error {

	var rules []*CasbinRule

	ctx := context.Background()
	query := datastore.NewQuery(casbinKind)
	_, err := a.db.GetAll(ctx, query, &rules)

	if err != nil {
		return err
	}

	for _, l := range rules {
		loadPolicyLine(*l, model)
	}

	return nil
}

func (a *adapter) SavePolicy(model model.Model) error {
	ctx := context.Background()

	// Drop all casbin entities
	var rules []*CasbinRule
	keys, err := a.db.GetAll(ctx, datastore.NewQuery(casbinKind), &rules)
	if err != nil {
		return err
	}
	for _, k := range keys {
		a.db.Delete(ctx, k)
	}

	var lines []interface{}

	for ptype, ast := range model["p"] {
		for _, rule := range ast.Policy {
			line := savePolicyLine(ptype, rule)
			lines = append(lines, &line)
		}
	}

	for ptype, ast := range model["g"] {
		for _, rule := range ast.Policy {
			line := savePolicyLine(ptype, rule)
			lines = append(lines, &line)
		}
	}

	a.db.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		for _, line := range lines {

			key := datastore.IncompleteKey(casbinKind, nil)
			_, err := tx.Put(key, line)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}

func (a *adapter) AddPolicy(sec string, ptype string, rule []string) error {
	return errors.New("not implemented")
}

func (a *adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return errors.New("not implemented")
}

func (a *adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return errors.New("not implemented")
}

func savePolicyLine(ptype string, rule []string) CasbinRule {
	line := CasbinRule{
		PType: ptype,
	}

	if len(rule) > 0 {
		line.V0 = rule[0]
	}
	if len(rule) > 1 {
		line.V1 = rule[1]
	}
	if len(rule) > 2 {
		line.V2 = rule[2]
	}
	if len(rule) > 3 {
		line.V3 = rule[3]
	}
	if len(rule) > 4 {
		line.V4 = rule[4]
	}
	if len(rule) > 5 {
		line.V5 = rule[5]
	}

	return line
}

func loadPolicyLine(line CasbinRule, model model.Model) {
	key := line.PType
	sec := key[:1]

	tokens := []string{}
	if line.V0 != "" {
		tokens = append(tokens, line.V0)
	} else {
		goto LineEnd
	}

	if line.V1 != "" {
		tokens = append(tokens, line.V1)
	} else {
		goto LineEnd
	}

	if line.V2 != "" {
		tokens = append(tokens, line.V2)
	} else {
		goto LineEnd
	}

	if line.V3 != "" {
		tokens = append(tokens, line.V3)
	} else {
		goto LineEnd
	}

	if line.V4 != "" {
		tokens = append(tokens, line.V4)
	} else {
		goto LineEnd
	}

	if line.V5 != "" {
		tokens = append(tokens, line.V5)
	} else {
		goto LineEnd
	}

LineEnd:
	model[sec][key].Policy = append(model[sec][key].Policy, tokens)
}
