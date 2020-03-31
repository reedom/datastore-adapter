package datastoreadapter

import (
	"context"
	"fmt"
	"runtime"

	"cloud.google.com/go/datastore"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
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

type AdapterConfig struct {
	// Datastore kind name.
	// Optional. (Default: "casbin")
	Kind string
	// Datastore namespace.
	// Optional. (Default: "")
	Namespace string
}

// adapter represents the GCP datastore adapter for policy storage.
type adapter struct {
	db *datastore.Client
	kind string
	namespace string
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
	return NewAdapterWithConfig(db, AdapterConfig{casbinKind, ""})
}

// NewAdapter is the constructor for Adapter. A valid datastore client must be provided.
func NewAdapterWithConfig(db *datastore.Client, config AdapterConfig) persist.Adapter {
	kind := casbinKind
	if config.Kind != "" {
		kind = config.Kind
	}
	namespace := config.Namespace

	a := &adapter{db, kind, namespace}

	// Call the destructor when the object is released.
	runtime.SetFinalizer(a, finalizer)

	return a
}

func (a *adapter) LoadPolicy(model model.Model) error {

	var rules []*CasbinRule

	ctx := context.Background()
	query := datastore.NewQuery(a.kind).Namespace(a.namespace)
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
	keys, err := a.db.GetAll(ctx, datastore.NewQuery(a.kind).Namespace(a.namespace), &rules)
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

			key := datastore.IncompleteKey(a.kind, nil)
			key.Namespace = a.namespace
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

	ctx := context.Background()
	line := savePolicyLine(ptype, rule)

	key := datastore.IncompleteKey(a.kind, nil)
	key.Namespace = a.namespace
	_, err := a.db.Put(ctx, key, &line)
	return err
}

func (a *adapter) RemovePolicy(sec string, ptype string, rule []string) error {

	var rules []*CasbinRule

	line := savePolicyLine(ptype, rule)

	ctx := context.Background()
	query := datastore.NewQuery(a.kind).Namespace(a.namespace).
		Filter("p_type =", line.PType).
		Filter("v0 =", line.V0).
		Filter("v1 =", line.V1).
		Filter("v2 =", line.V2).
		Filter("v3 =", line.V3).
		Filter("v4 =", line.V4)

	keys, err := a.db.GetAll(ctx, query, &rules)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil
		default:
			return err
		}
	}
	return a.db.DeleteMulti(ctx, keys)
}

func (a *adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {

	ctx := context.Background()

	var rules []*CasbinRule

	selector := make(map[string]interface{})
	selector["p_type"] = ptype

	if fieldIndex <= 0 && 0 < fieldIndex+len(fieldValues) {
		if fieldValues[0-fieldIndex] != "" {
			selector["v0"] = fieldValues[0-fieldIndex]
		}
	}
	if fieldIndex <= 1 && 1 < fieldIndex+len(fieldValues) {
		if fieldValues[1-fieldIndex] != "" {
			selector["v1"] = fieldValues[1-fieldIndex]
		}
	}
	if fieldIndex <= 2 && 2 < fieldIndex+len(fieldValues) {
		if fieldValues[2-fieldIndex] != "" {
			selector["v2"] = fieldValues[2-fieldIndex]
		}
	}
	if fieldIndex <= 3 && 3 < fieldIndex+len(fieldValues) {
		if fieldValues[3-fieldIndex] != "" {
			selector["v3"] = fieldValues[3-fieldIndex]
		}
	}
	if fieldIndex <= 4 && 4 < fieldIndex+len(fieldValues) {
		if fieldValues[4-fieldIndex] != "" {
			selector["v4"] = fieldValues[4-fieldIndex]
		}
	}
	if fieldIndex <= 5 && 5 < fieldIndex+len(fieldValues) {
		if fieldValues[5-fieldIndex] != "" {
			selector["v5"] = fieldValues[5-fieldIndex]
		}
	}

	query := datastore.NewQuery(a.kind).Namespace(a.namespace)
	for k, v := range selector {
		query = query.Filter(fmt.Sprintf("%s =", k), v)
	}

	keys, err := a.db.GetAll(ctx, query, &rules)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil
		default:
			return err
		}
	}
	return a.db.DeleteMulti(ctx, keys)
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
