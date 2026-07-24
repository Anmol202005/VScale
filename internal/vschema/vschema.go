package vschema

import (
	"encoding/json"
	"fmt"
	"os"
)

type VSchema struct {
	Keyspace      string            `json:"keyspace"`
	ShardedTables map[string]string `json:"sharded_tables"`
}

func Load(path string) (*VSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("vschema: failed to read %s: %w", path, err)
	}
	var vs VSchema
	if err := json.Unmarshal(data, &vs); err != nil {
		return nil, fmt.Errorf("vschema: failed to parse %s: %w", path, err)
	}
	return &vs, nil
}

func (v *VSchema) ShardKeyColumn(table string) (string, bool) {
	col, ok := v.ShardedTables[table]
	return col, ok
}