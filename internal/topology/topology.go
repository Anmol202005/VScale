package topology

type Tablet struct {
	Cell          string `json:"cell"`
	Keyspace      string `json:"keyspace"`
	Shard         string `json:"shard"`
	Type          string `json:"type"`
	Addr          string `json:"addr"`
	KeyRangeStart int64  `json:"key_range_start"`
	KeyRangeEnd   int64  `json:"key_range_end"`
}

type Topology struct {
	Tablets []Tablet
}

func NewStaticTopology(tablets []Tablet) *Topology {
	return &Topology{Tablets: tablets}
}

func (t *Topology) GetTablets() []Tablet {
	return t.Tablets
}
