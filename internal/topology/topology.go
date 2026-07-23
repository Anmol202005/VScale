package topology

type Tablet struct {
	Cell     string
	Keyspace string
	Shard    string
	Type     string
	Addr     string
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
