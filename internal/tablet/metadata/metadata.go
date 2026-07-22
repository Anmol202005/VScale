package metadata

import "fmt"

type TabletType int

const (
	TabletTypeUnknown TabletType = iota
	TabletTypePrimary
	TabletTypeReplica
	TabletTypeRdonly
)

func ParseTabletType(s string) TabletType {
	switch s {
	case "PRIMARY":
		return TabletTypePrimary
	case "REPLICA":
		return TabletTypeReplica
	case "RDONLY":
		return TabletTypeRdonly
	default:
		return TabletTypeUnknown
	}
}

func (t TabletType) String() string {
	switch t {
	case TabletTypePrimary:
		return "PRIMARY"
	case TabletTypeReplica:
		return "REPLICA"
	case TabletTypeRdonly:
		return "RDONLY"
	default:
		return "UNKNOWN"
	}
}

type TabletMetadata struct {
	Cell     string
	UID      uint32
	Keyspace string
	Shard    string
	Type     TabletType

	MaxConns int32 

	Hostname string
	GRPCPort int
	PGPort   int

	KeyRangeStart string
	KeyRangeEnd   string
}

func (m *TabletMetadata) Alias() string {
	return fmt.Sprintf("%s-%d", m.Cell, m.UID)
}

func (m *TabletMetadata) Validate() error {
	if m.Cell == "" {
		return fmt.Errorf("metadata: cell is required")
	}
	if m.Keyspace == "" {
		return fmt.Errorf("metadata: keyspace is required")
	}
	if m.Shard == "" {
		return fmt.Errorf("metadata: shard is required")
	}
	if m.Type == TabletTypeUnknown {
		return fmt.Errorf("metadata: tablet type must be PRIMARY, REPLICA, or RDONLY")
	}
	if m.GRPCPort == 0 {
		return fmt.Errorf("metadata: grpc_port is required")
	}
	if m.MaxConns <= 0 {
		return fmt.Errorf("metadata: max_conns must be > 0")
	}
	return nil
}