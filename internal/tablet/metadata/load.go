package metadata

import "flag"

func LoadFromFlags() (*TabletMetadata, error) {
	cell := flag.String("cell", "zone1", "cell this tablet belongs to")
	uid := flag.Uint("uid", 1, "unique id within the cell")
	keyspace := flag.String("keyspace", "", "logical keyspace name (required)")
	shard := flag.String("shard", "0", "shard name, e.g. '-80', '80-', or '0' if unsharded")
	tabletType := flag.String("tablet_type", "PRIMARY", "PRIMARY | REPLICA | RDONLY")
	hostname := flag.String("hostname", "localhost", "hostname this tablet is reachable at")
	grpcPort := flag.Int("grpc_port", 50051, "gRPC port this tablet listens on")
	pgPort := flag.Int("pg_port", 5432, "postgres port this tablet proxies to")
	maxConns := flag.Int("pg_max_conns", 20, "max postgres connections this tablet may hold")
	keyRangeStart := flag.String("key_range_start", "", "optional: sharded key range start")
	keyRangeEnd := flag.String("key_range_end", "", "optional: sharded key range end")

	flag.Parse()

	m := &TabletMetadata{
		Cell:          *cell,
		UID:           uint32(*uid),
		Keyspace:      *keyspace,
		Shard:         *shard,
		Type:          ParseTabletType(*tabletType),
		Hostname:      *hostname,
		GRPCPort:      *grpcPort,
		PGPort:        *pgPort,
		MaxConns:      int32(*maxConns),
		KeyRangeStart: *keyRangeStart,
		KeyRangeEnd:   *keyRangeEnd,
	}

	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}