package topology

import (
	"fmt"
	"strings"
)

func LoadFromEnv(tabletAddrsCSV string) (*Topology, error) {
	if strings.TrimSpace(tabletAddrsCSV) == "" {
		return nil, fmt.Errorf("topology: no tablet addresses provided")
	}

	parts := strings.Split(tabletAddrsCSV, ",")
	tablets := make([]Tablet, 0, len(parts))
	for _, p := range parts {
		addr := strings.TrimSpace(p)
		if addr == "" {
			continue
		}
		tablets = append(tablets, Tablet{Addr: addr})
	}

	if len(tablets) == 0 {
		return nil, fmt.Errorf("topology: no valid tablet addresses found")
	}

	return NewStaticTopology(tablets), nil
}