// Package migrations embeds ordered PostgreSQL schema migrations into the
// migration binary.
package migrations

import (
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Migration is one forward-only schema change.
type Migration struct {
	Version int64
	Name    string
	SQL     string
}

//go:embed *.up.sql
var files embed.FS

// All returns embedded up migrations ordered by version.
func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	result := make([]Migration, 0, len(entries))
	versions := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}

		versionText, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q must start with a numeric version followed by an underscore", entry.Name())
		}
		version, err := strconv.ParseInt(versionText, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version in %q: %w", entry.Name(), err)
		}
		if previous, exists := versions[version]; exists {
			return nil, fmt.Errorf("migrations %q and %q use the same version", previous, entry.Name())
		}

		contents, err := files.ReadFile(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}

		versions[version] = entry.Name()
		result = append(result, Migration{
			Version: version,
			Name:    strings.TrimSuffix(entry.Name(), ".up.sql"),
			SQL:     string(contents),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Version < result[j].Version
	})
	return result, nil
}
