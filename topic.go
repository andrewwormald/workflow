package workflow

import "strings"

func Topic(parts ...string) string {
	var ps []string
	for _, p := range parts {
		p = strings.ReplaceAll(p, " ", "-")
		ps = append(ps, p)
	}
	return strings.Join(ps, "-")
}
