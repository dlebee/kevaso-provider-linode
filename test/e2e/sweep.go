// Package e2e holds the tests that run on real Linodes (build tag e2e); this file, with no tag,
// holds the rule deciding what their sweeps may delete, so it is unit tested on every go test.
package e2e

import "strings"

// validPrefix says whether prefix names a family of test runs: kevaso-e2e-, or kvt-NAME- (kvt-ha-,
// kvt-wk-). A sweep never looks at anything else in the account, which holds the user's own
// machines too.
func validPrefix(prefix string) bool {
	if prefix == "kevaso-e2e-" {
		return true
	}
	name := strings.TrimPrefix(prefix, "kvt-")
	return name != prefix && strings.HasSuffix(name, "-") && strings.Trim(name, "-") != ""
}

// runOwns says whether a resource (its label and tags) belongs to the run tagged tag (a family
// prefix plus a run id, e.g. kvt-wk-3e656e): its label is the tag or starts with tag-, and, when
// it has tags at all, one of them is the tag. Never by tag alone.
func runOwns(tag, label string, tags []string) bool {
	i := strings.LastIndex(strings.TrimSuffix(tag, "-"), "-")
	if i < 0 || !validPrefix(tag[:i+1]) || len(tag) == i+1 {
		return false
	}
	if label != tag && !strings.HasPrefix(label, tag+"-") {
		return false
	}
	return len(tags) == 0 || has(tags, tag)
}

// familyOwns says whether a resource belongs to any run of the family prefix (kvt-ha-): only
// for a sweep a person asked for (KEVASO_E2E_SWEEP). Its label starts with prefix and, when it
// has tags, one is the family (kvt-ha) or a run's tag (kvt-ha-…).
func familyOwns(prefix, label string, tags []string) bool {
	if !validPrefix(prefix) || !strings.HasPrefix(label, prefix) || len(label) == len(prefix) {
		return false
	}
	if len(tags) == 0 {
		return true
	}
	for _, tg := range tags {
		if tg == strings.TrimSuffix(prefix, "-") || strings.HasPrefix(tg, prefix) && strings.HasPrefix(label, tg) {
			return true
		}
	}
	return false
}

func has(tags []string, tag string) bool {
	for _, tg := range tags {
		if tg == tag {
			return true
		}
	}
	return false
}
