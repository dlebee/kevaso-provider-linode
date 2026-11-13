package e2e

import "testing"

func TestSweepRule(t *testing.T) {
	const run = "kvt-wk-3e656e"
	for _, c := range []struct {
		what        string
		label       string
		tags        []string
		run, family bool // owned by the run, by a sweep of kvt-wk-
	}{
		{"own Linode", "kvt-wk-3e656e-cp", []string{run, "kvt-wk"}, true, true},
		{"own volume (the plugin's tags)", "kvt-wk-3e656e-data-dev-0", []string{"kevaso", run}, true, true},
		{"own, untagged", "kvt-wk-3e656e-w1", nil, true, true},
		{"own label, tagged otherwise", "kvt-wk-3e656e-w1", []string{"kevaso"}, false, false},
		{"another run of the family", "kvt-wk-a1b2c3-cp", []string{"kvt-wk-a1b2c3", "kvt-wk"}, false, true},
		{"another run, same id start", "kvt-wk-3e656e7-cp", []string{"kvt-wk-3e656e7"}, false, true},
		{"another family", "kvt-jfs-9f8e7d-cp", []string{"kvt-jfs-9f8e7d", "kvt-jfs"}, false, false},
		{"another family, our tag on it", "kvt-jfs-9f8e7d-cp", []string{run}, false, false},
		{"untagged, similar label", "kvt-wkx-3e656e-cp", nil, false, false},
		{"untagged, family label alone", "kvt-wk-", nil, false, false},
		{"the user's machine", "1002-oracle-rig-1", nil, false, false},
		{"the user's machine, tagged", "1002-oracle-rig-2", []string{"oracle"}, false, false},
		{"the user's machine, our tag on it", "1002-oracle-rig-3", []string{run, "kvt-wk"}, false, false},
	} {
		if got := runOwns(run, c.label, c.tags); got != c.run {
			t.Errorf("%s: runOwns(%s, %v) = %v", c.what, c.label, c.tags, got)
		}
		if got := familyOwns("kvt-wk-", c.label, c.tags); got != c.family {
			t.Errorf("%s: familyOwns(kvt-wk-, %s, %v) = %v", c.what, c.label, c.tags, got)
		}
	}
	// Tags and prefixes that aren't a test run's own own nothing, whatever the label.
	for _, tag := range []string{"", "kvt-", "kvt-wk", "kvt-wk-", "1002", "1002-oracle-rig", "kevaso", "kevaso-e2e-"} {
		for _, label := range []string{"1002-oracle-rig-1", tag + "-cp", tag + "cp"} {
			if runOwns(tag, label, nil) || runOwns(tag, label, []string{tag}) {
				t.Errorf("run tag %q owns %q", tag, label)
			}
		}
	}
	for _, p := range []string{"", "-", "1", "1002-", "kvt-", "kvt--", "kvt-wk", "kevaso-"} {
		for _, label := range []string{"1002-oracle-rig-1", p + "x", p + "x-cp"} {
			if familyOwns(p, label, nil) {
				t.Errorf("family %q owns %q", p, label)
			}
		}
	}
	if !runOwns("kevaso-e2e-abc123", "kevaso-e2e-abc123-cp", []string{"kevaso-e2e-abc123"}) {
		t.Error("kevaso-e2e- runs own nothing")
	}
}
