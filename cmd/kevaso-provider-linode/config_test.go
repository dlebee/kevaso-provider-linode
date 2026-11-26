package main

import (
	"strings"
	"testing"
)

// The token's credential is named by a cluster secret and a key, never a guessed key.
func TestCredentialNameNamesItsKey(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		want string
		bad  bool
	}{
		{Config{}, "linode_token", false},
		{Config{TokenSecret: &SecretRef{Name: "cloud", Key: "api-token"}}, "cloud_api-token", false},
		{Config{TokenSecret: &SecretRef{Name: "cloud"}}, "", true},
		{Config{TokenSecret: &SecretRef{Key: "token"}}, "", true},
		{Config{TokenSecret: &SecretRef{}}, "", true},
	} {
		got, err := tc.cfg.credentialName()
		if tc.bad {
			if err == nil || !strings.Contains(err.Error(), `"key"`) {
				t.Errorf("%+v: want a refusal that shows the form, got %q, %v", tc.cfg.TokenSecret, got, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%+v: %q, %v; want %q", tc.cfg.TokenSecret, got, err, tc.want)
		}
	}
}
