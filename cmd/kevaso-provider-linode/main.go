// Command kevaso-provider-linode is Kevaso's Linode provider plugin. kvoadm plugin install runs it
// as a hardened systemd unit:
//
//	kevaso-provider-linode serve --socket /run/kevaso-plugins/linode/plugin.sock [--config FILE]
//	kevaso-provider-linode node [--config FILE]
//
// serve is the controller side, on control planes: it reads the Linode API token from the systemd
// credential "token" (never from argv or the environment) and answers kevasod over the socket.
// node is the node side, on Linode workers: it announces shared IPs over BGP and needs no token.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"kevaso.com/core/sdk/kplugin"
	"kevaso.com/provider-linode/linode"
	"kevaso.com/provider-linode/node"
)

// Config is the file given with --config.
type Config struct {
	linode.Config
	// Region is the worker's Linode region (node side), for the BGP route servers; DCID overrides it.
	Region string `json:"region,omitempty"`
	DCID   int    `json:"dcid,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kevaso-provider-linode serve --socket PATH [--config FILE] | node [--config FILE] | version")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1], os.Args[2:]); err != nil {
		log.Printf("kevaso-provider-linode: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	socket := fs.String("socket", kplugin.Dir+"/linode/plugin.sock", "where kevasod finds this plugin")
	cfgFile := fs.String("config", "", "settings (JSON): tags, and for the node side region or dcid")
	_ = fs.Parse(args)
	var cfg Config
	if *cfgFile != "" {
		b, err := os.ReadFile(*cfgFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			return fmt.Errorf("%s: %w", *cfgFile, err)
		}
	}
	switch cmd {
	case "version":
		fmt.Println(linode.Version)
		return nil
	case "serve":
		token, err := credential("token")
		if err != nil {
			return err
		}
		p := linode.New(token, cfg.Config)
		log.Printf("linode provider %s serving on %s", linode.Version, *socket)
		return kplugin.Serve(ctx, *socket, p)
	case "node":
		dcid := cfg.DCID
		if dcid == 0 {
			dcid = node.DCIDs[cfg.Region]
		}
		if dcid == 0 {
			return fmt.Errorf("node side: no BGP data-center ID for region %q (set region or dcid in --config)", cfg.Region)
		}
		log.Printf("linode node side %s: announcing shared IPs on lo (dcid %d)", linode.Version, dcid)
		return (&node.Announcer{DCID: dcid, Log: log.Printf}).Run(ctx)
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// credential reads a systemd credential (LoadCredential=NAME:…), or, outside systemd, the file
// named by KEVASO_LINODE_TOKEN_FILE.
func credential(name string) (string, error) {
	var path string
	if d := os.Getenv("CREDENTIALS_DIRECTORY"); d != "" {
		path = filepath.Join(d, name)
	} else if f := os.Getenv("KEVASO_LINODE_TOKEN_FILE"); f != "" {
		path = f
	} else {
		return "", errors.New("no Linode token: run under systemd with LoadCredential=token:FILE (kvoadm plugin install --credential token=FILE)")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	t := strings.TrimSpace(string(b))
	if t == "" {
		return "", errors.New("the Linode token is empty")
	}
	return t, nil
}
