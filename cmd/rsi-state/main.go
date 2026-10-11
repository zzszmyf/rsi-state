package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/zzszmyf/rsi-state/evolution"
	"github.com/zzszmyf/rsi-state/persistence"
)

func main() {
	if len(os.Args) > 1 {
		runCommand(os.Args[1:])
		return
	}
	address := os.Getenv("RSI_STATE_ADDR")
	if address == "" {
		address = ":8080"
	}
	service := evolution.NewService(evolution.NewInMemoryStore(), nil)
	log.Printf("rsi-state listening on %s", address)
	handler := http.Handler(evolution.NewHTTPServer(service))
	if cacheRoot := os.Getenv("RSI_STATE_CACHE_ROOT"); cacheRoot != "" {
		mux := http.NewServeMux()
		mux.Handle("/v1/cache/", http.StripPrefix("/v1/cache", persistence.NewCacheHTTPHandler(persistence.WorkspaceCache{Root: cacheRoot})))
		mux.Handle("/", handler)
		handler = mux
	}
	if err := http.ListenAndServe(address, handler); err != nil {
		log.Fatal(err)
	}
}
func runCommand(args []string) {
	switch args[0] {
	case "inspect":
		flags := flag.NewFlagSet("inspect", flag.ExitOnError)
		server := flags.String("server", "http://localhost:8080", "API base URL")
		workspace := flags.String("workspace", "", "workspace ID")
		flags.Parse(args[1:])
		response, err := http.Get(*server + "/v1/workspaces/" + *workspace)
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			log.Fatalf("inspect failed: %s", response.Status)
		}
		_ = json.NewEncoder(os.Stdout).Encode(json.RawMessage(mustRead(response)))
	case "diff":
		flags := flag.NewFlagSet("diff", flag.ExitOnError)
		from := flags.String("from", "", "base Git revision")
		to := flags.String("to", "HEAD", "target Git revision")
		dir := flags.String("workspace", ".", "Git workspace")
		flags.Parse(args[1:])
		command := exec.Command("git", "-C", *dir, "diff", *from, *to)
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := command.Run(); err != nil {
			log.Fatal(err)
		}
	case "restore":
		flags := flag.NewFlagSet("restore", flag.ExitOnError)
		bundle := flags.String("bundle", "", "local Git bundle")
		destination := flags.String("destination", "", "empty restore directory")
		flags.Parse(args[1:])
		if *bundle == "" || *destination == "" || filepath.Clean(*destination) == "." {
			log.Fatal("bundle and destination are required")
		}
		if err := exec.Command("git", "clone", *bundle, *destination).Run(); err != nil {
			log.Fatal(err)
		}
	case "verify":
		flags := flag.NewFlagSet("verify", flag.ExitOnError)
		file := flags.String("event", "", "event envelope JSON")
		flags.Parse(args[1:])
		bytes, err := os.ReadFile(*file)
		if err != nil {
			log.Fatal(err)
		}
		var envelope evolution.EventEnvelope
		if err := json.Unmarshal(bytes, &envelope); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("event=%s content_hash=%s signer=%s\\n", envelope.Event.ID, envelope.ContentHash, envelope.KeyID)
	case "cache-health", "cache-repair":
		flags := flag.NewFlagSet(args[0], flag.ExitOnError)
		root := flags.String("root", "", "local workspace cache root")
		flags.Parse(args[1:])
		if *root == "" {
			log.Fatal("cache root is required")
		}
		cache := persistence.WorkspaceCache{Root: *root}
		var health persistence.CacheHealth
		var err error
		if args[0] == "cache-repair" {
			health, err = cache.Repair(context.Background())
		} else {
			health, err = cache.Health(context.Background())
		}
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(health)
	default:
		log.Fatalf("usage: rsi-state [serve|inspect|diff|restore|verify|cache-health|cache-repair]")
	}
}
func mustRead(response *http.Response) []byte {
	bytes, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	return bytes
}
