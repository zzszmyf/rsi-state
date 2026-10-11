package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotManifestRoundTripAndHydratesIsolatedCache(t *testing.T) {
	t.Parallel()
	objects := NewMemoryObjects()
	snapshots := SnapshotStore{Objects: objects, Prefix: "rsi"}
	now := time.Date(2026, 10, 11, 6, 0, 0, 0, time.UTC)
	manifest, err := snapshots.Put(context.Background(), "arc", 7, []byte("git-bundle"), now)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != SnapshotManifestVersion || manifest.BundleSHA256 == "" || manifest.ManifestSHA256 == "" {
		t.Fatalf("incomplete manifest: %#v", manifest)
	}

	cache := WorkspaceCache{Root: t.TempDir(), Snapshots: snapshots, MaxEntries: 2}
	first, err := cache.Hydrate(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Hydrate(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || filepath.Base(first) != "7-"+manifest.BundleSHA256[:12] {
		t.Fatalf("cache paths = %q, %q", first, second)
	}
	got, err := os.ReadFile(filepath.Join(first, bundleFilename))
	if err != nil || string(got) != "git-bundle" {
		t.Fatalf("cached bundle = %q, %v", got, err)
	}
	if filepath.Dir(filepath.Dir(first)) == cache.Root {
		t.Fatalf("cache path is not isolated: %q", first)
	}
}

func TestWorkspaceCacheRejectsTamperedBundleAndCleansPartial(t *testing.T) {
	t.Parallel()
	objects := NewMemoryObjects()
	snapshots := SnapshotStore{Objects: objects}
	manifest, err := snapshots.Put(context.Background(), "arc", 1, []byte("original"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	objects.Tamper(manifest.BundleRef, []byte("tampered"))
	cache := WorkspaceCache{Root: t.TempDir(), Snapshots: snapshots}
	if _, err := cache.Hydrate(context.Background(), manifest); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Hydrate() error = %v, want integrity error", err)
	}
	entries, err := os.ReadDir(cache.Root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("partial cache entries were retained: %#v", entries)
	}
}

func TestSnapshotStoreRejectsTamperedManifest(t *testing.T) {
	t.Parallel()
	objects := NewMemoryObjects()
	snapshots := SnapshotStore{Objects: objects, Prefix: "rsi"}
	manifest, err := snapshots.Put(context.Background(), "arc", 1, []byte("bundle"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	manifest.BundleRef = "attacker.bundle"
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	objects.Tamper(snapshots.manifestKey(manifest), body)
	if _, err := snapshots.Get(context.Background(), "arc", 1); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Get() error = %v, want manifest integrity error", err)
	}
}

func TestWorkspaceCacheValidatesVersionFreshnessAndEvictsOldest(t *testing.T) {
	t.Parallel()
	objects := NewMemoryObjects()
	snapshots := SnapshotStore{Objects: objects}
	now := time.Date(2026, 10, 11, 6, 0, 0, 0, time.UTC)
	first, err := snapshots.Put(context.Background(), "arc", 1, []byte("one"), now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := snapshots.Put(context.Background(), "arc", 2, []byte("two"), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cache := WorkspaceCache{Root: t.TempDir(), Snapshots: snapshots, MaxEntries: 1, MaxAge: time.Hour, Now: func() time.Time { return now.Add(30 * time.Minute) }}
	if _, err := cache.Hydrate(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Hydrate(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	health, err := cache.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Entries != 1 || health.Healthy != 1 {
		t.Fatalf("health = %#v", health)
	}
	stale := first
	stale.CreatedAt = now.Add(-2 * time.Hour)
	stale.ManifestSHA256, err = manifestChecksum(stale)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Hydrate(context.Background(), stale); !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("Hydrate(stale) error = %v", err)
	}
	unsupported := second
	unsupported.SchemaVersion++
	if _, err := cache.Hydrate(context.Background(), unsupported); !errors.Is(err, ErrUnsupportedManifest) {
		t.Fatalf("Hydrate(unsupported) error = %v", err)
	}
}

func TestWorkspaceCacheRepairRemovesCorruptEntry(t *testing.T) {
	t.Parallel()
	objects := NewMemoryObjects()
	snapshots := SnapshotStore{Objects: objects}
	manifest, err := snapshots.Put(context.Background(), "arc", 1, []byte("bundle"), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	cache := WorkspaceCache{Root: t.TempDir(), Snapshots: snapshots}
	dir, err := cache.Hydrate(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, bundleFilename), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	health, err := cache.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Entries != 0 || health.Repaired != 1 {
		t.Fatalf("repair health = %#v", health)
	}
}

func TestWorkspaceCacheSeparatesUnsafeWorkspaceIDs(t *testing.T) {
	t.Parallel()
	objects := NewMemoryObjects()
	snapshots := SnapshotStore{Objects: objects}
	first, err := snapshots.Put(context.Background(), "a/b", 1, []byte("one"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second, err := snapshots.Put(context.Background(), "a_b", 1, []byte("two"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	cache := WorkspaceCache{Root: t.TempDir(), Snapshots: snapshots}
	firstPath, err := cache.Hydrate(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	secondPath, err := cache.Hydrate(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstPath == secondPath || filepath.Dir(filepath.Dir(firstPath)) == filepath.Dir(filepath.Dir(secondPath)) {
		t.Fatalf("workspace cache paths overlap: %q and %q", firstPath, secondPath)
	}
}
