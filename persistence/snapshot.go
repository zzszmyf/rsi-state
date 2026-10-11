package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	SnapshotManifestVersion = 1
	bundleFilename          = "workspace.bundle"
	manifestFilename        = "manifest.json"
)

var (
	ErrIntegrity           = errors.New("snapshot integrity verification failed")
	ErrStaleSnapshot       = errors.New("snapshot is too old")
	ErrUnsupportedManifest = errors.New("unsupported snapshot manifest version")
	cacheLocks             sync.Map
)

// SnapshotManifest binds an immutable workspace bundle to the revision that
// produced it. ManifestSHA256 covers every manifest field except itself.
type SnapshotManifest struct {
	SchemaVersion  int       `json:"schema_version"`
	WorkspaceID    string    `json:"workspace_id"`
	Revision       int64     `json:"revision"`
	BundleRef      string    `json:"bundle_ref"`
	BundleSHA256   string    `json:"bundle_sha256"`
	BundleSize     int64     `json:"bundle_size"`
	CreatedAt      time.Time `json:"created_at"`
	ManifestSHA256 string    `json:"manifest_sha256"`
}

// SnapshotStore persists content-addressed bundles and their verifiable
// manifests using the existing durable object interface.
type SnapshotStore struct {
	Objects ObjectStore
	Prefix  string
}

func (s SnapshotStore) Put(ctx context.Context, workspaceID string, revision int64, bundle []byte, createdAt time.Time) (SnapshotManifest, error) {
	if s.Objects == nil || strings.TrimSpace(workspaceID) == "" || revision < 0 {
		return SnapshotManifest{}, fmt.Errorf("%w: object store, workspace ID, and revision are required", evolutionInvalid())
	}
	bundleHash := checksum(bundle)
	bundleRef := s.key("bundles", bundleHash+".bundle")
	if err := s.Objects.Put(ctx, bundleRef, bundle, "application/octet-stream"); err != nil {
		return SnapshotManifest{}, fmt.Errorf("write snapshot bundle: %w", err)
	}
	manifest := SnapshotManifest{
		SchemaVersion: SnapshotManifestVersion, WorkspaceID: workspaceID, Revision: revision,
		BundleRef: bundleRef, BundleSHA256: bundleHash, BundleSize: int64(len(bundle)), CreatedAt: createdAt.UTC(),
	}
	hash, err := manifestChecksum(manifest)
	if err != nil {
		return SnapshotManifest{}, err
	}
	manifest.ManifestSHA256 = hash
	body, err := json.Marshal(manifest)
	if err != nil {
		return SnapshotManifest{}, err
	}
	if err := s.Objects.Put(ctx, s.manifestKey(manifest), body, "application/json"); err != nil {
		return SnapshotManifest{}, fmt.Errorf("write snapshot manifest: %w", err)
	}
	return manifest, nil
}

func (s SnapshotStore) Get(ctx context.Context, workspaceID string, revision int64) (SnapshotManifest, error) {
	var manifest SnapshotManifest
	body, err := s.Objects.Get(ctx, s.manifestKey(SnapshotManifest{WorkspaceID: workspaceID, Revision: revision}))
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return manifest, fmt.Errorf("%w: decode manifest: %v", ErrIntegrity, err)
	}
	if err := validateManifest(manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func (s SnapshotStore) key(parts ...string) string {
	all := make([]string, 0, len(parts)+1)
	if prefix := strings.Trim(s.Prefix, "/"); prefix != "" {
		all = append(all, prefix)
	}
	all = append(all, parts...)
	return filepath.ToSlash(filepath.Join(all...))
}
func (s SnapshotStore) manifestKey(manifest SnapshotManifest) string {
	return s.key("manifests", safePathPart(manifest.WorkspaceID), fmt.Sprintf("%020d.json", manifest.Revision))
}

// WorkspaceCache hydrates verified bundles only underneath Root. A snapshot
// never shares a directory with another workspace or revision.
type WorkspaceCache struct {
	Root       string
	Snapshots  SnapshotStore
	MaxEntries int
	MaxAge     time.Duration
	Now        func() time.Time
}

type CacheHealth struct {
	Entries  int `json:"entries"`
	Healthy  int `json:"healthy"`
	Corrupt  int `json:"corrupt"`
	Evicted  int `json:"evicted"`
	Repaired int `json:"repaired"`
}

func (c WorkspaceCache) Hydrate(ctx context.Context, manifest SnapshotManifest) (string, error) {
	if err := validateManifest(manifest); err != nil {
		return "", err
	}
	now := c.now()
	if c.MaxAge > 0 && now.Sub(manifest.CreatedAt) > c.MaxAge {
		return "", fmt.Errorf("%w: created %s", ErrStaleSnapshot, manifest.CreatedAt.UTC().Format(time.RFC3339))
	}
	if strings.TrimSpace(c.Root) == "" || c.Snapshots.Objects == nil {
		return "", fmt.Errorf("cache root and snapshot store are required")
	}
	lock := cacheLock(c.Root)
	lock.Lock()
	defer lock.Unlock()
	target := c.entryPath(manifest)
	if err := c.validateEntry(target, manifest); err == nil {
		_ = os.Chtimes(target, now, now)
		return target, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		if removeErr := os.RemoveAll(target); removeErr != nil {
			return "", removeErr
		}
	}
	bundle, err := c.Snapshots.Objects.Get(ctx, manifest.BundleRef)
	if err != nil {
		return "", fmt.Errorf("download snapshot bundle: %w", err)
	}
	if err := verifyBundle(manifest, bundle); err != nil {
		return "", err
	}
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", err
	}
	staging, err := os.MkdirTemp(parent, ".hydrate-")
	if err != nil {
		return "", err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(staging)
		}
	}()
	if err := writeCacheEntry(staging, manifest, bundle); err != nil {
		return "", err
	}
	if err := os.RemoveAll(target); err != nil {
		return "", err
	}
	if err := os.Rename(staging, target); err != nil {
		return "", err
	}
	cleanup = false
	if _, err := c.evictLocked(now); err != nil {
		return "", err
	}
	return target, nil
}

func (c WorkspaceCache) Health(_ context.Context) (CacheHealth, error) {
	lock := cacheLock(c.Root)
	lock.Lock()
	defer lock.Unlock()
	return c.healthLocked(false)
}

// Repair removes corrupt cache entries and applies the configured entry bound.
func (c WorkspaceCache) Repair(_ context.Context) (CacheHealth, error) {
	lock := cacheLock(c.Root)
	lock.Lock()
	defer lock.Unlock()
	health, err := c.healthLocked(true)
	if err != nil {
		return health, err
	}
	evicted, err := c.evictLocked(c.now())
	health.Evicted += evicted
	return health, err
}

func (c WorkspaceCache) healthLocked(repair bool) (CacheHealth, error) {
	var health CacheHealth
	entries, err := c.entries()
	if errors.Is(err, os.ErrNotExist) {
		return health, nil
	}
	if err != nil {
		return health, err
	}
	for _, entry := range entries {
		health.Entries++
		manifest, err := readManifest(entry.path)
		if err == nil {
			err = c.validateEntry(entry.path, manifest)
		}
		if err == nil {
			health.Healthy++
			continue
		}
		health.Corrupt++
		if repair {
			if err := os.RemoveAll(entry.path); err != nil {
				return health, err
			}
			health.Entries--
			health.Corrupt--
			health.Repaired++
		}
	}
	return health, nil
}

type cacheEntry struct {
	path string
	info os.FileInfo
}

func (c WorkspaceCache) entries() ([]cacheEntry, error) {
	root := filepath.Join(c.Root, "workspaces")
	var entries []cacheEntry
	err := filepath.WalkDir(root, func(entry string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		if _, err := os.Stat(filepath.Join(entry, manifestFilename)); err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		entries = append(entries, cacheEntry{path: entry, info: info})
		return filepath.SkipDir
	})
	return entries, err
}

func (c WorkspaceCache) evictLocked(now time.Time) (int, error) {
	entries, err := c.entries()
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var stale []cacheEntry
	if c.MaxAge > 0 {
		for _, entry := range entries {
			manifest, err := readManifest(entry.path)
			if err == nil && now.Sub(manifest.CreatedAt) > c.MaxAge {
				stale = append(stale, entry)
			}
		}
	}
	remove := stale
	remaining := make([]cacheEntry, 0, len(entries)-len(stale))
	for _, entry := range entries {
		found := false
		for _, old := range stale {
			found = found || old.path == entry.path
		}
		if !found {
			remaining = append(remaining, entry)
		}
	}
	if c.MaxEntries > 0 && len(remaining) > c.MaxEntries {
		sort.Slice(remaining, func(i, j int) bool { return remaining[i].info.ModTime().Before(remaining[j].info.ModTime()) })
		remove = append(remove, remaining[:len(remaining)-c.MaxEntries]...)
	}
	for _, entry := range remove {
		if err := os.RemoveAll(entry.path); err != nil {
			return 0, err
		}
	}
	return len(remove), nil
}

func (c WorkspaceCache) entryPath(manifest SnapshotManifest) string {
	return filepath.Join(c.Root, "workspaces", safePathPart(manifest.WorkspaceID), "revisions", fmt.Sprintf("%d-%s", manifest.Revision, manifest.BundleSHA256[:12]))
}
func (c WorkspaceCache) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}
func (c WorkspaceCache) validateEntry(dir string, want SnapshotManifest) error {
	got, err := readManifest(dir)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: cached manifest differs", ErrIntegrity)
	}
	bundle, err := os.ReadFile(filepath.Join(dir, bundleFilename))
	if err != nil {
		return err
	}
	return verifyBundle(want, bundle)
}
func writeCacheEntry(dir string, manifest SnapshotManifest, bundle []byte) error {
	body, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, bundleFilename), bundle, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, manifestFilename), body, 0o600)
}
func readManifest(dir string) (SnapshotManifest, error) {
	var manifest SnapshotManifest
	body, err := os.ReadFile(filepath.Join(dir, manifestFilename))
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return manifest, fmt.Errorf("%w: decode cached manifest: %v", ErrIntegrity, err)
	}
	return manifest, validateManifest(manifest)
}
func validateManifest(manifest SnapshotManifest) error {
	if manifest.SchemaVersion != SnapshotManifestVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedManifest, manifest.SchemaVersion)
	}
	if strings.TrimSpace(manifest.WorkspaceID) == "" || manifest.Revision < 0 || manifest.BundleSize < 0 ||
		len(manifest.BundleSHA256) != sha256.Size*2 || manifest.BundleRef == "" || manifest.CreatedAt.IsZero() {
		return fmt.Errorf("%w: incomplete manifest", ErrIntegrity)
	}
	want, err := manifestChecksum(manifest)
	if err != nil || manifest.ManifestSHA256 != want {
		return fmt.Errorf("%w: manifest checksum mismatch", ErrIntegrity)
	}
	return nil
}
func manifestChecksum(manifest SnapshotManifest) (string, error) {
	manifest.ManifestSHA256 = ""
	body, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	return checksum(body), nil
}
func verifyBundle(manifest SnapshotManifest, bundle []byte) error {
	if int64(len(bundle)) != manifest.BundleSize || checksum(bundle) != manifest.BundleSHA256 {
		return fmt.Errorf("%w: bundle checksum mismatch", ErrIntegrity)
	}
	return nil
}
func checksum(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
func cacheLock(root string) *sync.Mutex {
	lock, _ := cacheLocks.LoadOrStore(filepath.Clean(root), &sync.Mutex{})
	return lock.(*sync.Mutex)
}
func safePathPart(part string) string {
	original := part
	part = strings.ReplaceAll(part, "/", "_")
	part = strings.ReplaceAll(part, "\\", "_")
	if part == "" || part == "." || part == ".." {
		part = "_"
	}
	return part + "-" + checksum([]byte(original))[:12]
}

// Kept local so the persistence package does not expose evolution's internal
// validation policy through its snapshot API.
func evolutionInvalid() error {
	return errors.New("invalid snapshot input")
}
