package http

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var markdownLinkPattern = regexp.MustCompile(`\[[^\]]+\]\(([^)]+)\)`)

func TestPublishedDocumentationContainsNoInternalReferences(t *testing.T) {
	for _, file := range publishedDocumentationFiles(t) {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		content := strings.ToLower(string(data))
		for _, forbidden := range []string{
			"file:///",
			".internal-docs",
			"docs/superpowers",
		} {
			if strings.Contains(content, strings.ToLower(forbidden)) {
				t.Errorf("%s contains non-public reference %q", file, forbidden)
			}
		}
	}
}

func TestPublishedMarkdownLinksResolve(t *testing.T) {
	for _, file := range publishedDocumentationFiles(t) {
		if filepath.Ext(file) != ".md" {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, match := range markdownLinkPattern.FindAllStringSubmatch(string(data), -1) {
			target := strings.Trim(match[1], "<>")
			if target == "" || strings.HasPrefix(target, "#") ||
				strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			target, _, _ = strings.Cut(target, "#")
			if _, err := os.Stat(filepath.Join(filepath.Dir(file), filepath.FromSlash(target))); err != nil {
				t.Errorf("%s contains unresolved link %q", file, match[1])
			}
		}
	}
}

func publishedDocumentationFiles(t *testing.T) []string {
	t.Helper()
	files := []string{"README.md", "CONTRIBUTING.md", "CHANGELOG.md", "SECURITY.md"}
	for _, root := range []string{"docs", "examples"} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			switch filepath.Ext(path) {
			case ".md", ".yaml", ".yml":
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	return files
}
