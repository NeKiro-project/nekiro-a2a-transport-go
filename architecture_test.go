package a2atransport

import (
	"os/exec"
	"strings"
	"testing"
)

func TestDependencyGraphContainsNoNeKiroPlatformModule(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, output)
	}
	for _, importPath := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if isNeKiroPlatformImport(importPath) {
			t.Fatalf("standalone dependency graph imports platform package %q", importPath)
		}
	}
}

func isNeKiroPlatformImport(importPath string) bool {
	for _, root := range []string{
		"github.com/NeKiro-project/NeKiro",
		"github.com/Nene7ko/NeKiro",
	} {
		if importPath == root || strings.HasPrefix(importPath, root+"/") {
			return true
		}
	}
	return false
}
