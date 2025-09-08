/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/
package main

import "github.com/redis-performance/serverless-cache-benchmark/cmd"

// Version information set by build
var (
	GitSHA1  = "unknown"
	GitDirty = "unknown"
)

func main() {
	// Set version info in cmd package
	cmd.SetVersionInfo(GitSHA1, GitDirty)
	cmd.Execute()
}
