/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Version information
var (
	gitSHA1  = "unknown"
	gitDirty = "unknown"
)

// SetVersionInfo sets the version information from main
func SetVersionInfo(sha, dirty string) {
	gitSHA1 = sha
	gitDirty = dirty
}

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  "Print version information including git commit hash and build time",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("serverless-cache-benchmark\n")
		fmt.Printf("Git Commit: %s", gitSHA1)
		if gitDirty != "0" && gitDirty != "unknown" {
			fmt.Printf(" (dirty)")
		}
		fmt.Printf("\n")
	},
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "serverless-cache-benchmark",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Check for --version flag
		if version, _ := cmd.Flags().GetBool("version"); version {
			fmt.Printf("serverless-cache-benchmark\n")
			fmt.Printf("Git Commit: %s", gitSHA1)
			if gitDirty != "0" && gitDirty != "unknown" {
				fmt.Printf(" (dirty)")
			}
			fmt.Printf("\n")
			return
		}
		// If no version flag, show help
		cmd.Help()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Add version command
	rootCmd.AddCommand(versionCmd)

	// Add --version flag to root command
	rootCmd.Flags().BoolP("version", "V", false, "Print version information")

}
