package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "queue",
	Short: "queue demonstrates simple commands for a postgres-backed queue in Golang.",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
