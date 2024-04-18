package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/abelanger5/postgres-fair-queue/internal/cmdutils"
	"github.com/abelanger5/postgres-fair-queue/internal/dbsqlc"
	"github.com/spf13/cobra"
)

// seedCmd represents the seed command
var seedCmd = &cobra.Command{
	Use:   "seed",
	Short: "seed the database with some tasks.",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := cmdutils.NewInterruptContext()
		defer cancel()

		seed(ctx)
	},
}

var seedCount int
var partitions int
var batchSize int

func init() {
	rootCmd.AddCommand(seedCmd)

	seedCmd.PersistentFlags().IntVarP(
		&seedCount,
		"count",
		"c",
		10,
		"The number of tasks to seed.",
	)

	seedCmd.PersistentFlags().IntVarP(
		&partitions,
		"partitions",
		"p",
		10,
		"The number of partitions to seed.",
	)

	seedCmd.PersistentFlags().IntVarP(
		&batchSize,
		"batch-size",
		"b",
		100,
		"The batch size for seeding.",
	)
}

type seedData struct {
	Group int `json:"group"`
	Index int `json:"index"`
}

func seed(ctx context.Context) {
	batchCount := 0

	for remaining := seedCount; remaining > 0; remaining -= min(batchSize, remaining) {
		j := min(batchSize, remaining)

		log.Printf("seeding batch of %d\n", j)

		batch := make([]dbsqlc.CreateTaskParams, 0)

		for i := 0; i < j; i++ {
			data, _ := json.Marshal(seedData{
				Group: batchCount % partitions,
				Index: i,
			})

			batch = append(batch, dbsqlc.CreateTaskParams{
				Args:     data,
				GroupKey: fmt.Sprintf("group-%d", batchCount%partitions),
			})
		}

		err := queries.CreateTask(ctx, pool, batch).Close()

		if err != nil {
			log.Fatalf("could not seed tasks: %v", err)
		}

		log.Printf("seeded tasks %d\n", len(batch))

		batchCount++
	}
}
