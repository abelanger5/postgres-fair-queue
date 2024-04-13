package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/abelanger5/postgres-partitioned-queue/internal/cmdutils"
	"github.com/abelanger5/postgres-partitioned-queue/internal/dbsqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var numWorkers int
var limitPerWorker int
var strategy string
var pollInterval string
var concurrencyMaxRuns int
var workerTaskDelay string

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "worker starts a set of workers to poll for tasks.",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := cmdutils.NewInterruptContext()
		defer cancel()

		eg := errgroup.Group{}
		eg.SetLimit(numWorkers)

		delay, err := time.ParseDuration(workerTaskDelay)

		if err != nil {
			log.Fatalf("could not parse task delay: %v", err)
		}

		for i := 0; i < numWorkers; i++ {
			idx := i

			eg.Go(func() error {
				poll(ctx, idx, func(ctx context.Context, task *dbsqlc.Task) {
					go func() {
						time.Sleep(delay)

						_, err := queries.UpdateTaskStatus(ctx, pool, dbsqlc.UpdateTaskStatusParams{
							ID: pgtype.Int8{
								Int64: int64(task.ID),
								Valid: true,
							},
							Status: dbsqlc.NullTaskStatus{
								TaskStatus: dbsqlc.TaskStatusSUCCEEDED,
								Valid:      true,
							},
						})

						if err != nil {
							log.Printf("[worker %d] could not update task status: %v", idx, err)
							return
						}
					}()
				})

				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			log.Fatalf("error in worker: %v", err)
		}
	},
}

var pool *pgxpool.Pool
var queries *dbsqlc.Queries

func init() {
	dbUrl := os.Getenv("DATABASE_URL")

	if dbUrl == "" {
		log.Fatal("DATABASE_URL must be set")
	}

	config, err := pgxpool.ParseConfig(dbUrl)

	if err != nil {
		log.Fatal("could not parse DATABASE_URL: %w", err)
	}

	config.MaxConns = 5

	pool, err = pgxpool.NewWithConfig(context.Background(), config)

	if err != nil {
		log.Fatal("could not create connection pool: %w", err)
	}

	queries = dbsqlc.New()

	rootCmd.AddCommand(workerCmd)

	workerCmd.PersistentFlags().IntVarP(
		&numWorkers,
		"workers",
		"w",
		1,
		"The number of workers to start.",
	)

	workerCmd.PersistentFlags().IntVarP(
		&limitPerWorker,
		"limit",
		"l",
		10,
		"The number of tasks to process per worker.",
	)

	workerCmd.PersistentFlags().StringVarP(
		&strategy,
		"strategy",
		"s",
		"fifo",
		"The strategy to use for polling tasks.",
	)

	workerCmd.PersistentFlags().StringVarP(
		&pollInterval,
		"poll-interval",
		"i",
		"5s",
		"The interval at which to poll for tasks.",
	)

	workerCmd.PersistentFlags().IntVarP(
		&concurrencyMaxRuns,
		"concurrency-max-runs",
		"m",
		4,
		"The maximum number of runs for the round-robin concurrency strategy.",
	)

	workerCmd.PersistentFlags().StringVarP(
		&workerTaskDelay,
		"task-delay",
		"d",
		"0s",
		"The delay to introduce for each task in the worker.",
	)
}

type HandleTask func(ctx context.Context, task *dbsqlc.Task)

func poll(ctx context.Context, id int, handleTask HandleTask) {
	interval, err := time.ParseDuration(pollInterval)

	if err != nil {
		log.Fatalf("could not parse poll interval: %v", err)
	}

	// sleep for random duration between 0 and polling interval to avoid thundering herd
	// sleepDuration := time.Duration(id) * interval / time.Duration(numWorkers)
	log.Printf("(worker %d) sleeping for %v\n", id, time.Duration(0))
	// time.Sleep(sleepDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			var (
				tasks []*dbsqlc.Task
				err   error
			)

			if strategy == "fifo" {
				tasks, err = queries.PopTasks(ctx, pool, pgtype.Int4{Int32: int32(limitPerWorker), Valid: true})

				if err != nil {
					log.Printf("could not pop tasks: %v", err)
					continue
				}
			} else if strategy == "round-robin" {
				eg := errgroup.Group{}

				eg.Go(func() error {
					tx, err := pool.BeginTx(ctx, pgx.TxOptions{})

					if err != nil {
						log.Printf("could not begin transaction: %v", err)
						return err
					}

					defer tx.Rollback(ctx)

					tasks, err = queries.PopTasks(ctx, tx, pgtype.Int4{Int32: int32(limitPerWorker), Valid: true})

					if err != nil {
						log.Printf("(worker %d) could not pop tasks: %v", id, err)
						return err
					}

					_, err = queries.UpdateTaskPtrs(ctx, tx)

					if err != nil {
						log.Printf("(worker %d) could not update address pointer: %v", id, err)
						return err
					}

					err = tx.Commit(ctx)

					if err != nil {
						log.Printf("(worker %d) could not commit transaction: %v", id, err)
						return err
					}

					log.Printf("(worker %d) popped %d tasks\n", id, len(tasks))

					return nil
				})

				if err := eg.Wait(); err != nil {
					log.Printf("could not pop tasks: %v", err)
					continue
				}
			} else if strategy == "round-robin-concurrency" {
				eg := errgroup.Group{}

				eg.Go(func() error {
					tx, err := pool.BeginTx(ctx, pgx.TxOptions{})

					if err != nil {
						log.Printf("could not begin transaction: %v", err)
						return err
					}

					defer tx.Rollback(ctx)

					tasks, err = queries.PopTasksWithConcurrency(ctx, tx, dbsqlc.PopTasksWithConcurrencyParams{
						Limit:       pgtype.Int4{Int32: int32(limitPerWorker), Valid: true},
						Concurrency: int32(concurrencyMaxRuns),
					})

					if err != nil {
						log.Printf("(worker %d) could not pop tasks: %v", id, err)
						return err
					}

					_, err = queries.UpdateTaskPtrs(ctx, tx)

					if err != nil {
						log.Printf("(worker %d) could not update address pointer: %v", id, err)
						return err
					}

					err = tx.Commit(ctx)

					if err != nil {
						log.Printf("(worker %d) could not commit transaction: %v", id, err)
						return err
					}

					log.Printf("(worker %d) popped %d tasks\n", id, len(tasks))

					return nil
				})

				if err := eg.Wait(); err != nil {
					log.Printf("could not pop tasks: %v", err)
					continue
				}
			}

			for _, task := range tasks {
				handleTask(ctx, task)
			}
		}
	}
}
