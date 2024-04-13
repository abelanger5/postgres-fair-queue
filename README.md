This repository accompanies [this blog post](https://docs.hatchet.run/blog/multi-tenant-queues).

## Setup

**Prerequisites:**

- [Taskfile](https://taskfile.dev/)
- Go 1.21+
- Docker Compose

Run `task setup` to get everything running. This will spin up a Postgres database on port 5432, generate the relevant `sqlc`, and write the schema to the database. You might need to run it multiple times if Postgres doesn't start quickly.

Next, set the `DATABASE_URL` environment variable for all commands below:

```
export DATABASE_URL=postgresql://hatchet:hatchet@127.0.0.1:5432/hatchet
```

### `seed`

Seeds the database with `QUEUED` tasks: for example, `go run . seed -c 100000 -b 1000 -p 100`. This would seed 100000 tasks across 100 partitions (groups).

```
seed the database with some tasks.

Usage:
  queue seed [flags]

Flags:
  -b, --batch-size int   The batch size for seeding. (default 100)
  -c, --count int        The number of tasks to seed. (default 10)
  -h, --help             help for seed
  -p, --partitions int   The number of partitions to seed. (default 10)
```

### `worker`

Runs a set of workers which poll for tasks: for example, `go run . worker -w 10 -s round-robin-concurrency -i 1s`.

```
worker starts a set of workers to poll for tasks.

Usage:
  queue worker [flags]

Flags:
  -m, --concurrency-max-runs int   The maximum number of runs for the round-robin concurrency strategy. (default 4)
  -h, --help                       help for worker
  -l, --limit int                  The number of tasks to process per worker. (default 10)
  -i, --poll-interval string       The interval at which to poll for tasks. (default "5s")
  -s, --strategy string            The strategy to use for polling tasks. (default "fifo")
  -d, --task-delay string          The delay to introduce for each task in the worker. (default "0s")
  -w, --workers int                The number of workers to start. (default 1)
```
