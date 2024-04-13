-- name: UpdateTaskPtrs :one
WITH 
    max_assigned_id AS (
        SELECT
            id
        FROM
            tasks
        WHERE
            "status" != 'QUEUED'
        ORDER BY id DESC
        LIMIT 1
    )
UPDATE task_addr_ptrs
SET
    max_assigned_block_addr = COALESCE(
        FLOOR((SELECT id FROM max_assigned_id)::decimal / 1024 / 1024), 
        COALESCE(
            (SELECT MAX(block_addr) FROM task_groups),
            0
        )
    )
FROM
    max_assigned_id
RETURNING task_addr_ptrs.*;

-- name: CreateTask :batchone
WITH 
    group_key_task AS (
        INSERT INTO task_groups (
            id,
            group_key,
            block_addr
        ) VALUES (
            COALESCE((SELECT max(id) FROM task_groups), -1) + 1,
            sqlc.arg('group_key')::text,
            (SELECT max_assigned_block_addr FROM task_addr_ptrs)
        ) ON CONFLICT (group_key)
        DO UPDATE SET 
            group_key = EXCLUDED.group_key,
            block_addr = GREATEST(
                task_groups.block_addr + 1,
                (SELECT max_assigned_block_addr FROM task_addr_ptrs)
            )
        RETURNING id, group_key, block_addr
    )
INSERT INTO tasks (
    id,
    created_at,
    status,
    args,
    group_key
) VALUES (
    (SELECT id FROM group_key_task) + 1024 * 1024 * (SELECT block_addr FROM group_key_task),
    COALESCE(sqlc.arg('created_at')::timestamp, now()),
    'QUEUED',
    COALESCE(sqlc.arg('args')::jsonb, '{}'::jsonb),
    sqlc.arg('group_key')::text
)
RETURNING *;

-- name: UpdateTaskStatus :one
UPDATE tasks
SET
    "status" = COALESCE(sqlc.narg('status')::"TaskStatus", "status")
WHERE
    id = sqlc.narg('id')::bigint
RETURNING *;

-- name: PopTasks :many
WITH
    eligible_tasks AS (
        SELECT
            tasks.id
        FROM
            tasks
        WHERE
            "status" = 'QUEUED' 
        ORDER BY id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT
            COALESCE(sqlc.narg('limit')::int, 10)
    )
UPDATE tasks
SET
    "status" = 'RUNNING'
FROM
    eligible_tasks
WHERE
    tasks.id = eligible_tasks.id
RETURNING tasks.*;

-- name: PopTasksWithConcurrency :many
WITH
    min_id AS (
        SELECT
            COALESCE(min(id), 0) AS min_id
        FROM
            tasks
        WHERE
            "status" = 'QUEUED'
    ),
    eligible_tasks AS (
        SELECT
            tasks.id
        FROM
            tasks
        WHERE
            "status" = 'QUEUED' AND
            "id" >= (SELECT min_id FROM min_id) AND
            "id" < (SELECT min_id FROM min_id) + sqlc.arg('concurrency')::int * 1024 * 1024
        ORDER BY id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT
            COALESCE(sqlc.narg('limit')::int, 10)
    )
UPDATE tasks
SET
    "status" = 'RUNNING'
FROM
    eligible_tasks
WHERE
    tasks.id = eligible_tasks.id
RETURNING tasks.*;