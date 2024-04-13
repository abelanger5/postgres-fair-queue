-- CreateEnum
CREATE TYPE "TaskStatus" AS ENUM (
    'QUEUED',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
);

-- CreateTable
CREATE TABLE
    tasks (
        id BIGINT NOT NULL,
        created_at timestamp,
        status "TaskStatus" NOT NULL,
        args jsonb,
        group_key text,
        PRIMARY KEY (id)
    );

-- CreateTable
CREATE TABLE
    task_groups (
        id BIGSERIAL NOT NULL,
        group_key text,
        block_addr BIGINT,
        PRIMARY KEY (id)
    );

CREATE TABLE
    task_addr_ptrs (
        max_assigned_block_addr BIGINT NOT NULL,
        onerow_id bool PRIMARY KEY DEFAULT true,
        CONSTRAINT onerow_uni CHECK (onerow_id)
    );

INSERT INTO
    task_addr_ptrs (max_assigned_block_addr)
VALUES
    (0);

ALTER TABLE task_groups ADD CONSTRAINT unique_group_key UNIQUE (group_key);

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_group_key FOREIGN KEY (group_key) REFERENCES task_groups (group_key);