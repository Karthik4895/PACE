CREATE TABLE IF NOT EXISTS tickets (
    id VARCHAR(255) PRIMARY KEY,
    ticket_id VARCHAR(50),
    summary TEXT,
    assignee VARCHAR(255),
    priority VARCHAR(50),
    status VARCHAR(50),
    created_at TIMESTAMP,
    resolved_at TIMESTAMP,
    sprint VARCHAR(255),
    story_points INT,
    days_to_resolve INT
);

CREATE TABLE IF NOT EXISTS aws_costs (
    id SERIAL PRIMARY KEY,
    billing_month DATE NOT NULL,
    service_name TEXT NOT NULL,
    usage_type TEXT,
    usage_quantity FLOAT,
    unblended_cost FLOAT,
    currency TEXT DEFAULT 'USD',
    UNIQUE (billing_month, service_name, usage_type)
);

CREATE TABLE IF NOT EXISTS github_commits (
    id SERIAL PRIMARY KEY,
    repo_name TEXT NOT NULL,
    commit_sha TEXT UNIQUE NOT NULL,
    author_name TEXT,
    commit_message TEXT,
    commit_date TIMESTAMP,
    url TEXT
);

