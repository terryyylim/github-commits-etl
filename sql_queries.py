# DROP TABLES BEFORE CREATING
commits_table_drop = "DROP TABLE IF EXISTS commits CASCADE"
authors_table_drop = "DROP TABLE IF EXISTS authors CASCADE"

commits_table_create = (
    """
        CREATE TABLE commits(
            email VARCHAR(100) REFERENCES authors(email),
            date TIMESTAMP,
            PRIMARY KEY (email, date)
        )
    """
)

authors_table_create = (
    """
        CREATE TABLE authors(
            email VARCHAR(100),
            name VARCHAR(80),
            first_commit TIMESTAMP,
            last_commit TIMESTAMP,
            PRIMARY KEY (email)
        )
    """
)

commits_table_insert = (
    """
        INSERT INTO commits (
            email,
            date
        ) VALUES (%s,%s)
    """
)

authors_table_insert = (
    """
        INSERT INTO authors (
            email,
            name,
            first_commit,
            last_commit
        ) VALUES (%s,%s,%s,%s)
    """
)

# Authors table should be inserted before commits table due to foreign key reference on `email` column.
create_table_queries = [authors_table_create, commits_table_create]
drop_table_queries = [authors_table_drop, commits_table_drop]

# Insight 1: Top 3 authors in the given time period
insight_1_query = (
    """
        SELECT
            c.email,
            a.name,
            c.num_commits
        FROM
            (
                SELECT
                    email,
                    COUNT(email) num_commits
                FROM
                    commits
                GROUP BY
                    email
                ORDER BY
                    num_commits DESC
                LIMIT 3
            ) AS c
        LEFT JOIN authors a
        ON c.email = a.email
    """
)

# Insight 2: Author with the longest contribution window
insight_2_query = (
    """
        SELECT
            email,
            (last_commit::timestamp - first_commit::timestamp) contribution_window
        FROM
            authors
        ORDER BY contribution_window DESC
        LIMIT 3
    """
)

# Insight 3: Heatmap of commits
insight_3_query = (
    """
        SELECT
            date
        FROM
            commits
    """
)
