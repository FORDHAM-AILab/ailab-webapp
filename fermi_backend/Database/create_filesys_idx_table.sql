DROP TABLE IF EXISTS filesys_idx;
CREATE TABLE filesys_idx (
    id VARCHAR(256),
    name VARCHAR(256),
    date_uploaded DATE,
    uploader_id VARCHAR(256),
    location VARCHAR(256),
    about LONGTEXT,
    PRIMARY KEY (id, name)
);