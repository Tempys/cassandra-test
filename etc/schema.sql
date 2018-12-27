CREATE TABLE books_small (
    id uuid,
    title text,
    author text,
    date timestamp,
    subject text,
    PRIMARY KEY(id)
);


CREATE TABLE books_index1 (
    id uuid,
    title text,
    author text,
    date timestamp,
    subject text,
    PRIMARY KEY(id,date)
)WITH CLUSTERING ORDER BY(date DESC)
     AND compaction = {'class':'TimeWindowCompactionStrategy', 'compaction_window_unit':'MINUTES','compaction_window_size':'1'}
     AND default_time_to_live=800000;