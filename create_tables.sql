DROP TABLE IF EXISTS "user" CASCADE;
DROP TABLE IF EXISTS tag CASCADE;
DROP TABLE IF EXISTS genre CASCADE;
DROP TABLE IF EXISTS kind CASCADE;
DROP TABLE IF EXISTS license CASCADE;
DROP TABLE IF EXISTS track CASCADE;
DROP TABLE IF EXISTS track_label CASCADE;

CREATE TABLE "user"
(
    id int,
    username varchar(200),
    kind varchar(100),
    last_modified varchar(100),
    permalink varchar(200),
    uri varchar(200),
    PRIMARY KEY (id)
);

CREATE TABLE tag(
    id int,
    tag varchar(100),
    PRIMARY KEY (id)
);

CREATE TABLE genre(
    id int,
    genre varchar(100),
    PRIMARY KEY (id)
);

CREATE TABLE kind(
    id int,
    kind varchar(100),
    PRIMARY KEY (id)
);

CREATE TABLE license(
    id int,
    license varchar(100),
    PRIMARY KEY (id)
);

CREATE TABLE track(
    id int,
    title varchar(200),
    uri varchar(200),
    isrc varchar(100),
    genre int,
    kind  int,
    license int,
    likes_count int,
    commentable bool,
    comment_count int,
    downloadable bool,
    download_count int,
    created_at varchar(100),
    description varchar(1000),
    duration int,
    label_name varchar(100),
    last_modified varchar(100),
    original_content_size int,
    original_format varchar(20),
    permalink varchar(200),
    permalink_url varchar(500),
    playback_count int,
    retrieved_utc int,
    stream_url varchar(500),
    streamable bool,
    track_type varchar(100),
    waveform_url varchar(200),

    PRIMARY KEY (id),
    FOREIGN KEY (genre) references genre(id),
    FOREIGN KEY (kind) references kind(id),
    FOREIGN KEY (license) references license(id)
);

CREATE TABLE track_label(
    track int,
    tag int,
    PRIMARY KEY (track, tag),
    FOREIGN KEY (track) references track(id),
    FOREIGN KEY (tag) references tag(id)
);