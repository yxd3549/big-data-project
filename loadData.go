package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func uploadUser(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY \"user\" FROM '/home/dan/Documents/College/BigData/Project/big-data-project/user_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func uploadTag(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY tag FROM '/home/dan/Documents/College/BigData/Project/big-data-project/tag_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func uploadGenre(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY genre FROM '/home/dan/Documents/College/BigData/Project/big-data-project/genre_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func uploadKind(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY kind FROM '/home/dan/Documents/College/BigData/Project/big-data-project/kind_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func uploadLicense(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY license FROM '/home/dan/Documents/College/BigData/Project/big-data-project/license_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func uploadTrack() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY track FROM '/home/dan/Documents/College/BigData/Project/big-data-project/track_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func uploadTrackTags() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Error(err)
	}

	queryString := "COPY track_label FROM '/home/dan/Documents/College/BigData/Project/big-data-project/track_tag_file' " +
		"WITH (DELIMITER '|', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func main() {

	start := time.Now()

	wg := new(sync.WaitGroup)

	wg.Add(5)

	//Everything depends on these but they don't depend on anything so we can add them all in parralel
	uploadUser(wg)
	uploadTag(wg)
	uploadGenre(wg)
	uploadKind(wg)
	uploadLicense(wg)

	wg.Wait()

	uploadTrack()
	uploadTrackTags()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
