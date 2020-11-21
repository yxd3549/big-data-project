package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

// Need to specifically create L1
func createL1() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE TABLE L1 AS " +
		"(SELECT tag as tag1, COUNT(tag) " +
		"FROM track_label " +
		"GROUP BY tag	" +
		"HAVING COUNT(tag) >= 10000)"

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

func generateSelectString(lattice int) string {

	selectColumn := "tag"

	selectString := "SELECT "

	for i := 1; i < lattice; i++ {
		selectString += "p." + selectColumn
		selectString += strconv.Itoa(i)
		selectString += " as " + selectColumn
		selectString += strconv.Itoa(i)
		selectString += ", "
	}

	selectString += "q." + selectColumn
	selectString += strconv.Itoa(lattice - 1)
	selectString += " as " + selectColumn
	selectString += strconv.Itoa(lattice)
	selectString += ", COUNT(*) as count "

	return selectString
}

func generateFromStatement(lattice int) string {

	fromTable := "track_label"

	alphabetMap := make(map[int]string)
	alphabetMap[0] = "a"
	alphabetMap[1] = "b"
	alphabetMap[2] = "c"
	alphabetMap[3] = "d"
	alphabetMap[4] = "e"
	alphabetMap[5] = "f"
	alphabetMap[6] = "g"
	alphabetMap[7] = "h"
	alphabetMap[8] = "i"
	alphabetMap[9] = "j"
	alphabetMap[10] = "k"
	alphabetMap[11] = "l"
	alphabetMap[12] = "m"
	alphabetMap[13] = "n"
	alphabetMap[14] = "o"
	alphabetMap[15] = "r"
	alphabetMap[16] = "s"
	alphabetMap[17] = "t"
	alphabetMap[18] = "u"
	alphabetMap[19] = "v"
	alphabetMap[20] = "w"
	alphabetMap[21] = "x"
	alphabetMap[22] = "y"
	alphabetMap[23] = "z"

	fromString := "FROM "
	fromString += "L"
	fromString += strconv.Itoa(lattice - 1)
	fromString += " p, "
	fromString += "L"
	fromString += strconv.Itoa(lattice - 1)
	fromString += " q"

	for i := 0; i < lattice; i++ {
		fromString += ", "
		fromString += fromTable + " "
		fromString += alphabetMap[i]
	}
	fromString += " "

	return fromString
}

func generateWhereString(lattice int) string {

	whereColumn := "tag"

	whereString := "WHERE "

	for i := 1; i <= lattice-2; i++ {
		whereString += "p." + whereColumn
		whereString += strconv.Itoa(i)
		whereString += " = "
		whereString += "q." + whereColumn
		whereString += strconv.Itoa(i)
		whereString += " AND "
	}

	whereString += "p." + whereColumn
	whereString += strconv.Itoa(lattice - 1)
	whereString += " < "
	whereString += "q." + whereColumn
	whereString += strconv.Itoa(lattice - 1)

	alphabetMap := make(map[int]string)
	alphabetMap[0] = "a"
	alphabetMap[1] = "b"
	alphabetMap[2] = "c"
	alphabetMap[3] = "d"
	alphabetMap[4] = "e"
	alphabetMap[5] = "f"
	alphabetMap[6] = "g"
	alphabetMap[7] = "h"
	alphabetMap[8] = "i"
	alphabetMap[9] = "j"
	alphabetMap[10] = "k"
	alphabetMap[11] = "l"
	alphabetMap[12] = "m"
	alphabetMap[13] = "n"
	alphabetMap[14] = "o"
	alphabetMap[15] = "r"
	alphabetMap[16] = "s"
	alphabetMap[17] = "t"
	alphabetMap[18] = "u"
	alphabetMap[19] = "v"
	alphabetMap[20] = "w"
	alphabetMap[21] = "x"
	alphabetMap[22] = "y"
	alphabetMap[23] = "z"

	for i := 1; i < lattice; i++ {
		whereString += " AND "
		whereString += alphabetMap[i-1]
		whereString += "." + whereColumn + " = p." + whereColumn
		whereString += strconv.Itoa(i)
	}

	whereString += " AND "
	whereString += alphabetMap[lattice-1]
	whereString += "." + whereColumn + " = q." + whereColumn
	whereString += strconv.Itoa(lattice - 1)

	comparisonColumn := "track"

	for i := 0; i < lattice-1; i++ {
		whereString += " AND "
		whereString += alphabetMap[i]
		whereString += "." + comparisonColumn + " = "
		whereString += alphabetMap[i+1]
		whereString += "." + comparisonColumn + " "
	}

	return whereString
}

func generateGroupBy(lattice int) string {

	groupByColumn := "tag"

	groupByString := "GROUP BY "

	for i := 1; i < lattice; i++ {
		groupByString += "p." + groupByColumn
		groupByString += strconv.Itoa(i)
		groupByString += ", "
	}

	groupByString += "q." + groupByColumn
	groupByString += strconv.Itoa(lattice - 1)
	groupByString += " "

	return groupByString
}

func getFrequentTags() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Fatal(err)
	}

	lattice := 2

	minSupport := "10000"

	for {

		queryString := "CREATE TABLE L" + strconv.Itoa(lattice) + " AS ("
		queryString += generateSelectString(lattice)
		queryString += generateFromStatement(lattice)
		queryString += generateWhereString(lattice)
		queryString += generateGroupBy(lattice)
		queryString += "HAVING COUNT(*) >= " + minSupport + ")"

		fmt.Println(queryString)

		commandTag, err := conn.Exec(context.Background(), queryString)

		if err != nil {
			log.Error(err)
		}

		if commandTag.RowsAffected() == 0 {
			break
		} else {
			fmt.Println("Lattice: " + strconv.Itoa(lattice) + " has " + strconv.Itoa(int(commandTag.RowsAffected())) + " entries")
		}

		lattice += 1
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}

}

func getItemsFor10000() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT a.tag, b.tag, c.tag, d.tag " +
		"FROM L4 " +
		"INNER JOIN Tag a ON a.id = tag1 " +
		"INNER JOIN Tag b ON b.id = tag2 " +
		"INNER JOIN Tag c ON c.id = tag3 " +
		"INNER JOIN Tag d ON d.id = tag4 "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	defer rows.Close()

	for rows.Next() {

		var tag1 string
		var tag2 string
		var tag3 string
		var tag4 string

		err = rows.Scan(&tag1, &tag2, &tag3, &tag4)
		if err != nil {
			log.Error(err)
		}

		fmt.Println(tag1 + ", " + tag2 + ", " + tag3 + ", " + tag4)

	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func main() {
	start := time.Now()

	//createL1()
	//getFrequentTags()

	getItemsFor10000()
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
