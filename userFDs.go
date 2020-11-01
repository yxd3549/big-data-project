package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

type user struct {
	Id           int
	Username     sql.NullString
	Kind         sql.NullString
	LastModified sql.NullString
	Permalink    sql.NullString
	Uri          sql.NullString
}

type genericMaps struct {
	Id           map[string]int
	Username     map[string]sql.NullString
	Kind         map[string]sql.NullString
	LastModified map[string]sql.NullString
	Permalink    map[string]sql.NullString
	Uri          map[string]sql.NullString
}

func readInData() []user {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT id, username, kind, last_modified, permalink, uri " +
		"FROM \"user\""

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	var data []user

	defer rows.Close()

	defer rows.Close()

	for rows.Next() {
		var u = user{}
		err = rows.Scan(&u.Id, &u.Username, &u.Kind, &u.LastModified, &u.Permalink, &u.Uri)

		if err != nil {
			log.Error(err)
		}

		data = append(data, u)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func getValueByColumnNum(loc int, elem user) string {
	switch loc {
	case 0:
		return strconv.Itoa(elem.Id)
	case 1:
		if elem.Username.Valid {
			return elem.Username.String
		}
		return ""
	case 2:
		if elem.Kind.Valid {
			return elem.Kind.String
		}
		return ""
	case 3:
		if elem.LastModified.Valid {
			return elem.LastModified.String
		}
		return ""
	case 4:
		if elem.Permalink.Valid {
			return elem.Permalink.String
		}
		return ""
	case 5:
		if elem.Uri.Valid {
			return elem.Uri.String
		}
		return ""
	}
	return ""
}

func checkGroupsOfOne(data []user) [6][6]bool {

	var relArr [6][6]bool

	// Iterate through each column in db
	for group := 0; group < 6; group++ {

		// All default to being valid functional dependencies. Change to false once we discover they are not
		// Reset after each group
		isValid := []bool{true, true, true, true, true, true}

		// Need to create new maps for each group too or else we'll get collisions
		maps := genericMaps{
			Id:           make(map[string]int),
			Username:     make(map[string]sql.NullString),
			Kind:         make(map[string]sql.NullString),
			LastModified: make(map[string]sql.NullString),
			Permalink:    make(map[string]sql.NullString),
			Uri:          make(map[string]sql.NullString),
		}

		// Iterate through each row in db
		for _, elem := range data {

			key := getValueByColumnNum(group, elem)

			// id
			id, ok := maps.Id[key]

			if !ok {
				maps.Id[key] = elem.Id
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Id != id {

					isValid[0] = false
				}
			}

			// username
			username, ok := maps.Username[key]

			if !ok {
				maps.Username[key] = elem.Username
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Username.String != username.String {

					isValid[1] = false
				}
			}

			// kind
			kind, ok := maps.Kind[key]

			if !ok {
				maps.Kind[key] = elem.Kind
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Kind.String != kind.String {

					isValid[2] = false
				}
			}

			// lastModified
			lastModified, ok := maps.LastModified[key]

			if !ok {
				maps.LastModified[key] = elem.LastModified
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.LastModified.String != lastModified.String {

					isValid[3] = false
				}
			}

			// permalink
			permalink, ok := maps.Permalink[key]

			if !ok {
				maps.Permalink[key] = elem.Permalink
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Permalink.String != permalink.String {

					isValid[4] = false
				}
			}

			// uri
			uri, ok := maps.Uri[key]

			if !ok {
				maps.Uri[key] = elem.Uri
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Uri.String != uri.String {

					isValid[5] = false
				}
			}
		}

		header := ""

		switch group {
		case 0:
			header += "id->"
		case 1:
			header += "username->"
		case 2:
			header += "kind->"
		case 3:
			header += "last_modified->"
		case 4:
			header += "permalink->"
		case 5:
			header += "uri->"
		}

		for idx, valid := range isValid {
			if valid && idx != group {
				relArr[group][idx] = true

				switch idx {
				case 0:
					println(header + "id")
				case 1:
					println(header + "username")
				case 2:
					println(header + "kind")
				case 3:
					println(header + "last_modified")
				case 4:
					println(header + "permalink")
				case 5:
					println(header + "uri")
				}
			}
		}
	}

	return relArr
}

func checkGroupOfTwo(data []user, relArr [6][6]bool) {

	// Iterate through each column in db
	for groupOne := 0; groupOne < 6; groupOne++ {

		for groupTwo := 0; groupTwo < 6; groupTwo++ {

			if groupOne != groupTwo {

				// All default to being valid functional dependencies. Change to false once we discover they are not
				// Reset after each group
				isValid := []bool{true, true, true, true, true, true}

				// Need to create new maps for each group too or else we'll get collisions
				maps := genericMaps{
					Id:           make(map[string]int),
					Username:     make(map[string]sql.NullString),
					Kind:         make(map[string]sql.NullString),
					LastModified: make(map[string]sql.NullString),
					Permalink:    make(map[string]sql.NullString),
					Uri:          make(map[string]sql.NullString),
				}

				// Iterate through each row in db
				for _, elem := range data {

					key := getValueByColumnNum(groupOne, elem)
					key += getValueByColumnNum(groupTwo, elem)

					// id
					id, ok := maps.Id[key]

					if !ok {
						maps.Id[key] = elem.Id
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Id != id {

							isValid[0] = false
						}
					}

					// username
					username, ok := maps.Username[key]

					if !ok {
						maps.Username[key] = elem.Username
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Username.String != username.String {

							isValid[1] = false
						}
					}

					// kind
					kind, ok := maps.Kind[key]

					if !ok {
						maps.Kind[key] = elem.Kind
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Kind.String != kind.String {

							isValid[2] = false
						}
					}

					// lastModified
					lastModified, ok := maps.LastModified[key]

					if !ok {
						maps.LastModified[key] = elem.LastModified
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.LastModified.String != lastModified.String {

							isValid[3] = false
						}
					}

					// permalink
					permalink, ok := maps.Permalink[key]

					if !ok {
						maps.Permalink[key] = elem.Permalink
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Permalink.String != permalink.String {

							isValid[4] = false
						}
					}

					// uri
					uri, ok := maps.Uri[key]

					if !ok {
						maps.Uri[key] = elem.Uri
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Uri.String != uri.String {

							isValid[5] = false
						}
					}
				}

				header := ""

				switch groupOne {
				case 0:
					header += "id,"
				case 1:
					header += "username,"
				case 2:
					header += "kind,"
				case 3:
					header += "last_modified,"
				case 4:
					header += "permalink,"
				case 5:
					header += "uri,"
				}

				switch groupTwo {
				case 0:
					header += "id->"
				case 1:
					header += "username->"
				case 2:
					header += "kind->"
				case 3:
					header += "last_modified->"
				case 4:
					header += "permalink->"
				case 5:
					header += "uri->"
				}

				for idx, valid := range isValid {
					if valid && idx != groupOne && idx != groupTwo &&
						!(relArr[groupOne][idx] || relArr[groupTwo][idx]) {
						switch idx {
						case 0:
							println(header + "id")
						case 1:
							println(header + "username")
						case 2:
							println(header + "kind")
						case 3:
							println(header + "last_modified")
						case 4:
							println(header + "permalink")
						case 5:
							println(header + "uri")
						}
					}
				}
			}
		}
	}
}

// run with go build userFDs.go
func main() {
	start := time.Now()

	data := readInData()

	// Can't use go routines because I actually need checkGroupsOfOne to finish before checkGroupsOfTwo
	// I'm going to keep a map of what functional dependencies I got from one and then not print them if that's what
	// I get from two
	relArr := checkGroupsOfOne(data)
	checkGroupOfTwo(data, relArr)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
