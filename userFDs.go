package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
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
	TitleID   map[string]int
	TitleType map[string]sql.NullString
	StartYear map[string]sql.NullInt32
	Runtime   map[string]int
	AvgRating map[string]decimal.NullDecimal
	GenreId   map[string]int
	Genre     map[string]sql.NullString
	MemberId  map[string]int
	BirthYear map[string]sql.NullInt32
	Role      map[string]string
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

//func getValueByColumnNum(loc int, elem movieTitleActor) string {
//	switch loc {
//	case 0:
//		return strconv.Itoa(elem.TitleID)
//	case 1:
//		return elem.TitleType.String
//	case 2:
//		return strconv.Itoa(int(elem.StartYear.Int32))
//	case 3:
//		return strconv.Itoa(elem.Runtime)
//	case 4:
//		return elem.AvgRating.Decimal.String()
//	case 5:
//		return strconv.Itoa(elem.GenreId)
//	case 6:
//		return elem.Genre.String
//	case 7:
//		return strconv.Itoa(elem.MemberId)
//	case 8:
//		return strconv.Itoa(int(elem.BirthYear.Int32))
//	case 9:
//		return elem.Role
//	}
//	return ""
//}
//
//func checkGroupsOfOne(data []movieTitleActor) [10][10]bool {
//
//	var relArr [10][10]bool
//
//	// Iterate through each column in db
//	for group := 0; group < 10; group++ {
//
//		// All default to being valid functional dependencies. Change to false once we discover they are not
//		// Reset after each group
//		isValid := []bool{true, true, true, true, true, true, true, true, true, true}
//
//		// Need to create new maps for each group too or else we'll get collisions
//		maps := genericMaps{
//			TitleID:   make(map[string]int),
//			TitleType: make(map[string]sql.NullString),
//			StartYear: make(map[string]sql.NullInt32),
//			Runtime:   make(map[string]int),
//			AvgRating: make(map[string]decimal.NullDecimal),
//			GenreId:   make(map[string]int),
//			Genre:     make(map[string]sql.NullString),
//			MemberId:  make(map[string]int),
//			BirthYear: make(map[string]sql.NullInt32),
//			Role:      make(map[string]string),
//		}
//
//		// Iterate through each row in db
//		for _, elem := range data {
//
//			key := getValueByColumnNum(group, elem)
//
//			// titleID
//			titleID, ok := maps.TitleID[key]
//
//			if !ok {
//				maps.TitleID[key] = elem.TitleID
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.TitleID != titleID {
//
//					isValid[0] = false
//				}
//			}
//
//			// type
//			titleType, ok := maps.TitleType[key]
//
//			if !ok {
//				maps.TitleType[key] = elem.TitleType
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.TitleType.String != titleType.String {
//
//					isValid[1] = false
//				}
//			}
//
//			// startYear
//			startYear, ok := maps.StartYear[key]
//
//			if !ok {
//				maps.StartYear[key] = elem.StartYear
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.StartYear.Int32 != startYear.Int32 {
//					isValid[2] = false
//				}
//			}
//
//			// runtimeMinutes
//			runtimeMinutes, ok := maps.Runtime[key]
//
//			if !ok {
//				maps.Runtime[key] = elem.Runtime
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.Runtime != runtimeMinutes {
//					isValid[3] = false
//				}
//			}
//
//			// avgRating
//			avgRating, ok := maps.AvgRating[key]
//
//			if !ok {
//				maps.AvgRating[key] = elem.AvgRating
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
//					isValid[4] = false
//				}
//			}
//
//			// genre_id
//			genreID, ok := maps.GenreId[key]
//
//			if !ok {
//				maps.GenreId[key] = elem.GenreId
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.GenreId != genreID {
//					isValid[5] = false
//				}
//			}
//
//			// genre
//			genre, ok := maps.Genre[key]
//
//			if !ok {
//				maps.Genre[key] = elem.Genre
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.Genre.String != genre.String {
//					isValid[6] = false
//				}
//			}
//
//			// member_id
//			memberID, ok := maps.MemberId[key]
//
//			if !ok {
//				maps.MemberId[key] = elem.MemberId
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.MemberId != memberID {
//					isValid[7] = false
//				}
//			}
//
//			// birthYear
//			birthYear, ok := maps.BirthYear[key]
//
//			if !ok {
//				maps.BirthYear[key] = elem.BirthYear
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.BirthYear.Int32 != birthYear.Int32 {
//					isValid[8] = false
//				}
//			}
//
//			// role
//			role, ok := maps.Role[key]
//
//			if !ok {
//				maps.Role[key] = elem.Role
//			} else {
//				// Since they differ, this is not a valid functional dependency
//				if elem.Role != role {
//					isValid[9] = false
//				}
//			}
//		}
//
//		header := ""
//
//		switch group {
//		case 0:
//			header += "movieID->"
//		case 1:
//			header += "type->"
//		case 2:
//			header += "startYear->"
//		case 3:
//			header += "runtimeMinutes->"
//		case 4:
//			header += "avgRating->"
//		case 5:
//			header += "genre_id->"
//		case 6:
//			header += "genre->"
//		case 7:
//			header += "member_id->"
//		case 8:
//			header += "birthYear->"
//		case 9:
//			header += "role->"
//		}
//
//		for idx, valid := range isValid {
//			if valid && idx != group {
//				relArr[group][idx] = true
//
//				switch idx {
//				case 0:
//					println(header + "movieID")
//				case 1:
//					println(header + "type")
//				case 2:
//					println(header + "startYear")
//				case 3:
//					println(header + "runtimeMinutes")
//				case 4:
//					println(header + "avgRating")
//				case 5:
//					println(header + "genre_id")
//				case 6:
//					println(header + "genre")
//				case 7:
//					println(header + "member_id")
//				case 8:
//					println(header + "birthYear")
//				case 9:
//					println(header + "role")
//				}
//			}
//		}
//	}
//
//	return relArr
//}
//
//func checkGroupOfTwo(data []movieTitleActor, relArr [10][10]bool) {
//
//	// Iterate through each column in db
//	for groupOne := 0; groupOne < 10; groupOne++ {
//
//		for groupTwo := 0; groupTwo < 10; groupTwo++ {
//
//			if groupOne != groupTwo {
//
//				// All default to being valid functional dependencies. Change to false once we discover they are not
//				// Reset after each group
//				isValid := []bool{true, true, true, true, true, true, true, true, true, true}
//
//				// Need to create new maps for each group too or else we'll get collisions
//				maps := genericMaps{
//					TitleID:   make(map[string]int),
//					TitleType: make(map[string]sql.NullString),
//					StartYear: make(map[string]sql.NullInt32),
//					Runtime:   make(map[string]int),
//					AvgRating: make(map[string]decimal.NullDecimal),
//					GenreId:   make(map[string]int),
//					Genre:     make(map[string]sql.NullString),
//					MemberId:  make(map[string]int),
//					BirthYear: make(map[string]sql.NullInt32),
//					Role:      make(map[string]string),
//				}
//
//				// Iterate through each row in db
//				for _, elem := range data {
//
//					key := getValueByColumnNum(groupOne, elem)
//					key += getValueByColumnNum(groupTwo, elem)
//
//					// titleID
//					titleID, ok := maps.TitleID[key]
//
//					if !ok {
//						maps.TitleID[key] = elem.TitleID
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.TitleID != titleID {
//
//							isValid[0] = false
//						}
//					}
//
//					// type
//					titleType, ok := maps.TitleType[key]
//
//					if !ok {
//						maps.TitleType[key] = elem.TitleType
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.TitleType.String != titleType.String {
//
//							isValid[1] = false
//						}
//					}
//
//					// startYear
//					startYear, ok := maps.StartYear[key]
//
//					if !ok {
//						maps.StartYear[key] = elem.StartYear
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.StartYear.Int32 != startYear.Int32 {
//							isValid[2] = false
//						}
//					}
//
//					// runtimeMinutes
//					runtimeMinutes, ok := maps.Runtime[key]
//
//					if !ok {
//						maps.Runtime[key] = elem.Runtime
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Runtime != runtimeMinutes {
//							isValid[3] = false
//						}
//					}
//
//					// avgRating
//					avgRating, ok := maps.AvgRating[key]
//
//					if !ok {
//						maps.AvgRating[key] = elem.AvgRating
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
//							isValid[4] = false
//						}
//					}
//
//					// genre_id
//					genreID, ok := maps.GenreId[key]
//
//					if !ok {
//						maps.GenreId[key] = elem.GenreId
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.GenreId != genreID {
//							isValid[5] = false
//						}
//					}
//
//					// genre
//					genre, ok := maps.Genre[key]
//
//					if !ok {
//						maps.Genre[key] = elem.Genre
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Genre.String != genre.String {
//							isValid[6] = false
//						}
//					}
//
//					// member_id
//					memberID, ok := maps.MemberId[key]
//
//					if !ok {
//						maps.MemberId[key] = elem.MemberId
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.MemberId != memberID {
//							isValid[7] = false
//						}
//					}
//
//					// birthYear
//					birthYear, ok := maps.BirthYear[key]
//
//					if !ok {
//						maps.BirthYear[key] = elem.BirthYear
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.BirthYear.Int32 != birthYear.Int32 {
//							isValid[8] = false
//						}
//					}
//
//					// role
//					role, ok := maps.Role[key]
//
//					if !ok {
//						maps.Role[key] = elem.Role
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Role != role {
//							isValid[9] = false
//						}
//					}
//				}
//
//				header := ""
//
//				switch groupOne {
//				case 0:
//					header += "movieID,"
//				case 1:
//					header += "type,"
//				case 2:
//					header += "startYear,"
//				case 3:
//					header += "runtimeMinutes,"
//				case 4:
//					header += "avgRating,"
//				case 5:
//					header += "genre_id,"
//				case 6:
//					header += "genre,"
//				case 7:
//					header += "member_id,"
//				case 8:
//					header += "birthYear,"
//				case 9:
//					header += "role,"
//				}
//
//				switch groupTwo {
//				case 0:
//					header += "movieID->"
//				case 1:
//					header += "type->"
//				case 2:
//					header += "startYear->"
//				case 3:
//					header += "runtimeMinutes->"
//				case 4:
//					header += "avgRating->"
//				case 5:
//					header += "genre_id->"
//				case 6:
//					header += "genre->"
//				case 7:
//					header += "member_id->"
//				case 8:
//					header += "birthYear->"
//				case 9:
//					header += "role->"
//				}
//
//				for idx, valid := range isValid {
//					if valid && idx != groupOne && idx != groupTwo &&
//						!(relArr[groupOne][idx] || relArr[groupTwo][idx]) {
//						switch idx {
//						case 0:
//							println(header + "movieID")
//						case 1:
//							println(header + "type")
//						case 2:
//							println(header + "startYear")
//						case 3:
//							println(header + "runtimeMinutes")
//						case 4:
//							println(header + "avgRating")
//						case 5:
//							println(header + "genre_id")
//						case 6:
//							println(header + "genre")
//						case 7:
//							println(header + "member_id")
//						case 8:
//							println(header + "birthYear")
//						case 9:
//							println(header + "role")
//						}
//					}
//				}
//			}
//		}
//	}
//}

// run with go build userFDs.go
func main() {
	start := time.Now()

	data := readInData()

	fmt.Println(len(data))

	//// Can't use go routines because I actually need checkGroupsOfOne to finish before checkGroupsOfTwo
	//// I'm going to keep a map of what functional dependencies I got from one and then not print them if that's what
	//// I get from two
	//relArr := checkGroupsOfOne(data)
	//checkGroupOfTwo(data, relArr)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
