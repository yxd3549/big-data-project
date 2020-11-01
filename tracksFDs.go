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

type track struct {
	Id                  int
	Title               sql.NullString
	Uri                 sql.NullString
	Isrc                sql.NullString
	Genre               sql.NullInt32
	Kind                sql.NullInt32
	License             sql.NullInt32
	LikesCount          sql.NullInt32
	Commentable         sql.NullBool
	CommentCount        sql.NullInt32
	Downloadable        sql.NullBool
	DownloadCount       sql.NullInt32
	CreatedAt           sql.NullString
	Description         sql.NullString
	Duration            sql.NullInt32
	LabelName           sql.NullString
	LastModified        sql.NullString
	OriginalContentSize sql.NullInt32
	OriginalFormat      sql.NullString
	Permalink           sql.NullString
	PermalinkUrl        sql.NullString
	PlaybackCount       sql.NullInt32
	RetrievedUtc        sql.NullInt32
	StreamUrl           sql.NullString
	Streamable          sql.NullBool
	TrackType           sql.NullString
	WaveformUrl         sql.NullString
}

type genericTrackMaps struct {
	Id                  map[string]int
	Title               map[string]sql.NullString
	Uri                 map[string]sql.NullString
	Isrc                map[string]sql.NullString
	Genre               map[string]sql.NullInt32
	Kind                map[string]sql.NullInt32
	License             map[string]sql.NullInt32
	LikesCount          map[string]sql.NullInt32
	Commentable         map[string]sql.NullBool
	CommentCount        map[string]sql.NullInt32
	Downloadable        map[string]sql.NullBool
	DownloadCount       map[string]sql.NullInt32
	CreatedAt           map[string]sql.NullString
	Description         map[string]sql.NullString
	Duration            map[string]sql.NullInt32
	LableName           map[string]sql.NullString
	LastModified        map[string]sql.NullString
	OriginalContentSize map[string]sql.NullInt32
	OriginalFormat      map[string]sql.NullString
	Permalink           map[string]sql.NullString
	PermalinkUrl        map[string]sql.NullString
	PlaybackCount       map[string]sql.NullInt32
	RetrievedUtc        map[string]sql.NullInt32
	StreamUrl           map[string]sql.NullString
	Streamable          map[string]sql.NullBool
	TrackType           map[string]sql.NullString
	WaveformUrl         map[string]sql.NullString
}

func readInTrackData() []track {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/soundcloud")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT id, title, uri, isrc, genre, kind, license, likes_count, commentable, comment_count, " +
		"downloadable, download_count, created_at, description, duration, label_name, last_modified, " +
		"original_content_size, original_format, permalink, permalink_url, playback_count, retrieved_utc, " +
		"stream_url , streamable bool, track_type, waveform_url " +
		"FROM track"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	var data []track

	defer rows.Close()

	defer rows.Close()

	for rows.Next() {
		var t = track{}
		err = rows.Scan(&t.Id, &t.Title, &t.Uri, &t.Isrc, &t.Genre, &t.Kind, &t.License, &t.LikesCount, &t.Commentable,
			&t.CommentCount, &t.Downloadable, &t.DownloadCount, &t.CreatedAt, &t.Description, &t.Duration, &t.LabelName,
			&t.LastModified, &t.OriginalContentSize, &t.OriginalFormat, &t.Permalink, &t.PermalinkUrl, &t.PlaybackCount,
			&t.RetrievedUtc, &t.StreamUrl, &t.Streamable, &t.TrackType, &t.WaveformUrl)

		if err != nil {
			log.Error(err)
		}

		data = append(data, t)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func getValueByColumnNumTrack(loc int, elem track) string {
	switch loc {
	case 0:
		return strconv.Itoa(elem.Id)
	case 1:
		if elem.Title.Valid {
			return elem.Title.String
		}
		return ""
	case 2:
		if elem.Uri.Valid {
			return elem.Uri.String
		}
		return ""
	case 3:
		if elem.Isrc.Valid {
			return elem.Isrc.String
		}
		return ""
	case 4:
		if elem.Genre.Valid {
			return strconv.Itoa(int(elem.Genre.Int32))
		}
		return ""
	case 5:
		if elem.Kind.Valid {
			return strconv.Itoa(int(elem.Kind.Int32))
		}
		return ""
	case 6:
		if elem.License.Valid {
			return strconv.Itoa(int(elem.License.Int32))
		}
		return ""
	case 7:
		if elem.LikesCount.Valid {
			return strconv.Itoa(int(elem.LikesCount.Int32))
		}
		return ""
	case 8:
		if elem.Commentable.Valid {
			return strconv.FormatBool(elem.Commentable.Bool)
		}
		return ""
	case 9:
		if elem.CommentCount.Valid {
			return strconv.Itoa(int(elem.CommentCount.Int32))
		}
		return ""
	case 10:
		if elem.Downloadable.Valid {
			return strconv.FormatBool(elem.Downloadable.Bool)
		}
		return ""
	case 11:
		if elem.DownloadCount.Valid {
			return strconv.Itoa(int(elem.DownloadCount.Int32))
		}
		return ""
	case 12:
		if elem.CreatedAt.Valid {
			return elem.CreatedAt.String
		}
		return ""
	case 13:
		if elem.Description.Valid {
			return elem.Description.String
		}
		return ""
	case 14:
		if elem.Duration.Valid {
			return strconv.Itoa(int(elem.Duration.Int32))
		}
		return ""
	case 15:
		if elem.LabelName.Valid {
			return elem.LabelName.String
		}
		return ""
	case 16:
		if elem.LastModified.Valid {
			return elem.LastModified.String
		}
		return ""
	case 17:
		if elem.OriginalContentSize.Valid {
			return strconv.Itoa(int(elem.OriginalContentSize.Int32))
		}
		return ""
	case 18:
		if elem.OriginalFormat.Valid {
			return elem.OriginalFormat.String
		}
		return ""
	case 19:
		if elem.Permalink.Valid {
			return elem.Permalink.String
		}
		return ""
	case 20:
		if elem.PermalinkUrl.Valid {
			return elem.PermalinkUrl.String
		}
		return ""
	case 21:
		if elem.PlaybackCount.Valid {
			return strconv.Itoa(int(elem.PlaybackCount.Int32))
		}
		return ""
	case 22:
		if elem.RetrievedUtc.Valid {
			return strconv.Itoa(int(elem.RetrievedUtc.Int32))
		}
		return ""
	case 23:
		if elem.StreamUrl.Valid {
			return elem.StreamUrl.String
		}
		return ""
	case 24:
		if elem.Streamable.Valid {
			return strconv.FormatBool(elem.Streamable.Bool)
		}
		return ""
	case 25:
		if elem.TrackType.Valid {
			return elem.TrackType.String
		}
		return ""
	case 26:
		if elem.WaveformUrl.Valid {
			return elem.WaveformUrl.String
		}
		return ""
	}
	return ""
}

func checkGroupsOfOneTrack(data []track) [27][27]bool {

	var relArr [27][27]bool

	// Iterate through each column in db
	for group := 0; group < 27; group++ {

		// All default to being valid functional dependencies. Change to false once we discover they are not
		// Reset after each group
		isValid := []bool{true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
			true, true, true, true, true, true, true, true, true, true, true}

		// Need to create new maps for each group too or else we'll get collisions
		maps := genericTrackMaps{
			Id:                  make(map[string]int),
			Title:               make(map[string]sql.NullString),
			Uri:                 make(map[string]sql.NullString),
			Isrc:                make(map[string]sql.NullString),
			Genre:               make(map[string]sql.NullInt32),
			Kind:                make(map[string]sql.NullInt32),
			License:             make(map[string]sql.NullInt32),
			LikesCount:          make(map[string]sql.NullInt32),
			Commentable:         make(map[string]sql.NullBool),
			CommentCount:        make(map[string]sql.NullInt32),
			Downloadable:        make(map[string]sql.NullBool),
			DownloadCount:       make(map[string]sql.NullInt32),
			CreatedAt:           make(map[string]sql.NullString),
			Description:         make(map[string]sql.NullString),
			Duration:            make(map[string]sql.NullInt32),
			LableName:           make(map[string]sql.NullString),
			LastModified:        make(map[string]sql.NullString),
			OriginalContentSize: make(map[string]sql.NullInt32),
			OriginalFormat:      make(map[string]sql.NullString),
			Permalink:           make(map[string]sql.NullString),
			PermalinkUrl:        make(map[string]sql.NullString),
			PlaybackCount:       make(map[string]sql.NullInt32),
			RetrievedUtc:        make(map[string]sql.NullInt32),
			StreamUrl:           make(map[string]sql.NullString),
			Streamable:          make(map[string]sql.NullBool),
			TrackType:           make(map[string]sql.NullString),
			WaveformUrl:         make(map[string]sql.NullString),
		}

		// Iterate through each row in db
		for _, elem := range data {

			key := getValueByColumnNumTrack(group, elem)

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

			// title
			title, ok := maps.Title[key]

			if !ok {
				maps.Title[key] = elem.Title
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Title.String != title.String {

					isValid[1] = false
				}
			}

			// uri
			uri, ok := maps.Uri[key]

			if !ok {
				maps.Uri[key] = elem.Uri
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Uri.String != uri.String {

					isValid[2] = false
				}
			}

			// isrc
			isrc, ok := maps.Isrc[key]

			if !ok {
				maps.Isrc[key] = elem.Isrc
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Isrc.String != isrc.String {

					isValid[3] = false
				}
			}

			// genre
			genre, ok := maps.Genre[key]

			if !ok {
				maps.Genre[key] = elem.Genre
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Genre.Int32 != genre.Int32 {

					isValid[4] = false
				}
			}

			// kind
			kind, ok := maps.Kind[key]

			if !ok {
				maps.Kind[key] = elem.Kind
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Kind.Int32 != kind.Int32 {

					isValid[5] = false
				}
			}

			// license
			license, ok := maps.License[key]

			if !ok {
				maps.License[key] = elem.License
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.License.Int32 != license.Int32 {

					isValid[6] = false
				}
			}

			// likesCount
			likesCount, ok := maps.LikesCount[key]

			if !ok {
				maps.LikesCount[key] = elem.LikesCount
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.LikesCount.Int32 != likesCount.Int32 {

					isValid[7] = false
				}
			}

			// commentable
			commentable, ok := maps.Commentable[key]

			if !ok {
				maps.Commentable[key] = elem.Commentable
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Commentable.Bool != commentable.Bool {

					isValid[8] = false
				}
			}

			// commentCount
			commentCount, ok := maps.CommentCount[key]

			if !ok {
				maps.CommentCount[key] = elem.CommentCount
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.CommentCount.Int32 != commentCount.Int32 {

					isValid[9] = false
				}
			}

			// downloadable
			downloadable, ok := maps.Downloadable[key]

			if !ok {
				maps.Downloadable[key] = elem.Downloadable
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Downloadable.Bool != downloadable.Bool {

					isValid[10] = false
				}
			}

			// downloadCount
			downloadCount, ok := maps.DownloadCount[key]

			if !ok {
				maps.DownloadCount[key] = elem.DownloadCount
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.DownloadCount.Int32 != downloadCount.Int32 {

					isValid[11] = false
				}
			}

			// createdAt
			createdAt, ok := maps.CreatedAt[key]

			if !ok {
				maps.CreatedAt[key] = elem.CreatedAt
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.CreatedAt.String != createdAt.String {

					isValid[12] = false
				}
			}

			// description
			description, ok := maps.Description[key]

			if !ok {
				maps.Description[key] = elem.Description
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Description.String != description.String {

					isValid[13] = false
				}
			}

			// genre
			duration, ok := maps.Duration[key]

			if !ok {
				maps.Duration[key] = elem.Duration
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Duration.Int32 != duration.Int32 {

					isValid[14] = false
				}
			}

			// labelName
			labelName, ok := maps.LableName[key]

			if !ok {
				maps.LableName[key] = elem.LabelName
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.LabelName.String != labelName.String {

					isValid[15] = false
				}
			}

			// lastModified
			lastModified, ok := maps.LastModified[key]

			if !ok {
				maps.LastModified[key] = elem.LastModified
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.LastModified.String != lastModified.String {

					isValid[16] = false
				}
			}

			// originalContentSize
			originalContentSize, ok := maps.OriginalContentSize[key]

			if !ok {
				maps.OriginalContentSize[key] = elem.OriginalContentSize
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.OriginalContentSize.Int32 != originalContentSize.Int32 {

					isValid[17] = false
				}
			}

			// originalFormat
			originalFormat, ok := maps.OriginalFormat[key]

			if !ok {
				maps.OriginalFormat[key] = elem.OriginalFormat
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.OriginalFormat.String != originalFormat.String {

					isValid[18] = false
				}
			}

			// permalink
			permalink, ok := maps.Permalink[key]

			if !ok {
				maps.Permalink[key] = elem.Permalink
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Permalink.String != permalink.String {

					isValid[19] = false
				}
			}

			// permalink
			permalinkUrl, ok := maps.PermalinkUrl[key]

			if !ok {
				maps.PermalinkUrl[key] = elem.PermalinkUrl
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.PermalinkUrl.String != permalinkUrl.String {

					isValid[20] = false
				}
			}

			// playbackCount
			playbackCount, ok := maps.PlaybackCount[key]

			if !ok {
				maps.PlaybackCount[key] = elem.PlaybackCount
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.PlaybackCount.Int32 != playbackCount.Int32 {

					isValid[21] = false
				}
			}

			// retrievedUTC
			RetrievedUtc, ok := maps.RetrievedUtc[key]

			if !ok {
				maps.RetrievedUtc[key] = elem.RetrievedUtc
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.RetrievedUtc.Int32 != RetrievedUtc.Int32 {

					isValid[22] = false
				}
			}

			// streamURL
			streamURL, ok := maps.StreamUrl[key]

			if !ok {
				maps.StreamUrl[key] = elem.StreamUrl
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.StreamUrl.String != streamURL.String {

					isValid[23] = false
				}
			}

			// streamable
			streamable, ok := maps.Streamable[key]

			if !ok {
				maps.Streamable[key] = elem.Streamable
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Streamable.Bool != streamable.Bool {

					isValid[24] = false
				}
			}

			// trackType
			trackType, ok := maps.TrackType[key]

			if !ok {
				maps.TrackType[key] = elem.TrackType
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.TrackType.String != trackType.String {

					isValid[25] = false
				}
			}

			// waveformURL
			waveformURL, ok := maps.WaveformUrl[key]

			if !ok {
				maps.WaveformUrl[key] = elem.WaveformUrl
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.WaveformUrl.String != waveformURL.String {

					isValid[26] = false
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

//func checkGroupOfTwo(data []user, relArr [6][6]bool) {
//
//	// Iterate through each column in db
//	for groupOne := 0; groupOne < 6; groupOne++ {
//
//		for groupTwo := 0; groupTwo < 6; groupTwo++ {
//
//			if groupOne != groupTwo {
//
//				// All default to being valid functional dependencies. Change to false once we discover they are not
//				// Reset after each group
//				isValid := []bool{true, true, true, true, true, true}
//
//				// Need to create new maps for each group too or else we'll get collisions
//				maps := genericMaps{
//					Id:           make(map[string]int),
//					Username:     make(map[string]sql.NullString),
//					Kind:         make(map[string]sql.NullString),
//					LastModified: make(map[string]sql.NullString),
//					Permalink:    make(map[string]sql.NullString),
//					Uri:          make(map[string]sql.NullString),
//				}
//
//				// Iterate through each row in db
//				for _, elem := range data {
//
//					key := getValueByColumnNum(groupOne, elem)
//					key += getValueByColumnNum(groupTwo, elem)
//
//					// id
//					id, ok := maps.Id[key]
//
//					if !ok {
//						maps.Id[key] = elem.Id
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Id != id {
//
//							isValid[0] = false
//						}
//					}
//
//					// username
//					username, ok := maps.Username[key]
//
//					if !ok {
//						maps.Username[key] = elem.Username
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Username.String != username.String {
//
//							isValid[1] = false
//						}
//					}
//
//					// kind
//					kind, ok := maps.Kind[key]
//
//					if !ok {
//						maps.Kind[key] = elem.Kind
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Kind.String != kind.String {
//
//							isValid[2] = false
//						}
//					}
//
//					// lastModified
//					lastModified, ok := maps.LastModified[key]
//
//					if !ok {
//						maps.LastModified[key] = elem.LastModified
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.LastModified.String != lastModified.String {
//
//							isValid[3] = false
//						}
//					}
//
//					// permalink
//					permalink, ok := maps.Permalink[key]
//
//					if !ok {
//						maps.Permalink[key] = elem.Permalink
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Permalink.String != permalink.String {
//
//							isValid[4] = false
//						}
//					}
//
//					// uri
//					uri, ok := maps.Uri[key]
//
//					if !ok {
//						maps.Uri[key] = elem.Uri
//					} else {
//						// Since they differ, this is not a valid functional dependency
//						if elem.Uri.String != uri.String {
//
//							isValid[5] = false
//						}
//					}
//				}
//
//				header := ""
//
//				switch groupOne {
//				case 0:
//					header += "id,"
//				case 1:
//					header += "username,"
//				case 2:
//					header += "kind,"
//				case 3:
//					header += "last_modified,"
//				case 4:
//					header += "permalink,"
//				case 5:
//					header += "uri,"
//				}
//
//				switch groupTwo {
//				case 0:
//					header += "id->"
//				case 1:
//					header += "username->"
//				case 2:
//					header += "kind->"
//				case 3:
//					header += "last_modified->"
//				case 4:
//					header += "permalink->"
//				case 5:
//					header += "uri->"
//				}
//
//				for idx, valid := range isValid {
//					if valid && idx != groupOne && idx != groupTwo &&
//						!(relArr[groupOne][idx] || relArr[groupTwo][idx]) {
//						switch idx {
//						case 0:
//							println(header + "id")
//						case 1:
//							println(header + "username")
//						case 2:
//							println(header + "kind")
//						case 3:
//							println(header + "last_modified")
//						case 4:
//							println(header + "permalink")
//						case 5:
//							println(header + "uri")
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

	data := readInTrackData()

	fmt.Println(len(data))

	// Can't use go routines because I actually need checkGroupsOfOne to finish before checkGroupsOfTwo
	// I'm going to keep a map of what functional dependencies I got from one and then not print them if that's what
	// I get from two
	_ = checkGroupsOfOneTrack(data)
	//checkGroupOfTwo(data, relArr)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
