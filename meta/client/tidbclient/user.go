package tidbclient

import (
	"database/sql"
)

func (t *TidbClient) GetUserBuckets(userId string) (buckets []string, err error) {
	sqltext := "select bucketname from users where userid=?;"
	rows, err := t.Client.Query(sqltext, userId)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tmp string
		err = rows.Scan(&tmp)
		if err != nil {
			return
		}
		buckets = append(buckets, tmp)
	}
	// must check the errors after rows.Next() function call.
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return
}

func (t *TidbClient) AddBucketForUser(bucketName, userId string) (err error) {
	sql := "insert into users(userid,bucketname) values(?,?)"
	_, err = t.Client.Exec(sql, userId, bucketName)
	return
}

func (t *TidbClient) RemoveBucketForUser(bucketName string, userId string) (err error) {
	sql := "delete from users where userid=? and bucketname=?;"
	_, err = t.Client.Exec(sql, userId, bucketName)
	return
}
