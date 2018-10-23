package kstordb

import (
	"bytes"
	"errors"
	"log"

	"github.com/boltdb/bolt"
)

const (
	Databasename = "my.db"
	Defaultpath  = "../kstor_db/" + Databasename
)

var db *bolt.DB
var backuppath string
var backuped bool = false

func init() {
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func CreateBucket(bucketname string) error {
	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Use the transaction...
	_, err = tx.CreateBucket([]byte(bucketname))
	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func DeleteBucket(bucketname string) error {
	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Use the transaction...
	err = tx.DeleteBucket([]byte(bucketname))
	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func SetKeyValue(key string, value string, name string) error {

	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(name))
	if b == nil {
		return errors.New("the bucket does not exist")
	}

	err = b.Put([]byte(key), []byte(value))
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func GetKeyValue(key string, name string) (string, error) {

	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(name))
	if b == nil {
		return "", errors.New("the bucket does not exist")
	}

	v := b.Get([]byte(key))
	//log.Println("there is getkv")
	return string(v), err

}

func GetKeyValueWithP(key string, name string) (string, error) {
	var str string

	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(name))
	if b == nil {
		return "", errors.New("the bucket does not exist")
	}

	c := b.Cursor()
	prefix := []byte(key)
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		str = str + string(k) + "--" + string(v) + ", "
	}
	//log.Println("there is getkv with p")
	return str, err

}

func DeleteKeyValue(key string, name string) error {

	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(name))
	if b == nil {
		return errors.New("the bucket does not exist")
	}

	err = b.Delete([]byte(key))
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func BackupDatabase(path string) error {
	backuppath = path + Databasename
	db, err := bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.CopyFile(backuppath, 0600); err != nil {
		return err
	}
	backuped = true
	return nil
}

func RestorDatabase() error {
	if backuped == false {
		return errors.New("the backup database is not exist")
	}

	db, err := bolt.Open(backuppath, 0600, nil)
	if err != nil {
		log.Fatal(err)
		return errors.New("the backup database is not exist")
	}
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.CopyFile(Defaultpath, 0600); err != nil {
		return err
	}

	return nil
}
