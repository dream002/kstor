package kstordb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

const (
	Databasename = "my.db"
	Defaultpath  = "/home/zhangjiahua/codes/src/kstor/kstor_db/my.db"
	//Defaultpath = "../kstor_db/my.db"
)

var DB *bolt.DB
var err error

//var backuped bool = false

func init() {
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	DB, err = bolt.Open(Defaultpath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	//defer db.Close()

	//创建DB事务
	/*tx, err := DB.Begin(true)
	if err != nil {
		panic("init() db begin fail")
	}
	defer tx.Rollback()

	//创建名为backupbucket的bucket用于存储备份路径
	_, err = tx.CreateBucket([]byte("backupbucket"))
	if err != nil {
		fmt.Println("the backupbucket is exist")
	}

	//提交当前修改
	if err := tx.Commit(); err != nil {
		panic("init() Commit fail")
	}*/

	DB.Update(func(tx *bolt.Tx) error {
		//创建名为backupbucket的bucket用于存储备份路径
		_, err = tx.CreateBucket([]byte("backupbucket"))
		if err != nil {
			fmt.Println("the backupbucket is exist")
		}
		return nil
	})

}

func CreateBucket(bucketname string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte(bucketname))
		if err != nil {
			return err
		}
		return nil
	})
}

func DeleteBucket(bucketname string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		err = tx.DeleteBucket([]byte(bucketname))
		if err != nil {
			return err
		}
		return nil
	})
}

func SetKeyValue(key string, value string, name string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		//根据name找到对应bucket
		b := tx.Bucket([]byte(name))
		if b == nil {
			return errors.New("the bucket does not exist")
		}

		//将key/value对加入bucket
		err = b.Put([]byte(key), []byte(value))
		if err != nil {
			return err
		}
		return nil
	})
}

func GetKeyValue(key string, name string) (string, error) {
	var v []byte

	err := DB.View(func(tx *bolt.Tx) error {
		//根据name找到对应bucket
		b := tx.Bucket([]byte(name))
		if b == nil {
			return errors.New("the bucket does not exist")
		}

		//获得key对应value
		v = b.Get([]byte(key))
		if v == nil {
			return errors.New("the key does not exist")
		}
		return nil
	})
	return string(v), err

}

// 获得以key开头的kv组
func GetKeyValueWithP(key string, name string) (string, error) {
	var str string

	err := DB.View(func(tx *bolt.Tx) error {
		//根据name找到对应bucket
		b := tx.Bucket([]byte(name))
		if b == nil {
			return errors.New("the bucket does not exist")
		}

		//获得以key开头的kv组
		c := b.Cursor()
		prefix := []byte(key)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			str = str + string(k) + "--" + string(v) + ", "
		}
		return nil
	})
	return str, err
}

func DeleteKeyValue(key string, name string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		//根据name找到对应bucket
		b := tx.Bucket([]byte(name))
		if b == nil {
			return errors.New("the bucket does not exist")
		}

		//删除key/value
		err = b.Delete([]byte(key))
		if err != nil {
			return err
		}
		return nil
	})
}

func BackupDatabase(path string) error {
	return DB.Update(func(tx *bolt.Tx) error {
		//备份
		if err := tx.CopyFile(path, 0600); err != nil {
			return err
		}
		return nil
	})
}

func RestorDatabase() error {
	//从backupbucket获取备份路径
	backuppath, err := GetKeyValue("path", "backupbucket")
	if backuppath == "" || err != nil {
		return errors.New("the backup database is not exist or other errors")
	}

	//检查备份是否存在
	f, err := os.Open(backuppath)
	if err != nil {
		return errors.New("the backup database is not exist")
	}
	f.Close()

	//打开备份
	db, err := bolt.Open(backuppath, 0600, nil)
	if err != nil {
		log.Fatal(err)
		return errors.New("the backup database is not exist")
	}
	defer db.Close()

	//创建DB事务
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//复制备份
	if err := tx.CopyFile(Defaultpath, 0600); err != nil {
		return err
	}

	return nil
}
