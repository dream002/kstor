package kstorcmd

import (
	kc "github.com/dream002/kstor/kstor_client"
	pb "github.com/dream002/kstor/kstor_pb"

	"github.com/spf13/cobra"
)

const (
	backuppath = "/home/zhangjiahua/codes/src/kstor/kstor_backup/"
	//backuppath = "/kstor_backup/"
)

func Command(c pb.KstorClient) {

	var bucketname string
	var databasepath string
	var thekey string
	var thevalue string
	var prefix *int

	//use：命令名，short：命令短说明，run：命令对应操作
	var cmdBucket = &cobra.Command{
		Use:   "bucket",
		Short: "operate the bucket",
	}

	var cmdKey = &cobra.Command{
		Use:   "key",
		Short: "operate the key/value",
	}

	var cmdBackup = &cobra.Command{
		Use:   "backup",
		Short: "backup the database",
		Run: func(cmd *cobra.Command, args []string) {
			kc.BuckupDB(c, databasepath)
		},
	}
	//添加命令参数
	cmdBackup.Flags().StringVarP(&databasepath, "path", "p", backuppath, "the backup path")

	var cmdRestor = &cobra.Command{
		Use:   "restor",
		Short: "restor the database",
		Run: func(cmd *cobra.Command, args []string) {
			kc.RestorDB(c)
		},
	}

	var cmdCreate = &cobra.Command{
		Use:   "create",
		Short: "create a bucket space",
		Run: func(cmd *cobra.Command, args []string) {
			kc.CreateBucket(c, bucketname)
		},
	}
	//添加命令参数
	cmdCreate.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	//该参数不允许省略
	cmdCreate.MarkFlagRequired("name")

	var cmdDeletebk = &cobra.Command{
		Use:   "delete",
		Short: "delete the bucket space",
		Run: func(cmd *cobra.Command, args []string) {
			kc.DeleteBucket(c, bucketname)
		},
	}
	cmdDeletebk.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdDeletebk.MarkFlagRequired("name")

	var cmdSet = &cobra.Command{
		Use:   "set",
		Short: "add a key/value pair",
		Run: func(cmd *cobra.Command, args []string) {
			kc.SetKV(c, thekey, thevalue, bucketname)
		},
	}
	cmdSet.Flags().StringVarP(&thekey, "key", "k", "", "your key")
	cmdSet.MarkFlagRequired("key")
	cmdSet.Flags().StringVarP(&thevalue, "value", "v", "", "your value")
	cmdSet.MarkFlagRequired("value")
	cmdSet.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdSet.MarkFlagRequired("name")

	var cmdGet = &cobra.Command{
		Use:   "get",
		Short: "get the key pair",
		Run: func(cmd *cobra.Command, args []string) {
			if *prefix == 1 {
				kc.GetKVwithP(c, thekey, bucketname)
			} else {
				kc.GetKV(c, thekey, bucketname)
			}
		},
	}
	cmdGet.Flags().StringVarP(&thekey, "key", "k", "", "your key")
	cmdGet.MarkFlagRequired("key")
	cmdGet.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdGet.MarkFlagRequired("name")
	prefix = cmdGet.Flags().Count("prefix", "get key%")

	var cmdDeletekv = &cobra.Command{
		Use:   "delete",
		Short: "delete the key pair",
		Run: func(cmd *cobra.Command, args []string) {
			kc.DeleteKV(c, thekey, bucketname)
		},
	}
	cmdDeletekv.Flags().StringVarP(&thekey, "key", "k", "", "your key")
	cmdDeletekv.MarkFlagRequired("key")
	cmdDeletekv.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdDeletekv.MarkFlagRequired("name")

	//addcommand添加子命令
	var rootCmd = &cobra.Command{Use: "kstor"}
	rootCmd.AddCommand(cmdBucket, cmdKey, cmdBackup, cmdRestor)
	cmdBucket.AddCommand(cmdCreate, cmdDeletebk)
	cmdKey.AddCommand(cmdSet, cmdGet, cmdDeletekv)

	rootCmd.Execute()

}
