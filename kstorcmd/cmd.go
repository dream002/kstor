package kstorcmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func Command() {

	var echoTimes int
	var bucketname string
	var databasepath string
	var thekey string
	var thevalue string
	var prefix string

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
			for i := 0; i < echoTimes; i++ {
				fmt.Println("Echo: " + strings.Join(args, " "))
			}
		},
	}
	cmdBackup.Flags().StringVarP(&databasepath, "path", "p", "", "the backup path")
	cmdBackup.MarkFlagRequired("path")

	var cmdRestor = &cobra.Command{
		Use:   "restor",
		Short: "restor the database",
		Run: func(cmd *cobra.Command, args []string) {
			for i := 0; i < echoTimes; i++ {
				fmt.Println("Echo: " + strings.Join(args, " "))
			}
		},
	}
	cmdRestor.Flags().StringVarP(&databasepath, "path", "p", "", "the backup path")
	cmdRestor.MarkFlagRequired("path")

	var cmdCreate = &cobra.Command{
		Use:   "create",
		Short: "create a bucket space",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(len(args))
			fmt.Println("Echo: " + strings.Join(args, " ") + bucketname)
		},
	}
	cmdCreate.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdCreate.MarkFlagRequired("name")

	var cmdDeletebk = &cobra.Command{
		Use:   "delete",
		Short: "delete the bucket space",
		Run: func(cmd *cobra.Command, args []string) {
			for i := 0; i < echoTimes; i++ {
				fmt.Println("Echo: " + strings.Join(args, " "))
			}
		},
	}
	cmdDeletebk.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdDeletebk.MarkFlagRequired("name")

	var cmdSet = &cobra.Command{
		Use:   "set",
		Short: "add a key/value pair",
		Run: func(cmd *cobra.Command, args []string) {
			//for i := 0; i < echoTimes; i++ {
			fmt.Println(len(args))
			fmt.Println("Echo: " + strings.Join(args, " "))
			//}
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
			//for i := 0; i < echoTimes; i++ {
			fmt.Println("Echo: " + strings.Join(args, " "))
			//}
		},
	}
	cmdGet.Flags().StringVarP(&thekey, "key", "k", "", "your key")
	cmdGet.MarkFlagRequired("key")
	cmdGet.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdGet.MarkFlagRequired("name")
	cmdGet.Flags().StringVarP(&prefix, "prefix", "", "", "get key%")

	var cmdDeletekv = &cobra.Command{
		Use:   "delete",
		Short: "delete the key pair",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			for i := 0; i < echoTimes; i++ {
				fmt.Println("Echo: " + strings.Join(args, " "))
			}
		},
	}
	cmdDeletekv.Flags().StringVarP(&thekey, "key", "k", "", "your key")
	cmdDeletekv.MarkFlagRequired("key")
	cmdDeletekv.Flags().StringVarP(&bucketname, "name", "n", "", "the bucket name")
	cmdDeletekv.MarkFlagRequired("name")

	var rootCmd = &cobra.Command{Use: "kstor"}
	rootCmd.AddCommand(cmdBucket, cmdKey, cmdBackup, cmdRestor)
	cmdBucket.AddCommand(cmdCreate, cmdDeletebk)
	cmdKey.AddCommand(cmdSet, cmdGet, cmdDeletekv)
	rootCmd.Execute()

}
