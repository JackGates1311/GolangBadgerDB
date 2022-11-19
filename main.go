package main

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	"log"
	"os"
)

func main() {

	db, err := badger.Open(badger.DefaultOptions("tmp/badger"))

	if err != nil {
		log.Fatal(err)
	}

	defer func(db *badger.DB) {
		err := db.Close()
		if err != nil {

		}
	}(db)

	for {
		var choice string
		fmt.Println("Enter your choice")
		fmt.Println("1. Enter new (or override) key-value pair")
		fmt.Println("2. Display all records")
		fmt.Println("3. Delete records")
		fmt.Println("4. Exit")

		fmt.Scanln(&choice)

		switch choice {
		case "1":
			{
				var key, value string
				fmt.Println("Enter key:")
				fmt.Scanln(&key)
				fmt.Println("Enter value:")
				fmt.Scanln(&value)
				InsertRecords(db, key, value)
			}
		case "2":
			{
				DisplayRecords(db)
			}
		case "3":
			{
				var key string
				fmt.Println("Enter key:")
				fmt.Scanln(&key)
				DeleteRecords(db, key)
			}
		default:
			{
				fmt.Println("Exiting ...")
				os.Exit(0)
			}
		}

	}

}

func DeleteRecords(db *badger.DB, key string) {
	err := db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			if string(k) == key {
				err := txn.Delete(k)

				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println("Record deleted successfully (", key, ")")
				}
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}

}

func DisplayRecords(db *badger.DB) {

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		fmt.Println("==========================")
		fmt.Println("KEY AND VALUES")
		fmt.Println("==========================")
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {

				fmt.Printf("key=%s, value=%s\n", k, v)
				fmt.Println("==========================\n\n\n")
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}

}

func InsertRecords(db *badger.DB, key string, value string) {

	err := db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Record successfully inserted")
	}

}
