package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Customer struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	Balance   string `json:"balance"`
	CreatedAt int64  `json:"created_at"`
}

type Source struct {
	Database string `json:"db"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
}

type DebeziumEvent struct {
	Before *Customer `json:"before"`
	After  *Customer `json:"after"`
	Source Source    `json:"source"`
	Op     string    `json:"op"`
}

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:19092"),

		kgo.ConsumeTopics(
			"shopdb.public.customers",
		),

		kgo.ConsumerGroup("go-cdc-consumer-v2"),

		kgo.ConsumeResetOffset(
			kgo.NewOffset().AtEnd(),
		),
	)

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	ctx := context.Background()

	fmt.Println("CDC consumer running...")
	fmt.Println("Topic: shopdb.public.customers")
	fmt.Println("Waiting for PostgreSQL changes...")
	fmt.Println("--------------------------------")

	for {
		fetches := client.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf(
					"Kafka error: %v\n",
					err,
				)
			}

			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			handleRecord(record)
		})
	}
}

func handleRecord(record *kgo.Record) {

	if record.Value == nil {
		fmt.Println("TOMBSTONE")

		if record.Key != nil {
			fmt.Printf(
				"Key: %s\n",
				string(record.Key),
			)
		}

		fmt.Println("--------------------------------")
		return
	}

	var event DebeziumEvent

	err := json.Unmarshal(
		record.Value,
		&event,
	)

	if err != nil {
		log.Printf(
			"could not decode event: %v\n",
			err,
		)
		return
	}

	if event.Op == "" {
		return
	}

	printEvent(event)
}

func printEvent(event DebeziumEvent) {

	switch event.Op {

	case "c":
		printCreate(event)

	case "u":
		printUpdate(event)

	case "d":
		printDelete(event)

	case "r":
		printSnapshot(event)

	default:
		fmt.Printf(
			"UNKNOWN OPERATION: %s\n",
			event.Op,
		)
	}

	fmt.Printf(
		"Source: %s.%s.%s\n",
		event.Source.Database,
		event.Source.Schema,
		event.Source.Table,
	)

	fmt.Println("--------------------------------")
}

func printCreate(event DebeziumEvent) {

	fmt.Println("CUSTOMER CREATED")

	if event.After == nil {
		fmt.Println("No customer data available.")
		return
	}

	fmt.Printf(
		"ID:      %d\n",
		event.After.ID,
	)

	fmt.Printf(
		"Name:    %s\n",
		event.After.Name,
	)

	fmt.Printf(
		"Email:   %s\n",
		event.After.Email,
	)

	fmt.Printf(
		"Balance: %s\n",
		event.After.Balance,
	)
}

func printUpdate(event DebeziumEvent) {

	fmt.Println("CUSTOMER UPDATED")

	if event.After == nil {
		fmt.Println("No updated customer data available.")
		return
	}

	fmt.Printf(
		"ID:   %d\n",
		event.After.ID,
	)

	fmt.Printf(
		"Name: %s\n",
		event.After.Name,
	)

	if event.Before != nil {
		fmt.Printf(
			"Balance: %s -> %s\n",
			event.Before.Balance,
			event.After.Balance,
		)
	} else {
		fmt.Printf(
			"Balance: %s\n",
			event.After.Balance,
		)
	}
}

func printDelete(event DebeziumEvent) {

	fmt.Println("CUSTOMER DELETED")

	if event.Before == nil {
		fmt.Println("No previous customer data available.")
		return
	}

	fmt.Printf(
		"ID:   %d\n",
		event.Before.ID,
	)

	fmt.Printf(
		"Name: %s\n",
		event.Before.Name,
	)

	fmt.Printf(
		"Email: %s\n",
		event.Before.Email,
	)
}

func printSnapshot(event DebeziumEvent) {

	fmt.Println("CUSTOMER SNAPSHOT")

	if event.After == nil {
		fmt.Println("No snapshot data available.")
		return
	}

	fmt.Printf(
		"ID:      %d\n",
		event.After.ID,
	)

	fmt.Printf(
		"Name:    %s\n",
		event.After.Name,
	)

	fmt.Printf(
		"Email:   %s\n",
		event.After.Email,
	)

	fmt.Printf(
		"Balance: %s\n",
		event.After.Balance,
	)
}
