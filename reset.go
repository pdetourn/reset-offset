package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type offsetAction func(*kafka.Consumer, kafka.TopicPartition, kafka.Offset) error

// Set the offset of a partition to a new value.
func setOffset(consumer *kafka.Consumer, p kafka.TopicPartition, newOffset int64) error {
	res, err := consumer.CommitOffsets([]kafka.TopicPartition{
		{
			Topic:     p.Topic,
			Partition: p.Partition,
			Offset:    kafka.Offset(newOffset),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to commit offset for %v:%v to %v: %w", *p.Topic, p.Partition, newOffset, err)
	}
	if res[0].Error != nil {
		return fmt.Errorf("failed to commit offset for %v:%v to %v: %w", *p.Topic, p.Partition, newOffset, res[0].Error)
	}
	return nil
}

// Return an offsetAction that rewinds the offset of a partition by a given number of messages.
func rewindAction(howMany int64) offsetAction {
	return func(consumer *kafka.Consumer, p kafka.TopicPartition, currentOffset kafka.Offset) error {
		// Do not act if offset is either 0 or special (negative) value.
		if currentOffset > 0 {
			newOffset := int64(currentOffset) - howMany
			if newOffset <= 0 {
				newOffset = 0
			}
			err := setOffset(consumer, p, newOffset)
			if err != nil {
				return err
			}
			fmt.Printf("rewinded offset of %v:%v from %v down to %v\n", *p.Topic, p.Partition, currentOffset, newOffset)
		}
		return nil
	}
}

// Return an offsetAction that resets the offset of a partition to 0.
func resetAction() offsetAction {
	return func(consumer *kafka.Consumer, p kafka.TopicPartition, currentOffset kafka.Offset) error {
		// Do not act if offset is either 0 or special (negative) value.
		if currentOffset > 0 {
			err := setOffset(consumer, p, 0)
			if err != nil {
				return err
			}
			fmt.Printf("reset offset of %v:%v from %v back to 0\n", *p.Topic, p.Partition, currentOffset)
		}
		return nil
	}
}

// Perform the specified action on the given partition.
func act(consumer *kafka.Consumer, p kafka.TopicPartition, action offsetAction) error {
	// Get the current committed offset.
	committed, err := consumer.Committed([]kafka.TopicPartition{p}, 1000)
	if err != nil {
		return fmt.Errorf("failed to get committed offset for %v:%v: %w", *p.Topic, p.Partition, err)
	}
	if committed[0].Error != nil {
		return fmt.Errorf("failed to get committed offset for %v:%v: %w", *p.Topic, p.Partition, committed[0].Error)
	}
	currentOffset := committed[0].Offset

	// Then perform the action.
	err = action(consumer, p, currentOffset)
	if err != nil {
		return fmt.Errorf("failed to perform action on %v:%v: %w", *p.Topic, p.Partition, err)
	}
	return nil
}

func execute(ctx context.Context, broker string, groupId string, topic string, action offsetAction) error {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupId,
		"client.id":         "reset-client",
	}

	// Connect to the bootstrap server.
	consumer, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	// Subscribe to the topic. We need to listen to the rebalance event, so that we
	// can act immediately after we get the partitions assigned to us.
	assignments := make(chan kafka.AssignedPartitions, 1)
	err = consumer.Subscribe(topic, func(c *kafka.Consumer, e kafka.Event) error {
		if _, ok := e.(kafka.AssignedPartitions); ok {
			assignments <- e.(kafka.AssignedPartitions)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %v: %w", topic, err)
	}
	defer func() { _ = consumer.Unsubscribe() }()

	fmt.Println("waiting for topic assignments...")

	// This loop will continuously poll the consumer for events, and act immediately
	// after we receive the expected event.
	for {
		select {
		case a := <-assignments:
			// We received the assignments, let's act on them.
			for _, p := range a.Partitions {
				err := act(consumer, p, action)
				if err != nil {
					return fmt.Errorf("failed to act on %v:%v: %w", *p.Topic, p.Partition, err)
				}
			}
			return nil
		case <-ctx.Done():
			// We received a signal to stop.
			return ctx.Err()
		default:
			// Poll the consumer for events.
			_ = consumer.Poll(100)
		}
	}

}

func usage() {
	fmt.Printf("Usage: %v <broker> <groupid> <topic> <action>\n", filepath.Base(os.Args[0]))
	fmt.Println("  broker: Kafka broker address")
	fmt.Println("  groupid: Kafka consumer group id")
	fmt.Println("  topic: Kafka topic")
	fmt.Println("  action: rewind:<howmany> | reset")
}

func main() {
	// What to do? Make these program parameters in the future...
	if len(os.Args) != 5 {
		usage()
		os.Exit(1)
	}
	broker := os.Args[1]
	groupId := os.Args[2]
	topic := os.Args[3]
	actionString := os.Args[4]

	var action offsetAction

	if actionString == "reset" {
		action = resetAction()
	} else if strings.HasPrefix(actionString, "rewind:") {
		howMany, err := strconv.Atoi(strings.TrimPrefix(actionString, "rewind:"))
		if err != nil {
			usage()
			os.Exit(1)
		}
		action = rewindAction(int64(howMany))
	} else {
		usage()
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background())
	defer cancel()

	err := execute(ctx, broker, groupId, topic, action)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
}
