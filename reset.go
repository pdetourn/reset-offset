package main

import (
	"fmt"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type offsetAction func(*kafka.Consumer, kafka.TopicPartition, kafka.Offset) error

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

func rewindAction(howMany int64) offsetAction {
	return func(consumer *kafka.Consumer, p kafka.TopicPartition, currentOffset kafka.Offset) error {
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

func resetAction() offsetAction {
	return func(consumer *kafka.Consumer, p kafka.TopicPartition, currentOffset kafka.Offset) error {
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

func act(consumer *kafka.Consumer, p kafka.TopicPartition, action offsetAction) error {
	committed, err := consumer.Committed([]kafka.TopicPartition{p}, 1000)
	if err != nil {
		return fmt.Errorf("failed to get committed offset for %v:%v: %w", *p.Topic, p.Partition, err)
	}
	if committed[0].Error != nil {
		return fmt.Errorf("failed to get committed offset for %v:%v: %w", *p.Topic, p.Partition, committed[0].Error)
	}

	currentOffset := committed[0].Offset
	err = action(consumer, p, currentOffset)
	if err != nil {
		return fmt.Errorf("failed to perform action on %v:%v: %w", *p.Topic, p.Partition, err)
	}
	return nil
}

func execute(broker string, groupId string, topic string, action offsetAction) error {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupId,
		"client.id":         "reset-client",
	}

	consumer, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

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

	for {
		select {
		case a := <-assignments:
			for _, p := range a.Partitions {
				err := act(consumer, p, action)
				if err != nil {
					return fmt.Errorf("failed to act on %v:%v: %w", *p.Topic, p.Partition, err)
				}
			}
			return nil
		default:
			_ = consumer.Poll(100)
		}
	}

}

func main() {
	// What to do? Make these program parameters in the future...
	broker := "localhost:9092"
	groupId := "groupid"
	topic := "topic"
	action := rewindAction(100)
	//action := resetAction()
	err := execute(broker, groupId, topic, action)
	if err != nil {
		fmt.Println(err)
	}
}
