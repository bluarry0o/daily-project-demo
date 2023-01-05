package main

import (
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

var (
	TOPIC        = "test"
	CONNECT_ADDR = []string{"a.bd.net:9092", "a.bd.net:9093", "a.bd.net:9094"}
	wg           sync.WaitGroup
)

func main() {
	cnf := sarama.NewConfig()
	//设置 ack应答机制
	cnf.Producer.RequiredAcks = sarama.WaitForAll

	//发送分区
	cnf.Producer.Partitioner = sarama.NewRandomPartitioner

	//回复确认
	cnf.Producer.Return.Successes = true

	consumer, err := sarama.NewConsumer(CONNECT_ADDR, cnf)
	if err != nil {
		log.Fatalf("error while create Consumer: %v\n", err)
	}

	partitions, err := consumer.Partitions(TOPIC)
	if err != nil {
		log.Fatalf("error while get Partitions: %v\n", err)
	}

	log.Println("-----------------------partions start --------------")
	log.Println(partitions)
	log.Println("-----------------------partions end --------------")

	for p := range partitions {
		pt, err := consumer.ConsumePartition(TOPIC, int32(p), sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("error to get partion: topic：%v offect: %v\n", p)
		}
		defer pt.Close()
		wg.Add(1)
		go func(pt sarama.PartitionConsumer) {
			for msg := range pt.Messages() {
				log.Printf("partition: %v Offset: %v Key: %v Value: %s \n", msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(pt)

	}
	wg.Wait()
}
