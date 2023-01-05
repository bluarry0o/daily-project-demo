package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

func Produce(index int, cnf *sarama.Config, wg *sync.WaitGroup, cli sarama.SyncProducer) {
	//构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "test"
	msg.Value = sarama.StringEncoder(fmt.Sprintf("hello world!! %d", index))

	pid, offset, err := cli.SendMessage(msg)
	if err != nil {
		log.Fatalf("kafka  error while send message: %v\n", err)
	}

	log.Printf("pid: %v \t offect: %v \n", pid, offset)

	log.Println("Producer Finished")
	wg.Done()
}

func main() {
	cnf := sarama.NewConfig()

	//设置 ack应答机制
	cnf.Producer.RequiredAcks = sarama.WaitForAll

	//发送分区
	cnf.Producer.Partitioner = sarama.NewRandomPartitioner

	//回复确认
	cnf.Producer.Return.Successes = true

	var wg sync.WaitGroup

	//构造连接kafka
	cli, err := sarama.NewSyncProducer([]string{"a.bd.net:9092", "a.bd.net:9093", "a.bd.net:9094"}, cnf)
	if err != nil {
		log.Fatalf("kafka connect error: %v\n", err)
	}
	defer cli.Close()

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go Produce(i, cnf, &wg, cli)
		//time.Sleep(time.Second * 1)
	}
	wg.Wait()
}
