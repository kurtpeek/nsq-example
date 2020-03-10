package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/nsqio/go-nsq"
	"github.com/sirupsen/logrus"
)

type myMessageHandler struct{}

// HandleMessage implements the Handler interface
func (h *myMessageHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		return nil
	}
	logrus.Infoln(string(m.Body))
	return errors.New("this will re-queue the message")
}

func main() {
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("test", "channel", config)
	if err != nil {
		logrus.WithError(err).Fatal("new consumer")
	}

	consumer.AddHandler(&myMessageHandler{})

	if err := consumer.ConnectToNSQLookupd("localhost:4161"); err != nil {
		logrus.WithError(err).Fatal("connect to NSQ Lookup daemon")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	s := <-c
	fmt.Println("Got signal:", s)

	consumer.Stop()
}
