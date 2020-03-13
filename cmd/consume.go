package cmd

import (
	"os"
	"os/signal"

	"github.com/nsqio/go-nsq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from NSQ",
	Long:  `Consume messages from NSQ`,
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Infoln("consume called")
		consume()
	},
}

type myMessageHandler struct{}

// HandleMessage implements the Handler interface
func (h *myMessageHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		return nil
	}
	logrus.Infoln(string(m.Body))
	return nil
}

func consume() {
	consumer, err := nsq.NewConsumer(topicName, "test-channel", nsq.NewConfig())
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
	logrus.Infoln("Got signal:", s)

	consumer.Stop()
}

func init() {
	rootCmd.AddCommand(consumeCmd)
}
