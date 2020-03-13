package cmd

import (
	"github.com/nsqio/go-nsq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var message string

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to NSQ",
	Long:  `Produce messages to NSQ`,
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Infoln("produce called")
		produce()
	},
}

func produce() {
	producer, err := nsq.NewProducer("127.0.0.1:4150", nsq.NewConfig())
	if err != nil {
		logrus.WithError(err).Fatal("new producer")
	}

	// Synchronously publish a single message to the specified topic.
	// Messages can also be sent asynchronously and/or in batches.
	if err = producer.Publish(topicName, []byte(message)); err != nil {
		logrus.WithError(err).Fatal("publish message")
	}
	logrus.Infof("Published message with body %q", message)

	// Gracefully stop the producer.
	producer.Stop()
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVar(&message, "message", "foobar", "message body")
}
