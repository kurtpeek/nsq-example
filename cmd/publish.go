package cmd

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var pubSubMessageBody string

// publishCmd represents the publish command
var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish a message to the Google Pub/Sub emulator",
	Long:  `Publish a message to the Google Pub/Sub emulator`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logrus.Infoln("publish called")
		return publish(pubSubMessageBody)
	},
}

func publish(pubSubMessageBody string) error {
	logrus.Infoln("Publishing message...")
	publishResult := pubSubTopic.Publish(context.Background(), &pubsub.Message{
		Data: []byte(pubSubMessageBody),
	})

	messageID, err := publishResult.Get(context.Background())
	if err != nil {
		return errors.Wrap(err, "get publish result")
	}
	logrus.Infof("Published a message with ID %s and message body %s", messageID, pubSubMessageBody)
	return nil
}

func init() {
	pubsubCmd.AddCommand(publishCmd)

	publishCmd.Flags().StringVar(&pubSubMessageBody, "message", "foobar", "Pub/Sub message body")
}
