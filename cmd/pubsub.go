package cmd

import (
	"context"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	pubSubClient    *pubsub.Client
	pubSubTopic     *pubsub.Topic
	projectID       string
	pubSubTopicName string
)

// pubsubCmd represents the pubsub command
var pubsubCmd = &cobra.Command{
	Use:   "pubsub",
	Short: "Use the Google Pub/Sub emulator",
	Long:  `Run a subcommand to use the Google Pub/Sub emulator`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8681")

		var err error
		pubSubClient, err = pubsub.NewClient(context.Background(), projectID)
		if err != nil {
			logrus.WithError(err).Fatal("new Pub/Sub client")
		}
		logrus.Infof("Created new Pub/Sub client with project ID %q", projectID)

		pubSubTopic, err = ensureTopic(pubSubTopicName)
		if err != nil {
			logrus.WithError(err).Fatalf("ensure topic with name %s exists", pubSubTopicName)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Infoln("pubsub called")
	},
}

func init() {
	rootCmd.AddCommand(pubsubCmd)

	pubsubCmd.PersistentFlags().StringVar(&projectID, "projectID", "my-project", "Google project ID")
	pubsubCmd.PersistentFlags().StringVar(&pubSubTopicName, "pubSubTopicName", "my-topic", "Pub/Sub topic")
}

func ensureTopic(topicName string) (*pubsub.Topic, error) {
	topic := pubSubClient.Topic(topicName)
	exists, err := topic.Exists(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "topic exists")
	}

	if exists {
		logrus.Infof("Topic %v already exists.")
		return topic, nil
	}

	topic, err = pubSubClient.CreateTopic(context.Background(), topicName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Pub/Sub topic")
	}
	logrus.Infof("Created Pub/Sub topic: %v", topic)

	return topic, nil
}
