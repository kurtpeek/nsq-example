package cmd

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	sub            *pubsub.Subscription
	subscriptionID string
)

// receiveCmd represents the receive command
var receiveCmd = &cobra.Command{
	Use:   "receive",
	Short: "Receive messages from Pub/Sub",
	Long:  `Receive and handle messages from Pub/Sub`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logrus.Infoln("receive called")
		return receive(pubSubTopicName, subscriptionID)
	},
}

func receive(pubSubTopicName, subscriptionID string) error {
	var err error
	sub, err = ensureSubscription(pubSubTopicName, subscriptionID)
	if err != nil {
		return errors.Wrap(err, "ensure subscription exists")
	}

	logrus.Infoln("Receiving messages...")
	var mu sync.Mutex
	received := 0
	cctx, cancel := context.WithCancel(context.Background())
	if err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		logrus.Infof("got message: %q", string(msg.Data))
		msg.Ack()

		mu.Lock()
		defer mu.Unlock()
		received++
		if received == 10 {
			cancel()
		}
	}); err != nil {
		return errors.Wrap(err, "Receive")
	}
	return nil
}

func init() {
	pubsubCmd.AddCommand(receiveCmd)

	receiveCmd.Flags().StringVar(&subscriptionID, "subscription", "my-subscription", "ID for Pub/Sub subscription")
}

func ensureSubscription(topicName, subscriptionName string) (*pubsub.Subscription, error) {
	topic, err := ensureTopic(topicName)
	if err != nil {
		return nil, errors.Wrap(err, "ensure topic exists")
	}

	subscription := pubSubClient.Subscription(subscriptionName)
	exists, err := subscription.Exists(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "subscription exists")
	}

	if exists {
		logrus.Infof("Subscription %v already exists.")
		return subscription, nil
	}

	subscription, err = pubSubClient.CreateSubscription(context.Background(), subscriptionName, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Pub/Sub subscription")
	}
	logrus.Infof("Created Pub/Sub subscription: %v", subscription)

	return subscription, nil
}
