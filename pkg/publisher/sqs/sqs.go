package sqs

import (
	"context"
	"fmt"
	"go-transactional-outbox/pkg/core"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSPublisher struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSOutboxMessagePublisher(ctx context.Context, queueURL string) (*SQSPublisher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	client := sqs.NewFromConfig(cfg)

	return &SQSPublisher{
		client:   client,
		queueURL: queueURL,
	}, nil
}

func (p *SQSPublisher) Publish(ctx context.Context, message core.OutboxMessage) error {
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(p.queueURL),
		MessageBody: aws.String(message.Payload),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"MessageID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(message.ID),
			},
		},
	}

	_, err := p.client.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to send message to SQS: %w", err)
	}

	return nil
}
