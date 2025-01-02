package sqs

import (
	"context"
	"errors"
	"fmt"
	"go-transactional-outbox/pkg/core"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockSQSClient struct {
	mock.Mock
}

func (m *MockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) != nil {
		return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
	}
	return nil, args.Error(1)
}

func TestSQSPublisher_Publish_Success(t *testing.T) {
	mockClient := new(MockSQSClient)
	publisher := &SQSPublisher{
		client:   mockClient,
		queueURL: "https://sqs.example.com/queue",
	}

	testMessage := core.OutboxMessage{
		ID:      "123",
		Payload: "Test Payload",
		Status:  core.MessageStatusPending,
	}

	mockClient.On("SendMessage", mock.Anything, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		return *input.QueueUrl == "https://sqs.example.com/queue" &&
			*input.MessageBody == "Test Payload" &&
			input.MessageAttributes["MessageID"].DataType != nil &&
			*input.MessageAttributes["MessageID"].DataType == "String" &&
			input.MessageAttributes["MessageID"].StringValue != nil &&
			*input.MessageAttributes["MessageID"].StringValue == "123"
	})).Return(&sqs.SendMessageOutput{
		MessageId: aws.String("msg-123"),
	}, nil)

	err := publisher.Publish(context.Background(), testMessage)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestSQSPublisher_Publish_Failure(t *testing.T) {
	mockClient := new(MockSQSClient)
	publisher := &SQSPublisher{
		client:   mockClient,
		queueURL: "https://sqs.example.com/queue",
	}

	testMessage := core.OutboxMessage{
		ID:      "123",
		Payload: "Test Payload",
		Status:  core.MessageStatusPending,
	}

	fakeAwsSqsErrorMessage := "failed to send message"
	mockClient.On("SendMessage", mock.Anything, mock.Anything).Return(
		nil, errors.New(fakeAwsSqsErrorMessage),
	)

	err := publisher.Publish(context.Background(), testMessage)

	assert.Error(t, err)
	assert.EqualError(t, err, fmt.Sprintf("failed to send message to SQS: %s", fakeAwsSqsErrorMessage))
	mockClient.AssertExpectations(t)
}
