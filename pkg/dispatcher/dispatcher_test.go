package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"go-transactional-outbox/pkg/core"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockOutboxMessageRepository struct {
	mock.Mock
}

func (m *MockOutboxMessageRepository) SaveMessage(ctx context.Context, message core.OutboxMessage) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

func (m *MockOutboxMessageRepository) FetchPendingMessages(ctx context.Context, limit int) ([]core.OutboxMessage, error) {
	args := m.Called(ctx, limit)
	return args.Get(0).([]core.OutboxMessage), args.Error(1)
}

func (m *MockOutboxMessageRepository) MarkMessageAsSent(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockOutboxMessageRepository) MarkMessageAsFailed(ctx context.Context, id string, error string) error {
	args := m.Called(ctx, id, error)
	return args.Error(0)
}

type MockOutboxMessagePublisher struct {
	mock.Mock
}

func (m *MockOutboxMessagePublisher) Publish(ctx context.Context, message core.OutboxMessage) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

func TestDefaultOutboxMessageDispatcher_Success(t *testing.T) {
	mockRepo := new(MockOutboxMessageRepository)
	mockPub := new(MockOutboxMessagePublisher)

	dispatcher := &DefaultOutboxMessageDispatcher{
		repository: mockRepo,
		publisher:  mockPub,
		configs:    DefaultDispatcherConfigs(),
	}

	ctx := context.Background()

	messages := []core.OutboxMessage{
		{ID: "1", Payload: "Test Message 1", Status: core.MessageStatusPending},
		{ID: "2", Payload: "Test Message 2", Status: core.MessageStatusPending},
	}

	mockRepo.On("FetchPendingMessages", ctx, dispatcher.configs.FetchLimit).Return(messages, nil)
	mockPub.On("Publish", ctx, messages[0]).Return(nil)
	mockPub.On("Publish", ctx, messages[1]).Return(nil)
	mockRepo.On("MarkMessageAsSent", ctx, "1").Return(nil)
	mockRepo.On("MarkMessageAsSent", ctx, "2").Return(nil)

	err := dispatcher.Dispatch(ctx)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockPub.AssertExpectations(t)
}

func TestDefaultOutboxMessageDispatcher_PartialFailure(t *testing.T) {
	mockRepo := new(MockOutboxMessageRepository)
	mockPub := new(MockOutboxMessagePublisher)

	dispatcher := &DefaultOutboxMessageDispatcher{
		repository: mockRepo,
		publisher:  mockPub,
		configs:    DefaultDispatcherConfigs(),
	}

	ctx := context.Background()

	messages := []core.OutboxMessage{
		{ID: "1", Payload: "Test Message 1", Status: core.MessageStatusPending},
		{ID: "2", Payload: "Test Message 2", Status: core.MessageStatusPending},
	}

	mockRepo.On("FetchPendingMessages", ctx, dispatcher.configs.FetchLimit).Return(messages, nil)
	mockPub.On("Publish", ctx, messages[0]).Return(nil)                             // First succeeds
	mockPub.On("Publish", ctx, messages[1]).Return(errors.New("failed to publish")) // Second fails
	mockRepo.On("MarkMessageAsSent", ctx, "1").Return(nil)
	mockRepo.On("MarkMessageAsFailed", ctx, "2", "failed to publish").Return(nil)

	err := dispatcher.Dispatch(ctx)
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockPub.AssertExpectations(t)
}

func TestDefaultOutboxMessageDispatcher_RepositoryFetchError(t *testing.T) {
	mockRepo := new(MockOutboxMessageRepository)
	mockPub := new(MockOutboxMessagePublisher)

	dispatcher := &DefaultOutboxMessageDispatcher{
		repository: mockRepo,
		publisher:  mockPub,
		configs:    DefaultDispatcherConfigs(),
	}

	ctx := context.Background()

	errorMessage := "fetch error"
	mockRepo.On("FetchPendingMessages", ctx, dispatcher.configs.FetchLimit).Return([]core.OutboxMessage{}, errors.New(errorMessage))

	err := dispatcher.Dispatch(ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, fmt.Sprintf("failed to fetch messages: %s", errorMessage))

	mockRepo.AssertExpectations(t)
	mockPub.AssertExpectations(t)
}
