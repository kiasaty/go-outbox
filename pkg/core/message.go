package core

import "time"

type OutboxMessage struct {
	ID          string    `json:"id"`
	Payload     string    `json:"payload"`
	Status      string    `json:"status"`
	Attempts    uint8     `json:"attempts"`
	AvailableAt time.Time `json:"available_at"`
	CreatedAt   time.Time `json:"created_at"`
}

func (m *OutboxMessage) GetRetryAttempts() uint8 {
	if m.Attempts == 0 {
		return 0
	}

	return m.Attempts - 1 // The first attempt is not a retry attempt
}
