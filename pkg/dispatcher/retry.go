package dispatcher

import (
	"math/rand"
	"time"
)

func (d *DefaultOutboxMessageDispatcher) calculateRetryDelay(attempt uint8) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	retryConfigs := d.configs.Retry

	var delay time.Duration

	// Backoff Strategy
	switch retryConfigs.BackoffStrategy {
	case BackoffExponential:
		// Exponential: RetryDelay * 2^(attempt-1)
		delay = retryConfigs.RetryDelay * time.Duration(1<<(attempt-1))
	case BackoffLinear:
		// Linear: RetryDelay * attempt
		delay = retryConfigs.RetryDelay * time.Duration(attempt)
	default:
		// Fixed: RetryDelay
		delay = retryConfigs.RetryDelay
	}

	// Add Jitter
	if retryConfigs.Jitter > 0 {
		jitter := time.Duration(rand.Int63n(int64(retryConfigs.Jitter)))
		delay += jitter
	}

	// Cap at MaxDelay
	if retryConfigs.MaxDelay > 0 && delay > retryConfigs.MaxDelay {
		delay = retryConfigs.MaxDelay
	}

	return delay
}
