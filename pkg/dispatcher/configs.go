package dispatcher

import "time"

type DispatcherConfigs struct {
	Retry                 RetryConfigs
	FetchLimit            uint32 // Maximum number of messages fetched per batch.
	ProcessingLockTimeout uint32 // Maximum duration (in seconds) a message remains locked for processing.
}

func DefaultDispatcherConfigs() DispatcherConfigs {
	return DispatcherConfigs{
		Retry:                 DefaultRetryConfigs(),
		FetchLimit:            100,
		ProcessingLockTimeout: 30,
	}
}

func DefaultRetryConfigs() RetryConfigs {
	return RetryConfigs{
		MaxRetryAttempts: 3,
		RetryDelay:       1 * time.Second,
		BackoffStrategy:  BackoffExponential,
		Jitter:           500 * time.Millisecond,
		MaxDelay:         30 * time.Second,
	}
}

type RetryConfigs struct {
	MaxRetryAttempts uint8           // Maximum retry attempts
	RetryDelay       time.Duration   // Initial delay between retries
	BackoffStrategy  BackoffStrategy // Strategy for increasing delay
	Jitter           time.Duration   // Random time variation added to delay
	MaxDelay         time.Duration   // Maximum delay between retries (also refered to as Timeout)
}

type BackoffStrategy string

const (
	BackoffFixed       BackoffStrategy = "fixed"       // Constant delay (2s → 2s → 2s → 2s)
	BackoffLinear      BackoffStrategy = "linear"      // Increases by a fixed multiple (2s → 4s → 6s → 8s).
	BackoffExponential BackoffStrategy = "exponential" // Doubles delay with each attempt (2s → 4s → 8s → 16s).
)
