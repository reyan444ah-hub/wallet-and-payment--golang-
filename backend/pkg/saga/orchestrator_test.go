package saga

import (
	"testing"
	"time"
)

func TestSagaRetryDelayUsesExponentialBackoffWithJitterAndCap(t *testing.T) {
	tests := []struct {
		name       string
		deliveries uint64
		minimum    time.Duration
		maximum    time.Duration
	}{
		{name: "first delivery", deliveries: 1, minimum: time.Second, maximum: 1500 * time.Millisecond},
		{name: "second delivery", deliveries: 2, minimum: 2 * time.Second, maximum: 3 * time.Second},
		{name: "retry cap", deliveries: 100, minimum: 30 * time.Second, maximum: 45 * time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for range 100 {
				delay := sagaRetryDelay(test.deliveries)
				if delay < test.minimum || delay > test.maximum {
					t.Fatalf("retry delay %s is outside [%s, %s]",
						delay, test.minimum, test.maximum)
				}
			}
		})
	}
}
