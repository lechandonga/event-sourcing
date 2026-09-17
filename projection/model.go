package projection

import (
	"context"

	"github.com/lechandonga/event-sourcing/eventstore"
)

// ReadModel is the projection state a domain author implements.
//
// Handle applies ONE event. Requirements for correct idempotent replay:
//
//   - Handle must be idempotent per event identity: if the same committed
//     event is delivered twice (crash between handler and checkpoint write,
//     late delivery, redelivery), applying it a second time must not change
//     the result. Use env.EventID (or env.Version within a stream) as the
//     dedup key when the model keeps durable state.
//   - Handle should be based on the event payload only; the projector feeds
//     events strictly in ascending global position, never backwards, and
//     filters duplicate/regressing positions before calling Handle.
//   - Events are upcast to the current schema before Handle runs; decoded is
//     the registry's decoded payload (nil when the projector runs without a
//     registry).
type ReadModel interface {
	Handle(ctx context.Context, env eventstore.Envelope, decoded any) error
}
