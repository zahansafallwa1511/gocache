package cache

import "context"

// Listener observes cache activity. Register one with [WithListener] to record
// hit rates or trace cache behaviour.
//
// Callbacks run inline on the calling goroutine and receive fully resolved keys,
// prefix and tag namespace included. They must not block, and must not call back
// into the Cache that invoked them.
type Listener interface {
	OnHit(ctx context.Context, key string)
	OnMiss(ctx context.Context, key string)
	OnWrite(ctx context.Context, key string)
	OnForget(ctx context.Context, key string)
}

// ListenerFuncs adapts a set of functions to the [Listener] interface, so a
// listener can be written without declaring a type. Nil fields are ignored.
//
//	cache.WithListener(cache.ListenerFuncs{
//		Hit:  func(ctx context.Context, key string) { hits.Inc() },
//		Miss: func(ctx context.Context, key string) { misses.Inc() },
//	})
type ListenerFuncs struct {
	Hit    func(ctx context.Context, key string)
	Miss   func(ctx context.Context, key string)
	Write  func(ctx context.Context, key string)
	Forget func(ctx context.Context, key string)
}

// OnHit implements [Listener].
func (l ListenerFuncs) OnHit(ctx context.Context, key string) {
	if l.Hit != nil {
		l.Hit(ctx, key)
	}
}

// OnMiss implements [Listener].
func (l ListenerFuncs) OnMiss(ctx context.Context, key string) {
	if l.Miss != nil {
		l.Miss(ctx, key)
	}
}

// OnWrite implements [Listener].
func (l ListenerFuncs) OnWrite(ctx context.Context, key string) {
	if l.Write != nil {
		l.Write(ctx, key)
	}
}

// OnForget implements [Listener].
func (l ListenerFuncs) OnForget(ctx context.Context, key string) {
	if l.Forget != nil {
		l.Forget(ctx, key)
	}
}
