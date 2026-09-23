package cache

import "context"

type Listener interface {
	OnHit(ctx context.Context, key string)
	OnMiss(ctx context.Context, key string)
	OnWrite(ctx context.Context, key string)
	OnForget(ctx context.Context, key string)
}

type ListenerFuncs struct {
	Hit    func(ctx context.Context, key string)
	Miss   func(ctx context.Context, key string)
	Write  func(ctx context.Context, key string)
	Forget func(ctx context.Context, key string)
}

func (l ListenerFuncs) OnHit(ctx context.Context, key string) {
	if l.Hit != nil {
		l.Hit(ctx, key)
	}
}

func (l ListenerFuncs) OnMiss(ctx context.Context, key string) {
	if l.Miss != nil {
		l.Miss(ctx, key)
	}
}

func (l ListenerFuncs) OnWrite(ctx context.Context, key string) {
	if l.Write != nil {
		l.Write(ctx, key)
	}
}

func (l ListenerFuncs) OnForget(ctx context.Context, key string) {
	if l.Forget != nil {
		l.Forget(ctx, key)
	}
}
