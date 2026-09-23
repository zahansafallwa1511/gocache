package cache_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zahansafallwa1511/gocache"
	"github.com/zahansafallwa1511/gocache/memory"
)

type article struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func ExampleNew() {
	c := cache.New(memory.New(), cache.WithPrefix("app:"))
	defer c.Close()

	ctx := context.Background()
	c.Set(ctx, "greeting", "hello", time.Hour)

	greeting, err := cache.Get[string](ctx, c, "greeting")
	fmt.Println(greeting, err)

}

func ExampleRemember() {
	c := cache.New(memory.New())
	defer c.Close()

	loadFromDB := func(ctx context.Context) (article, error) {
		fmt.Println("hitting the database")
		return article{ID: 1, Title: "Caching in Go"}, nil
	}

	ctx := context.Background()
	for range 3 {
		a, _ := cache.Remember(ctx, c, "article:1", time.Hour, loadFromDB)
		fmt.Println(a.Title)
	}

}

func ExampleCache_Tags() {
	c := cache.New(memory.New())
	defer c.Close()

	ctx := context.Background()
	tagged := c.Tags("articles")
	tagged.Set(ctx, "feed", []string{"a", "b"}, time.Hour)

	tagged.FlushTags(ctx)

	_, err := cache.Get[[]string](ctx, tagged, "feed")
	fmt.Println(errors.Is(err, cache.ErrNotFound))

}

func ExampleCache_Lock() {
	c := cache.New(memory.New())
	defer c.Close()

	ctx := context.Background()
	lock, err := c.Lock("rebuild-index", 30*time.Second)
	if err != nil {
		panic(err)
	}

	ran, err := lock.Get(ctx, func(ctx context.Context) error {
		fmt.Println("rebuilding the index")
		return nil
	})
	fmt.Println(ran, err)

}

func ExampleManager() {
	m := cache.NewManager()
	m.Register("memory", cache.New(memory.New()))
	defer m.Close()

	ctx := context.Background()
	m.MustStore("memory").Set(ctx, "k", 1, time.Minute)

	n, _ := cache.Get[int](ctx, m.Default(), "k")
	fmt.Println(n)

}
