<!--
 * @Author: gitsrc
 * @Date: 2020-08-19 17:13:39
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-08-19 17:43:12
 * @FilePath: /ratelimiter-ci123/README.md
-->
# ratelimiter
## Features

- Distributed
- Atomicity
- High-performance
- Support redis cluster
- Support memory storage

## Installation

```sh
go get gitlab.oneitfarm.com/bifrost/ratelimiter
```

## HTTP Server Demo
Try into `gitlab.oneitfarm.com/bifrost/ratelimiter` directory:

```sh
go run example/main.go
```
Visit: http://127.0.0.1:8080/

```go
package main

import (
	"fmt"
	"html"
	"log"
	"net/http"
	"strconv"
	"time"

	redis "github.com/go-redis/redis"
	ratelimiter "gitlab.oneitfarm.com/bifrost/ratelimiter"
)

// Implements RedisClient for redis.ClusterClient
type clusterClient struct {
	*redis.ClusterClient
}

func (c *clusterClient) RateDel(key string) error {
	return c.Del(key).Err()
}

func (c *clusterClient) RateEvalSha(sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	return c.EvalSha(sha1, keys, args...).Result()
}

func (c *clusterClient) RateScriptLoad(script string) (string, error) {
	var sha1 string
	err := c.ForEachMaster(func(client *redis.Client) error {
		res, err := client.ScriptLoad(script).Result()
		if err == nil {
			sha1 = res
		}
		return err
	})
	return sha1, err
}

func main() {
	// use memory
	// limiter := ratelimiter.New(ratelimiter.Options{
	// 	Max:      10,
	// 	Duration: time.Minute, // limit to 1000 requests in 1 minute.
	// })
	var redisOptions = redis.ClusterOptions{Addrs: []string{
		"192.168.2.80:9001",
	}}
	// or use redis
	client := redis.NewClusterClient(&redisOptions)
	limiter, err := ratelimiter.New(ratelimiter.Options{Client: &clusterClient{client}, Max: 10})
	if err != nil {
		log.Println(err)
		return
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		res, err := limiter.Get(r.URL.Path)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		header := w.Header()
		header.Set("X-Ratelimit-Limit", strconv.FormatInt(int64(res.Total), 10))
		header.Set("X-Ratelimit-Remaining", strconv.FormatInt(int64(res.Remaining), 10))
		header.Set("X-Ratelimit-Reset", strconv.FormatInt(res.Reset.Unix(), 10))

		if res.Remaining >= 0 {
			w.WriteHeader(200)
			fmt.Fprintf(w, "Path: %q\n", html.EscapeString(r.URL.Path))
			fmt.Fprintf(w, "Remaining: %d\n", res.Remaining)
			fmt.Fprintf(w, "Total: %d\n", res.Total)
			fmt.Fprintf(w, "Duration: %v\n", res.Duration)
			fmt.Fprintf(w, "Reset: %v\n", res.Reset)
		} else {
			after := int64(res.Reset.Sub(time.Now())) / 1e9
			header.Set("Retry-After", strconv.FormatInt(after, 10))
			w.WriteHeader(429)
			fmt.Fprintf(w, "Rate limit exceeded, retry in %d seconds.\n", after)
		}
	})
	log.Fatal(http.ListenAndServe(":8081", nil))
}
```