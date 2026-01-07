package main

import (
	"log"
	"net/http"

	"github.com/izzyblues/ratelimiter"
)

func main() {
	mux := http.NewServeMux()

	l := ratelimiter.NewTokenBucketLimiter(100, 100) // it limits to 100 requests per second

	mux.Handle("GET /foo", l.Middleware(http.HandlerFunc(fooHandler)))
	log.Print("listening on :3000...")
	err := http.ListenAndServe(":3000", mux)
	log.Fatal(err)
}

func fooHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.Path, "executing fooHandler")
	_, _ = w.Write([]byte("OK"))
}
