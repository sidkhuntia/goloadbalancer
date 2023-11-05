package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Backend struct {
	url     *url.URL
	proxy   *httputil.ReverseProxy
	isAlive bool
	mux     sync.RWMutex
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.isAlive = alive
	b.mux.Unlock()
}

func (b *Backend) IsAlive() (alive bool) {
	b.mux.RLock()
	alive = b.isAlive
	b.mux.RUnlock()
	return
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

func (s *ServerPool) GetNextPeer() *Backend {
	next := s.NextIndex()
	l := len(s.backends) + next

	for i := next; i < l; i++ {
		idx := i % len(s.backends)

		if s.backends[idx].isAlive {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(idx))
			}
			return s.backends[idx]
		}
	}

	return nil
}

func (s *ServerPool) MarkBackendStatus(url *url.URL, alive bool) {
	for _, b := range s.backends {
		if b.url.String() == url.String() {
			b.SetAlive(alive)
			break
		}
	}
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 0
}

func loadBalancer(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	peer := serverPool.GetNextPeer()
	if peer != nil {
		log.Printf("%s(%s) forwarding to %s\n", r.RemoteAddr, r.URL.Path, peer.url)
		peer.proxy.ServeHTTP(w, r)
		return
	}

	http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
}

func isBackendAlive(url *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", url.Host, timeout)
	if err != nil {
		log.Println("Site unreachable, error: ", err)
		return false
	}
	_ = conn.Close()
	return true
}

func (s *ServerPool) checkHealth() {
	for _, b := range s.backends {
		status := "up"
		alive := isBackendAlive(b.url)
		b.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", b.url, status)
	}
}

func healthCheck() {
	t := time.NewTicker(time.Second * 30)
	for {
		select {
		case <-t.C:
			log.Println("Starting health check...")
			serverPool.checkHealth()
			log.Println("Health check completed")
		}
	}
}

var serverPool ServerPool

func main() {
	var serverList = []string{
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
	}

	for _, server := range serverList {
		url, err := url.Parse(server)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(url)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", url.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			serverPool.MarkBackendStatus(url, false)

			attemps := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attemps)
			ctx := context.WithValue(request.Context(), Attempts, attemps+1)
			loadBalancer(writer, request.WithContext(ctx))

		}
		serverPool.AddBackend(&Backend{
			url:     url,
			proxy:   proxy,
			isAlive: true,
		})

		log.Printf("Configured server: %s\n", url)

	}
	server := http.Server{
		Addr:    ":8080",
		Handler: http.HandlerFunc(loadBalancer),
	}

	go healthCheck()

	log.Println("Starting load balancer server on port 8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}
