package cache

import "expvar"

var (
    CacheHits     = expvar.NewInt("cache_hits")
    CacheMisses   = expvar.NewInt("cache_misses")
    CacheRequests = expvar.NewInt("cache_requests")
)