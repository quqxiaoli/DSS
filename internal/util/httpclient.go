package util

import (
    "crypto/tls"
    "net/http"
    "time"
)

var globalClient *http.Client

func init() {
    globalClient = &http.Client{
        Transport: &http.Transport{
            TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
            MaxIdleConns:        50,
            MaxIdleConnsPerHost: 20,
            IdleConnTimeout:     15 * time.Second,
            DisableKeepAlives:   false,
            TLSHandshakeTimeout: 2 * time.Second,
        },
        Timeout: 10 * time.Second,
    }
}

func GetHTTPClient() *http.Client {
    return globalClient
}