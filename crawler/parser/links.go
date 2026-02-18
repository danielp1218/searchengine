package parser

import (
	"errors"
	"net"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

var ErrEmptyHost = errors.New("url host is empty")

func isFileLink(link string) bool {
	fileExtensions := []string{
		".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg",
		".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
		".zip", ".rar", ".7z", ".tar", ".gz",
		".mp3", ".wav", ".mp4", ".avi", ".mkv",
	}
	for _, ext := range fileExtensions {
		if len(link) >= len(ext) && link[len(link)-len(ext):] == ext {
			return true
		}
	}
	return false
}

func isValidURL(toTest string) bool {
	_, err := url.ParseRequestURI(toTest)
	if err != nil {
		return false
	}

	u, err := url.Parse(toTest)
	if err != nil || !(u.Scheme == "http" || u.Scheme == "https") || u.Host == "" {
		return false
	}
	if isFileLink(u.Path) {
		return false
	}

	return true
}

func GetDomain(link string) (string, error) {
	parsedURL, err := url.Parse(link)
	if err != nil {
		return link, err
	}
	return parsedURL.Hostname(), nil
}

// returns a stable bucket key for scheduling/rate limiting.
// using eTLD+1 for key
// TODO: might explode message queue when scaled, look into hashing keys when this breaks
func GetBucketKey(link string) (string, error) {
	parsedURL, err := url.Parse(link)
	if err != nil {
		return "", err
	}

	host := strings.ToLower(strings.TrimSpace(parsedURL.Hostname()))
	if host == "" {
		return "", ErrEmptyHost
	}

	if host == "localhost" || net.ParseIP(host) != nil {
		return host, nil
	}

	bucket, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return host, nil
	}

	return strings.ToLower(bucket), nil
}
