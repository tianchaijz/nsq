package auth

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
)

func AccessToken(id string, secret string) string {
	if id == "" {
		hostname, err := os.Hostname()
		if err == nil {
			id = hostname
		}
	}

	s := fmt.Sprintf("%s&%s", id, secret)

	first := md5.Sum([]byte(s))
	second := md5.Sum(first[:])

	return hex.EncodeToString(second[:])
}
