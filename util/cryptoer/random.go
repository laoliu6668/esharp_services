package cryptoer

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func RandomInviteCode(n int) string {
	var letters = []byte("23456789qwertyupkjhgfdsazxcvbnmMNBVCXZASDFGHJKPOUYTREWQ")
	result := make([]byte, n)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)
}

func RandomAddress(n int) string {
	var letters = []byte("123456789qwertyupkjhgfdsazxcvbnmMNBVCXZASDFGHJKPOUYTREWQ")
	result := make([]byte, n)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)
}

func RandomString(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	sb := strings.Builder{}
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}
	return sb.String()
}

func RandomNo(n int) string {
	var letters = []byte("1234567890qwertyuiopasdfghjklzxcvbnm")
	result := make([]byte, n)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)
}
func GenerateNo(prefix string, id int) string {
	max := 26
	r := RandomNo(max)
	ids := strconv.FormatInt(int64(id), 16)
	return strings.ToUpper(fmt.Sprintf("%v%v%v%v", prefix, time.Now().Format("200601021504"), ids, r)[:max])
}
