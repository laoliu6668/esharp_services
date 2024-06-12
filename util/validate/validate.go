package validate

import "regexp"

func Email(email string) bool {
	regex := regexp.MustCompile(`^[w.-]+@[a-zA-Z0-9]+.[a-zA-Z]{2,4}$`)
	return regex.MatchString(email)
}
