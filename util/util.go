package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func FormatTimestamp(i int, layout string) string {
	if i == 0 {
		return ""
	}
	if layout == "" {
		layout = "2006/01/02 15:04:05"
	}
	return time.Unix(int64(i), 0).Format(layout)
}

func Now() int {
	return int(time.Now().Unix())
}

func InMap(s any, rows map[string]any) bool {
	for _, row := range rows {
		if row == s {
			return true
		}
	}
	return false
}

func MapKeys(rows map[string]any) []string {
	var res []string
	for key := range rows {
		res = append(res, key)
	}
	return res
}

func ArrayCloumn(array []map[string]any, cloumn string, indexKey string) (map[string]any, error) {
	var rows = make(map[string]any, len(array))
	for _, oneMap := range array {

		value, exist := oneMap[indexKey]
		if !exist {
			continue
		}

		newValue := ""

		if i, ok := value.(int); ok {
			newValue = string(strconv.Itoa(i))
		}

		if i, ok := value.(string); ok {
			newValue = i
		}

		if newValue == "" {
			continue
		}

		if cloumn == "" {
			rows[newValue] = oneMap
			continue
		}

		if i, ok := oneMap[cloumn]; ok {
			rows[newValue] = i
		}
	}

	if len(array) == len(rows) {
		return rows, nil
	}
	return rows, errors.New("转换失败")
}

func SliceJoinWith(data []int, sep string) (str string) {
	tmpData := make([]string, len(data))
	for i, v := range data {
		tmpData[i] = strconv.Itoa(v)
	}
	return strings.Join(tmpData, ",")
}

func StrSlice2int(data []string) (row []int) {
	row = make([]int, len(data))
	for i, v := range data {
		row[i], _ = strconv.Atoi(v)
	}
	return
}

func PrintDataType(data any) {
	fmt.Printf("data type : %s \n", reflect.TypeOf(data))
}

func MapToOptions(model []map[string]any, label string, value string) []map[string]any {
	roleOptions := make([]map[string]any, len(model))

	for i, v := range model {
		roleOptions[i] = map[string]any{
			"label": v[label],
			"value": v[value],
		}
	}
	return roleOptions
}

func ArrIsContain[T string | int | float64](arr []T, e T) bool {
	for _, v := range arr {
		if v == e {
			return true
		}
	}
	return false
}

func GetWeek(year, month, day int) int {
	var weekday = [7]int{7, 1, 2, 3, 4, 5, 6}
	var y, m, c int
	if month >= 3 {
		m = month
		y = year % 100
		c = year / 100
	} else {
		m = month + 12
		y = (year - 1) % 100
		c = (year - 1) / 100
	}
	week := y + (y / 4) + (c / 4) - 2*c + ((26 * (m + 1)) / 10) + day - 1
	if week < 0 {
		week = 7 - (-week)%7
	} else {
		week = week % 7
	}
	which_week := int(week)
	return weekday[which_week]
}
func GetWeekStr(year, month, day int) string {
	week := GetWeek(year, month, day)
	switch week {
	case 1:
		return "星期一"
	case 2:
		return "星期二"
	case 3:
		return "星期三"
	case 4:
		return "星期四"
	case 5:
		return "星期五"
	case 6:
		return "星期六"
	case 7:
		return "星期天"
	default:
		return ""
	}
}

// 生成32位uid
func GetUUID32() string {
	return strings.Replace(uuid.New().String(), "-", "", -1)
}

// 精度修复 保留prec位小数
func FixedFloat(f float64, prec int) float64 {
	str := strconv.FormatFloat(f, 'f', prec, 64)
	res, _ := strconv.ParseFloat(str, 64)
	return res
}

func HttpBuildQuery(params map[string]any) (param_str string) {
	uri := url.URL{}
	q := uri.Query()
	for k, v := range params {
		q.Add(k, fmt.Sprintf("%v", v))
	}
	return q.Encode()
}

func EmailRelaceWithSecret(email string) string {
	i := strings.Index(email, "@")
	if i == -1 {
		return email
	}
	email = email[0:2] + "***" + email[i:]
	return email
}

func ParseFloat(s any, emptyDefault float64) float64 {
	f, err := strconv.ParseFloat(fmt.Sprintf("%v", s), 64)
	if err != nil {
		return emptyDefault
	}
	return f
}

func ParseInt(s any, emptyDefault int64) int64 {
	f, err := strconv.ParseInt(fmt.Sprintf("%v", s), 10, 64)
	if err != nil {
		return emptyDefault
	}
	return f
}

func SubSlice(s []any, start, end int) []any {
	return s[start:end]
}
func WriteTestJsonFile(name string, body []byte) {
	f, err := os.Create(fmt.Sprintf("./%s.json", name))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer f.Close()
	f.Write(body)
}

func JsonDecodeNumber(s string) (mp map[string]any, err error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	err = d.Decode(&mp)
	if err != nil {
		return nil, err
	}
	return mp, nil
}
