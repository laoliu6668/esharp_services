package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/laoliu6668/esharp_services/util"
)

type SpotTickerConfig struct {
	Exchange string         `json:"exchange"`
	RdsData  map[string]any `json:"rds_data"`
}

// example
var SpotTickerConfigExample = SpotTickerConfig{
	Exchange: "htx",
	RdsData: map[string]any{
		"BTC": map[string]any{
			"platform": "htx",
			"symbol":   "BTC",
			"buy": map[string]any{
				"price": "67777.15",
				"size":  "1.08",
			},
			"sell": map[string]any{
				"price":  "67778.28",
				"amount": "1.08",
			},
		},
	},
}

func (c *SpotTickerConfig) RdsName() string {
	return fmt.Sprintf("%s_spot_ticker", c.Exchange)
}

func (c *SpotTickerConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SpotTickerConfig) GetAll() (all map[string]map[string]any, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]map[string]any{}
	for k, v := range res {
		mp, err1 := util.JsonDecodeNumber(v)
		if err1 != nil {
			err = err1
			return all, err
		}
		all[k] = mp
	}
	return all, err
}
func (c *SpotTickerConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *SpotTickerConfig) Get(key string) (value Ticker, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SpotTickerConfig) Set(key string, value any) (err error) {
	buf, err := json.Marshal(value)
	if err != nil {
		return
	}
	err = redisDB.HSet(context.Background(), c.RdsName(), key, string(buf)).Err()
	if err != nil {
		return
	}
	return nil
}
