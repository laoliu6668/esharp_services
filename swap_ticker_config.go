package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type Values struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}
type Ticker struct {
	Exchange string  `json:"exchange"`
	Symbol   string  `json:"symbol"`
	Buy      Values  `json:"buy"`
	Sell     Values  `json:"sell"`
	UpdateAt float64 `json:"update_at"`
}

type SwapTickerConfig struct {
	Exchange string            `json:"exchange"`
	RdsData  map[string]Ticker `json:"rds_data"`
}

// example
var SwapTickerConfigExample = SwapTickerConfig{
	Exchange: "htx",
	RdsData: map[string]Ticker{
		"BTC": {
			Exchange: "htx",
			Symbol:   "BTC",
			Buy: Values{
				Price: 67777.15,
				Size:  1.08,
			},
			Sell: Values{
				Price: 67778.28,
				Size:  1.08,
			},
		},
	},
}

func (c *SwapTickerConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_ticker", c.Exchange)
}

func (c *SwapTickerConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SwapTickerConfig) GetAll() (all map[string]Ticker, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]Ticker{}
	for k, v := range res {
		item := Ticker{}
		err = json.Unmarshal([]byte(v), &item)
		if err != nil {
			return
		}
		all[k] = item
	}
	return all, err
}
func (c *SwapTickerConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SwapTickerConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *SwapTickerConfig) Get(key string) (value Ticker, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SwapTickerConfig) Set(key string, value Ticker) (err error) {
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
