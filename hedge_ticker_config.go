package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type DiffRate struct {
	Diff float64 `json:"diff"`
	Rate float64 `json:"rate"`
}

type HedgeTickerItem struct {
	Symbol       string   `json:"symbol"`
	SpotExchange string   `json:"spot_exchange"`
	SwapExchange string   `json:"swap_exchange"`
	Open         DiffRate `json:"open"`
	Close        DiffRate `json:"close"`
	UpdateAt     float64  `json:"update_at"`
}

type HedgeTickerConfig struct {
	SpotExchange string                     `json:"spot_exchange"`
	SwapExchange string                     `json:"swap_exchange"`
	RdsData      map[string]HedgeTickerItem `json:"rds_data"`
}

// example
var HedgeTickerConfigExample = HedgeTickerConfig{
	SpotExchange: "htx",
	SwapExchange: "binance",
	RdsData: map[string]HedgeTickerItem{
		"htx_htx_BTC": {},
	},
}

func (c *HedgeTickerConfig) RdsName() string {
	return "hedge_ticker"
}
func (c *HedgeTickerConfig) MQName() string {
	return fmt.Sprintf("%s_%s_hedge_ticker", c.SpotExchange, c.SwapExchange)
}
func (c *HedgeTickerConfig) RdsHKey(spotExchange, swapExchange, symbol string) string {
	return fmt.Sprintf("%s_%s_%s", spotExchange, swapExchange, symbol)
}

func (c *HedgeTickerConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *HedgeTickerConfig) GetAll() (all map[string]HedgeTickerItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]HedgeTickerItem{}
	for k, v := range res {
		item := HedgeTickerItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *HedgeTickerConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *HedgeTickerConfig) Get(key string) (value HedgeTickerItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c *HedgeTickerConfig) SetSymbol(symbol string, value HedgeTickerItem) (err error) {
	return c.Set(c.RdsHKey(c.SpotExchange, c.SwapExchange, symbol), value)
}
func (c *HedgeTickerConfig) Set(key string, value HedgeTickerItem) (err error) {
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
