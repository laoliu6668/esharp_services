package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type FundingHedgeTickerItem struct {
	Symbol        string   `json:"symbol"`
	LongExchange  string   `json:"long_exchange"`
	ShortExchange string   `json:"short_exchange"`
	Open          DiffRate `json:"open"`
	Close         DiffRate `json:"close"`
	UpdateAt      float64  `json:"update_at"`
}

type FundingHedgeTickerConfig struct {
	LongExchange  string                            `json:"long_exchange"`
	ShortExchange string                            `json:"short_exchange"`
	RdsData       map[string]FundingHedgeTickerItem `json:"rds_data"`
}

// example
// var HedgeTickerConfigExample = FundingHedgeTickerConfig{
// 	LongExchange: "htx",
// 	ShortExchange: "binance",
// 	RdsData: map[string]FundingHedgeTickerItem{
// 		"htx_htx_BTC": {},
// 	},
// }

func (c FundingHedgeTickerConfig) RdsName() string {
	return "funding_hedge_ticker"
}
func (c FundingHedgeTickerConfig) MQName() string {
	return fmt.Sprintf("%s_%s_funding_hedge_ticker", c.LongExchange, c.ShortExchange)
}
func (c FundingHedgeTickerConfig) RdsHKey(longExchange, shortExchange, symbol string) string {
	return fmt.Sprintf("%s_%s_%s", longExchange, shortExchange, symbol)
}

func (c FundingHedgeTickerConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c FundingHedgeTickerConfig) GetAll() (all map[string]FundingHedgeTickerItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]FundingHedgeTickerItem{}
	for k, v := range res {
		item := FundingHedgeTickerItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c FundingHedgeTickerConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c FundingHedgeTickerConfig) Get(key string) (value FundingHedgeTickerItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c FundingHedgeTickerConfig) SetSymbol(symbol string, value FundingHedgeTickerItem) (err error) {
	return c.Set(c.RdsHKey(c.LongExchange, c.ShortExchange, symbol), value)
}
func (c FundingHedgeTickerConfig) Set(key string, value FundingHedgeTickerItem) (err error) {
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
