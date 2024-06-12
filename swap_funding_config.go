package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SwapFundingRateItem struct {
	Symbol      string  `json:"symbol"`       // 币对
	FundingRate float64 `json:"funding_rate"` // 费率
	FundingTime int64   `json:"funding_time"` // 费率时间 13位时间戳
}

type SwapFundingConfig struct {
	Exchange string                         `json:"exchange"`
	RdsData  map[string]SwapFundingRateItem `json:"rds_data"`
}

// example
var SwapFundingConfigExample = SwapFundingConfig{
	Exchange: "htx",
	RdsData: map[string]SwapFundingRateItem{
		"BTC": {},
	},
}

func (c *SwapFundingConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_funding", c.Exchange)
}

func (c *SwapFundingConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SwapFundingConfig) GetAll() (all map[string]SwapFundingRateItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SwapFundingRateItem{}
	for k, v := range res {
		item := SwapFundingRateItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SwapFundingConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}

func (c *SwapFundingConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SwapFundingConfig) Vals() (vals []SwapFundingRateItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SwapFundingRateItem{}
	for _, str := range strList {
		item := SwapFundingRateItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c *SwapFundingConfig) Get(key string) (value SwapFundingRateItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SwapFundingConfig) Set(key string, value SwapFundingRateItem) (err error) {
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
