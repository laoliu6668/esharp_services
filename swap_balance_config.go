package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SwapBalanceConfig struct {
	Exchange string                     `json:"exchange"`
	RdsData  map[string]SwapBalanceItem `json:"rds_data"`
}

type SwapBalanceItem struct {
	LockBalance float64 `json:"lock_balance"`
	FreeBalance float64 `json:"free_balance"`
	Symbol      string  `json:"symbol"`
}

// example
var SwapBalanceConfigExample = SwapBalanceConfig{
	Exchange: "htx",
	RdsData: map[string]SwapBalanceItem{
		"BTC": {},
	},
}

func (c *SwapBalanceConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_balance", c.Exchange)
}

func (c *SwapBalanceConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SwapBalanceConfig) GetAll() (all map[string]SwapBalanceItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SwapBalanceItem{}
	for k, v := range res {
		item := SwapBalanceItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SwapBalanceConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *SwapBalanceConfig) Get(key string) (value SwapBalanceItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c *SwapBalanceConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SwapBalanceConfig) Vals() (vals []SwapBalanceItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SwapBalanceItem{}
	for _, str := range strList {
		item := SwapBalanceItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c *SwapBalanceConfig) Set(key string, value SwapBalanceItem) (err error) {
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
func (c *SwapBalanceConfig) SetFreeBalance(key string, amount float64) (err error) {

	vals, err := c.Get(key)
	if err != nil {
		return
	}
	vals.FreeBalance = amount
	buf, _ := json.Marshal(vals)
	err = redisDB.HSet(context.Background(), c.RdsName(), key, string(buf)).Err()
	if err != nil {
		return
	}
	return nil
}
