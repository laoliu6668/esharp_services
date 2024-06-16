package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SpotBalanceConfig struct {
	Exchange string                     `json:"exchange"`
	RdsData  map[string]SpotBalanceItem `json:"rds_data"`
}

type SpotBalanceItem struct {
	LockBalance float64 `json:"lock_balance"`
	FreeBalance float64 `json:"free_balance"`
	Symbol      string  `json:"symbol"`
	UpdateAt    int64   `json:"update_at"`
}

// example
var SpotBalanceConfigExample = SpotBalanceConfig{
	Exchange: "htx",
	RdsData: map[string]SpotBalanceItem{
		"BTC": {},
	},
}

func (c *SpotBalanceConfig) RdsName() string {
	return fmt.Sprintf("%s_spot_balance", c.Exchange)
}

func (c *SpotBalanceConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SpotBalanceConfig) GetAll() (all map[string]SpotBalanceItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SpotBalanceItem{}
	for k, v := range res {
		item := SpotBalanceItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SpotBalanceConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *SpotBalanceConfig) Get(key string) (value SpotBalanceItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c *SpotBalanceConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SpotBalanceConfig) Vals() (vals []SpotBalanceItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SpotBalanceItem{}
	for _, str := range strList {
		item := SpotBalanceItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c *SpotBalanceConfig) Set(key string, value SpotBalanceItem) (err error) {
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
func (c *SpotBalanceConfig) SetFreeBalance(key string, amount float64) (err error) {

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
