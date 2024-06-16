package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type HedgeSymbolConfig struct {
	Exchange string              `json:"exchange"`
	RdsData  map[string][]string `json:"rds_data"`
}

// example
var HedgeSymbolConfigExample = HedgeSymbolConfig{
	Exchange: "htx",
	RdsData: map[string][]string{
		"spot": {"BTC", "ETH"},
		"swap": {"ETH", "DOT", "LINK"},
	},
}

func (c *HedgeSymbolConfig) RdsName() string {
	return fmt.Sprintf("%s_hedge_symbol", c.Exchange)
}

func (c *HedgeSymbolConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *HedgeSymbolConfig) GetAll() (all map[string][]string, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string][]string{}
	for k, v := range res {
		item := []string{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *HedgeSymbolConfig) GetSpot() (all []string, err error) {
	return c.Get("spot")
}
func (c *HedgeSymbolConfig) GetSwap() (all []string, err error) {
	return c.Get("swap")
}
func (c *HedgeSymbolConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *HedgeSymbolConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}

func (c *HedgeSymbolConfig) Get(key string) (value []string, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c *HedgeSymbolConfig) SetSpot(value []string) (err error) {
	return c.Set("spot", value)
}
func (c *HedgeSymbolConfig) SetSwap(value []string) (err error) {
	return c.Set("swap", value)
}

func (c *HedgeSymbolConfig) Set(key string, value []string) (err error) {
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
