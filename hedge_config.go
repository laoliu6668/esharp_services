package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/laoliu6668/esharp_services/util"
)

type HedgeConfig struct {
	RdsData map[string]any `json:"rds_data"`
}

// example
var HedgeConfigExample = HedgeConfig{
	RdsData: map[string]any{
		"htx_htx_ETH": map[string]any{
			"status":              true,
			"symbol":              "ETH",
			"spot_exchange":       "htx",
			"swap_exchange":       "htx",
			"open_rate":           2,
			"close_rate":          0,
			"swap_position_limit": 1000,
			"configs": map[string]any{
				"spot": map[string]any{},
				"swap": map[string]any{},
			},
		},
	},
}

func (c *HedgeConfig) RdsName() string {
	return "hedge_config"
}
func (c *HedgeConfig) RdsHKey(spotExchange, swapExchange, symbol string) string {
	return fmt.Sprintf("%s_%s_%s", spotExchange, swapExchange, symbol)
}

func (c *HedgeConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *HedgeConfig) GetAll() (all map[string]map[string]any, err error) {
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
func (c *HedgeConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *HedgeConfig) Get(key string) (value map[string]any, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *HedgeConfig) Set(key string, value any) (err error) {
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
