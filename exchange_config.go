package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/laoliu6668/esharp_services/util"
)

type ExchangeConfig struct {
	ExchangeName string         `json:"exchange_name"`
	RdsData      map[string]any `json:"rds_data"`
}

// example
var ExchangeConfigExample = ExchangeConfig{
	ExchangeName: "htx",
	RdsData: map[string]any{
		"api": map[string]any{
			"access_key": "",
			"secret_key": "",
		},
		"account": map[string]any{
			"uid":             0,
			"spot_account_id": 0,
		},
	},
}

func (c *ExchangeConfig) RdsName() string {
	return fmt.Sprintf("%s_config", c.ExchangeName)
}

func (c *ExchangeConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *ExchangeConfig) GetAll() (all map[string]map[string]any, err error) {
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
func (c *ExchangeConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *ExchangeConfig) Get(key string) (value map[string]any, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c *ExchangeConfig) GetString(key string) (value string, err error) {
	return redisDB.HGet(context.Background(), c.RdsName(), key).Result()
}

func (c *ExchangeConfig) Set(key string, value any) (err error) {
	fmt.Printf("value: %s\n", value)
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
