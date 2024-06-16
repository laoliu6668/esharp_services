package esharp_services

import (
	"context"
	"fmt"
	"strconv"
)

type BaseConfig struct {
	ExchangeName string            `json:"exchange_name"`
	RdsData      map[string]string `json:"rds_data"`
}

// example
var BaseConfigExample = BaseConfig{
	ExchangeName: "htx",
	RdsData: map[string]string{
		"uid":             "",
		"spot_account_id": "",
	},
}

func (c *BaseConfig) RdsName() string {
	return fmt.Sprintf("%s_base_config", c.ExchangeName)
}

func (c *BaseConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}
func (c *BaseConfig) Keys() (all []string, err error) {
	return redisDB.Keys(context.Background(), c.RdsName()).Result()
}
func (c *BaseConfig) GetAll() (all map[string]string, err error) {
	return redisDB.HGetAll(context.Background(), c.RdsName()).Result()
}

func (c *BaseConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *BaseConfig) Get(key string) (ret string, err error) {
	return redisDB.HGet(context.Background(), c.RdsName(), key).Result()
}
func (c *BaseConfig) GetInt(key string) (value int64, err error) {
	ret, err1 := c.Get(key)
	if err1 != nil {
		err = err1
		return
	}
	return strconv.ParseInt(ret, 10, 64)
}

func (c *BaseConfig) Set(key string, value string) (err error) {
	err = redisDB.HSet(context.Background(), c.RdsName(), key, value).Err()
	if err != nil {
		return
	}
	return nil
}

func (c *BaseConfig) SetInt(key string, value int64) (err error) {
	return c.Set(key, strconv.FormatInt(value, 10))

}
