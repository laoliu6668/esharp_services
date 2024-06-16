package esharp_services

import (
	"context"
	"fmt"
)

type ApiConfig struct {
	ExchangeName string            `json:"exchange_name"`
	RdsData      map[string]string `json:"rds_data"`
}

// example
var ApiConfigExample = BaseConfig{
	ExchangeName: "htx",
	RdsData: map[string]string{
		"access_key": "",
		"secret_key": "",
	},
}

func (c *ApiConfig) RdsName() string {
	return fmt.Sprintf("%s_api_config", c.ExchangeName)
}

func (c *ApiConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}
func (c *ApiConfig) Keys() (all []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *ApiConfig) GetAll() (all map[string]string, err error) {
	return redisDB.HGetAll(context.Background(), c.RdsName()).Result()
}
func (c *ApiConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *ApiConfig) Get(key string) (ret string, err error) {
	return redisDB.HGet(context.Background(), c.RdsName(), key).Result()
}

func (c *ApiConfig) Set(key string, value string) (err error) {
	err = redisDB.HSet(context.Background(), c.RdsName(), key, value).Err()
	if err != nil {
		return
	}
	return nil
}
