package esharp_services

import (
	"context"
	"fmt"
)

// 对冲订单映射
// 1.对冲交易端，下单后写入
// 2.订阅现货订单成功后计算实开（平）差率及实际盈亏后删除
type HedgeOrderConfig struct {
	ExchangeName string            `json:"exchange_name"`
	RdsData      map[string]string `json:"rds_data"`
}

// example
var HedgeOrderExample = BaseConfig{
	ExchangeName: "htx",
	RdsData: map[string]string{
		"SP1x378f": "",
		"SW2fdwds": "",
	},
}

func (c *HedgeOrderConfig) RdsName() string {
	return fmt.Sprintf("%s_hedge_order_config", c.ExchangeName)
}

func (c *HedgeOrderConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}
func (c *HedgeOrderConfig) Keys() (all []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *HedgeOrderConfig) GetAll() (all map[string]string, err error) {
	return redisDB.HGetAll(context.Background(), c.RdsName()).Result()
}
func (c *HedgeOrderConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *HedgeOrderConfig) Get(key string) (ret string, err error) {
	return redisDB.HGet(context.Background(), c.RdsName(), key).Result()
}

func (c *HedgeOrderConfig) Set(key string, value string) (err error) {
	err = redisDB.HSet(context.Background(), c.RdsName(), key, value).Err()
	if err != nil {
		return
	}
	return nil
}

func (c *HedgeOrderConfig) Del(key string) (err error) {
	err = redisDB.HDel(context.Background(), c.RdsName(), key).Err()
	if err != nil {
		return
	}
	return nil
}
