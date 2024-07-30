package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type HedgeOrderItem struct {
	SpotExchange    string  `json:"spot_exchange"`     // 现货交易所
	SwapExchange    string  `json:"swap_exchange"`     // 期货交易所
	Symbol          string  `json:"symbol"`            // 币对
	SpotOrderId     string  `json:"spot_order_id"`     // 现货订单ID
	SwapOrderId     string  `json:"swap_order_id"`     // 期货订单ID
	SpotTickerPrice float64 `json:"spot_ticker_price"` // 现货行情价
	SwapTickerPrice float64 `json:"swap_ticker_price"` // 期货行情价
	SpotTickerSize  float64 `json:"spot_ticker_size"`  // 现货交易量
	SwapTickerSize  float64 `json:"swap_ticker_size"`  // 期货交易量
}

// 对冲方案订单双向队列
// 1.对冲交易端，下单后写入
// 2.队列消费时，要同时获取到现货期货订单才能消费，否则视为消费失败
// 3.队列消费成功后出栈，删除订单表记录
// 4.队列消费失败，重新尾部入栈
type HedgeOrderConfig struct {
	ExchangeName string                    `json:"exchange_name"`
	RdsData      map[string]HedgeOrderItem `json:"rds_data"`
}

// example
// var HedgeOrderExample = HedgeOrderConfig{
// 	ExchangeName: "htx",
// 	RdsData: map[string]HedgeOrderItem{
// 		"DOT": {},
// 	},
// }

func (c HedgeOrderConfig) RdsName() string {
	return fmt.Sprintf("%s_hedge_order_config", c.ExchangeName)
}

func (c HedgeOrderConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}
func (c HedgeOrderConfig) Keys() (all []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c HedgeOrderConfig) GetAll() (all map[string]HedgeOrderItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]HedgeOrderItem{}
	for k, v := range res {
		item := HedgeOrderItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c HedgeOrderConfig) Vals() (all []HedgeOrderItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = []HedgeOrderItem{}
	for _, v := range res {
		item := HedgeOrderItem{}
		json.Unmarshal([]byte(v), &item)
		all = append(all, item)
	}
	return all, err
}
func (c HedgeOrderConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c HedgeOrderConfig) Get(key string) (value HedgeOrderItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c HedgeOrderConfig) Set(key string, value HedgeOrderItem) (err error) {
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

func (c HedgeOrderConfig) Del(key string) (err error) {
	err = redisDB.HDel(context.Background(), c.RdsName(), key).Err()
	if err != nil {
		return
	}
	return nil
}
