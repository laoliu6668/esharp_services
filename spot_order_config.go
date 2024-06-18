package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SpotOrderItem struct {
	Exchange    string  `json:"exchange"`     // 交易所
	Symbol      string  `json:"symbol"`       // 币对
	OrderId     string  `json:"order_id"`     // 订单编号
	OrderType   string  `json:"order_type"`   // 订单类型 buy-market: 市价买单 sell-market: 市价卖单
	OrderPrice  float64 `json:"order_price"`  // 下单价格
	TradePrice  float64 `json:"trade_price"`  // 成交价格
	OrderValue  float64 `json:"order_value"`  // 下单金额
	TradeValue  float64 `json:"trade_value"`  // 成交金额
	OrderVolume float64 `json:"order_volume"` // 下单数量
	TradeVolume float64 `json:"trade_volume"` // 成交数量
	Status      int64   `json:"status"`       // 订单状态 1-已下单 2-已成交 8-已撤单
	CreateAt    int64   `json:"create_at"`    // 下单时间 13
	FilledAt    int64   `json:"filled_at"`    // 成交时间 13
	CancelAt    int64   `json:"cancel_at"`    // 撤单时间 13
}

type SpotOrderConfig struct {
	ExchangeName string                   `json:"exchange_name"`
	RdsData      map[string]SpotOrderItem `json:"rds_data"`
}

// example
var SpotOrderExample = SpotOrderConfig{
	ExchangeName: "htx",
	RdsData: map[string]SpotOrderItem{
		"SPferwa1213": {},
	},
}

func (c *SpotOrderConfig) RdsName() string {
	return fmt.Sprintf("%s_spot_order_config", c.ExchangeName)
}

func (c *SpotOrderConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}
func (c *SpotOrderConfig) Keys() (all []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SpotOrderConfig) GetAll() (all map[string]SpotOrderItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SpotOrderItem{}
	for k, v := range res {
		item := SpotOrderItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SpotOrderConfig) Vals() (all []SpotOrderItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = []SpotOrderItem{}
	for _, v := range res {
		item := SpotOrderItem{}
		json.Unmarshal([]byte(v), &item)
		all = append(all, item)
	}
	return all, err
}
func (c *SpotOrderConfig) Has(orderId string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), orderId).Result()
	return
}
func (c *SpotOrderConfig) Get(orderId string) (value SpotOrderItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), orderId).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SpotOrderConfig) Set(orderId string, value SpotOrderItem) (err error) {
	buf, err := json.Marshal(value)
	if err != nil {
		return
	}
	err = redisDB.HSet(context.Background(), c.RdsName(), orderId, string(buf)).Err()
	if err != nil {
		return
	}
	return nil
}

func (c *SpotOrderConfig) Del(orderId string) (err error) {
	err = redisDB.HDel(context.Background(), c.RdsName(), orderId).Err()
	if err != nil {
		return
	}
	return nil
}
