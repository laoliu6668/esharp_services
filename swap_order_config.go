package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SwapOrderItem struct {
	Exchange    string  `json:"exchange"`     // 交易所
	Symbol      string  `json:"symbol"`       // 币对
	OrderId     string  `json:"order_id"`     // 订单编号
	OrderType   string  `json:"order_type"`   // 订单类型 buy-open: 买入开多 buy-close: 买入平空 sell-open: 卖出开空 sell-close: 卖出平多
	OrderPrice  float64 `json:"order_price"`  // 下单价格
	TradePrice  float64 `json:"trade_price"`  // 成交价格
	OrderValue  float64 `json:"order_value"`  // 下单金额
	TradeValue  float64 `json:"trade_value"`  // 成交金额
	OrderVolume int64   `json:"order_volume"` // 下单数量 张
	TradeVolume int64   `json:"trade_volume"` // 成交数量 张
	Status      int64   `json:"status"`       // 订单状态 1-已下单 2-已成交 8-已撤单
	CreateAt    int64   `json:"create_at"`    // 下单时间
	FilledAt    int64   `json:"filled_at"`    // 成交时间
	CancelAt    int64   `json:"cancel_at"`    // 撤单时间

}

type SwapOrderConfig struct {
	ExchangeName string                   `json:"exchange_name"`
	RdsData      map[string]SwapOrderItem `json:"rds_data"`
}

// example
var SwapOrderExample = SwapOrderConfig{
	ExchangeName: "htx",
	RdsData: map[string]SwapOrderItem{
		"SPferwa1213": {},
	},
}

func (c *SwapOrderConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_order_config", c.ExchangeName)
}

func (c *SwapOrderConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}
func (c *SwapOrderConfig) Keys() (all []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SwapOrderConfig) GetAll() (all map[string]SwapOrderItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SwapOrderItem{}
	for k, v := range res {
		item := SwapOrderItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SwapOrderConfig) Vals() (all []SwapOrderItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = []SwapOrderItem{}
	for _, v := range res {
		item := SwapOrderItem{}
		json.Unmarshal([]byte(v), &item)
		all = append(all, item)
	}
	return all, err
}
func (c *SwapOrderConfig) Has(orderId string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), orderId).Result()
	return
}
func (c *SwapOrderConfig) Get(orderId string) (value SwapOrderItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), orderId).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SwapOrderConfig) Set(orderId string, value SwapOrderItem) (err error) {
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

func (c *SwapOrderConfig) Del(orderId string) (err error) {
	err = redisDB.HDel(context.Background(), c.RdsName(), orderId).Err()
	if err != nil {
		return
	}
	return nil
}
