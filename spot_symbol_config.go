package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SpotSymbolItem struct {
	Symbol           string  `json:"symbol"`             // 币对
	MinOrderVolume   float64 `json:"min_order_volume"`   // 最小下单数量
	MaxOrderVolume   float64 `json:"max_order_volume"`   // 最大下单数量
	MinOrderAmount   float64 `json:"min_order_amount"`   // 最小下单金额
	TradeVolumePoint int64   `json:"trade_volume_point"` // 交易数量精度
	TradePricePoint  int64   `json:"trade_price_point"`  // 交易价格精度
	TradeAmountPoint int64   `json:"trade_amount_point"` // 交易金额精度
}

type SpotSymbolConfig struct {
	Exchange string                    `json:"exchange"`
	RdsData  map[string]SpotSymbolItem `json:"rds_data"`
}

// example
var SpotSymbolConfigExample = SpotSymbolConfig{
	Exchange: "htx",
	RdsData: map[string]SpotSymbolItem{
		"BTC": {},
	},
}

func (c *SpotSymbolConfig) RdsName() string {
	return fmt.Sprintf("%s_spot_symbol_config", c.Exchange)
}

func (c *SpotSymbolConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SpotSymbolConfig) GetAll() (all map[string]SpotSymbolItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SpotSymbolItem{}
	for k, v := range res {
		item := SpotSymbolItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SpotSymbolConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *SpotSymbolConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SpotSymbolConfig) Vals() (vals []SpotSymbolItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SpotSymbolItem{}
	for _, str := range strList {
		item := SpotSymbolItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c *SpotSymbolConfig) Get(key string) (value SpotSymbolItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SpotSymbolConfig) Set(key string, value SpotSymbolItem) (err error) {
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
