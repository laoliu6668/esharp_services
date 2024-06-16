package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type SwapSymbolItem struct {
	Symbol                string  `json:"symbol"`                   // 币对
	ContractSize          float64 `json:"contract_size"`            // 合约面值
	MaxBuyPositionVolume  int64   `json:"max_buy_position_volume"`  // 多仓持仓上限(张)
	MaxSellPositionVolume int64   `json:"max_sell_position_volume"` // 空仓持仓上限(张)
	MaxOpenOrderVolume    int64   `json:"max_open_order_volume"`    // 开仓单笔下单上限(张)
	MaxCloseOrderVolume   int64   `json:"max_close_order_volume"`   // 平仓单笔下单上限(张)
}

type SwapSymbolConfig struct {
	Exchange string                    `json:"exchange"`
	RdsData  map[string]SwapSymbolItem `json:"rds_data"`
}

// example
var SwapSymbolConfigExample = SwapSymbolConfig{
	Exchange: "htx",
	RdsData: map[string]SwapSymbolItem{
		"BTC": {},
	},
}

func (c *SwapSymbolConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_symbol_config", c.Exchange)
}

func (c *SwapSymbolConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SwapSymbolConfig) GetAll() (all map[string]SwapSymbolItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SwapSymbolItem{}
	for k, v := range res {
		item := SwapSymbolItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SwapSymbolConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}

func (c *SwapSymbolConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SwapSymbolConfig) Vals() (vals []SwapSymbolItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SwapSymbolItem{}
	for _, str := range strList {
		item := SwapSymbolItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c *SwapSymbolConfig) Get(key string) (value SwapSymbolItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}

func (c *SwapSymbolConfig) Set(key string, value SwapSymbolItem) (err error) {
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
