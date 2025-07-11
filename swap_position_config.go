package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type SwapPositionItem struct {
	Exchange   string  `json:"exchange"`    // 交易所
	Symbol     string  `json:"symbol"`      // 币对
	BuyVolume  float64 `json:"buy_volume"`  // 多仓持仓数量
	SellVolume float64 `json:"sell_volume"` // 空仓持仓数量
	UpdateAt   float64 `json:"update_at"`   // 更新时间 13位时间戳
}

type SwapPositionConfig struct {
	Exchange string                      `json:"exchange"`
	RdsData  map[string]SwapPositionItem `json:"rds_data"`
}

// example
// var SwapPositionConfigExample = SwapPositionConfig{
// 	Exchange: "htx",
// 	RdsData: map[string]SwapPositionItem{
// 		"BTC": {},
// 	},
// }

func (c SwapPositionConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_position_config", c.Exchange)
}

func (c SwapPositionConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c SwapPositionConfig) GetAll() (all map[string]SwapPositionItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SwapPositionItem{}
	for k, v := range res {
		item := SwapPositionItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c SwapPositionConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}

func (c SwapPositionConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c SwapPositionConfig) Vals() (vals []SwapPositionItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SwapPositionItem{}
	for _, str := range strList {
		item := SwapPositionItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c SwapPositionConfig) Get(key string) (value SwapPositionItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		if err1 == redis.Nil {
			return SwapPositionItem{
				Exchange: c.Exchange,
				Symbol:   key,
			}, nil
		}
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c SwapPositionConfig) Del(key string) (err error) {
	err = redisDB.HDel(context.Background(), c.RdsName(), key).Err()
	if err != nil {
		return
	}
	return nil
}

func (c SwapPositionConfig) Set(key string, value SwapPositionItem) (err error) {
	value.Exchange = c.Exchange
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
