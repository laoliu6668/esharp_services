package esharp_services

import (
	"context"
	"encoding/json"
	"fmt"
)

type HedgeSymbolConfig struct {
	Exchange string           `json:"exchange"`
	RdsData  HedgeSymbolDatas `json:"rds_data"`
}

type HedgeSymbolDatas struct {
	Spot []string `json:"spot"`
	Swap []string `json:"swap"`
}

// example
var HedgeSymbolConfigExample = HedgeSymbolConfig{
	Exchange: "htx",
	RdsData: HedgeSymbolDatas{
		Spot: []string{"BTC", "ETH"},
		Swap: []string{"ETH", "DOT", "LINK"},
	},
}

func (c HedgeSymbolConfig) RdsName() string {
	return fmt.Sprintf("%s_hedge_symbol_config", c.Exchange)
}

func (c HedgeSymbolConfig) Init() (err error) {
	err = c.SetSpot(c.RdsData.Spot)
	if err != nil {
		return
	}
	err = c.SetSwap(c.RdsData.Swap)
	if err != nil {
		return
	}
	return nil
}

func (c HedgeSymbolConfig) GetSpot() (all []string, err error) {
	return c.Get("spot")
}
func (c HedgeSymbolConfig) GetSwap() (all []string, err error) {
	return c.Get("swap")
}
func (c HedgeSymbolConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c HedgeSymbolConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}

func (c HedgeSymbolConfig) Get(key string) (value []string, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		err = err1
		return
	}
	err = json.Unmarshal([]byte(ret), &value)
	return
}
func (c HedgeSymbolConfig) SetSpot(value []string) (err error) {
	return c.Set("spot", value)
}
func (c HedgeSymbolConfig) SetSwap(value []string) (err error) {
	return c.Set("swap", value)
}

func (c HedgeSymbolConfig) Set(key string, value []string) (err error) {
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
