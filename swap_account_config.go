package esharp_services

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"

	"github.com/laoliu6668/esharp_services/util"
	"github.com/laoliu6668/esharp_services/util/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type SwapAccountConfig struct {
	Exchange string                     `json:"exchange"`
	RdsData  map[string]SwapAccountItem `json:"rds_data"`
}

type SwapAccountItem struct {
	Exchange    string  `json:"exchange"`
	Symbol      string  `json:"symbol"`
	FreeBalance float64 `json:"free_balance"` // 可用保金
	LockBalance float64 `json:"lock_balance"` // 冻结保金
	LiquidPrice float64 `json:"liquid_price"` // 强平价格
	MarginRatio float64 `json:"margin_ratio"` // 保证金率
	UpdateAt    float64 `json:"update_at"`
}

// example
var SwapAccountConfigExample = SwapAccountConfig{
	Exchange: "htx",
	RdsData: map[string]SwapAccountItem{
		"BTC": {},
	},
}

func (c *SwapAccountConfig) RdsName() string {
	return fmt.Sprintf("%s_swap_account_config", c.Exchange)
}

func (c *SwapAccountConfig) Init() (err error) {
	for k, v := range c.RdsData {
		err = c.Set(k, v)
		if err != nil {
			return
		}
	}
	return nil
}

func (c *SwapAccountConfig) GetAll() (all map[string]SwapAccountItem, err error) {
	res, err := redisDB.HGetAll(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	all = map[string]SwapAccountItem{}
	for k, v := range res {
		item := SwapAccountItem{}
		json.Unmarshal([]byte(v), &item)
		all[k] = item
	}
	return all, err
}
func (c *SwapAccountConfig) Has(key string) (has bool, err error) {
	has, err = redisDB.HExists(context.Background(), c.RdsName(), key).Result()
	return
}
func (c *SwapAccountConfig) Get(key string) (value SwapAccountItem, err error) {
	ret, err1 := redisDB.HGet(context.Background(), c.RdsName(), key).Result()
	if err1 != nil {
		if err1 == redis.Nil {
			return SwapAccountItem{
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
func (c *SwapAccountConfig) Keys() (keys []string, err error) {
	return redisDB.HKeys(context.Background(), c.RdsName()).Result()
}
func (c *SwapAccountConfig) Vals() (vals []SwapAccountItem, err error) {
	strList, err := redisDB.HVals(context.Background(), c.RdsName()).Result()
	if err != nil {
		return
	}
	vals = []SwapAccountItem{}
	for _, str := range strList {
		item := SwapAccountItem{}
		json.Unmarshal([]byte(str), &item)
		vals = append(vals, item)
	}
	return
}
func (c *SwapAccountConfig) Set(key string, value SwapAccountItem) (err error) {
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
func (c *SwapAccountConfig) SetFreeBalance(key string, amount float64) (err error) {

	vals, err := c.Get(key)
	if err != nil {
		return
	}
	vals.FreeBalance = amount
	return c.Set(key, vals)
}

// 变动金额
func (c *SwapAccountConfig) ChangeFreeBalanceAndPushMQ(symbol string, changeAmount float64) (err error) {

	vals, err := c.Get(symbol)
	if err != nil {
		return
	}
	if vals.UpdateAt >= util.GetTimeFloat() {
		return
	}
	vals.FreeBalance += changeAmount
	vals.UpdateAt = util.GetTimeFloat()
	c.Set(symbol, vals)

	ch, err := rabbitmq.NewChannel()
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	err = ch.ExchangeDeclare(
		c.RdsName(), // name
		"topic",     // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	input, _ := json.Marshal(vals)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	defer gw.Close()
	_, err = gw.Write(input)
	if err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	schemaCh.Publish(
		c.RdsName(), // exchange
		symbol,      // routing key
		false,       // mandatory
		false,       // immediate
		amqp091.Publishing{
			Body: buf.Bytes(),
		},
	)
	return nil
}
