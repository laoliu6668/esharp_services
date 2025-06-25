package esharp_services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type FundingHedgeSchemaItem struct {
	Id     string `json:"id"`     //  方案编号
	Status bool   `json:"status"` //  运行状态: on-运行中 off-已停止

	OpenLock  bool   `json:"open_lock"`  // 开仓行为锁: on-锁定 off-解锁
	CloseLock bool   `json:"close_lock"` // 平仓行为锁: on-锁定 off-解锁
	Symbol    string `json:"symbol"`     // c 对冲币对

	LongExchange      string `json:"long_exchange"`      // c 多仓交易所
	LongType          string `json:"long_type"`          // c 多仓类型: spot-现货合约 swap-永续合约
	LongSymbolConfigs string `json:"long_symbol_config"` // c 多仓币对配置

	ShortExchange      string `json:"short_exchange"`       // c 空仓交易所
	ShortType          string `json:"short_type"`           // c 空仓类型: swap-永续合约
	ShortSymbolConfigs string `json:"short_symbol_configs"` // c 空仓币对配置
	// group end 币对配置
	OpenRate                float64 `json:"open_rate"`                  // * 开仓差率
	CloseRate               float64 `json:"close_rate"`                 // * 平仓差率
	ShortPositionValueLimit float64 `json:"short_position_value_limit"` // * 空仓仓位持仓金额
	SingleOrderValue        float64 `json:"single_order_value"`         // * 空仓订单单笔金额(预计)
	MinOrderVolumeRate      float64 `json:"min_order_volume_rate"`      // * 最小下单量比 default:50(%)

	Totals string `json:"totals"` // 统计信息

	CreatedAt int64 `json:"created_at"` // 创建时间
}

type FundingHedgeSchemaConfig struct {
	RedisDB *redis.Client
}

func (c FundingHedgeSchemaConfig) RdsName(long_exchange, short_exchange, symbol string) string {
	return fmt.Sprintf("%s_%s_%s", long_exchange, short_exchange, symbol)
}

func (c FundingHedgeSchemaConfig) Keys() ([]string, error) {
	return c.RedisDB.Keys(context.Background(), "*").Result()
}
func (c FundingHedgeSchemaConfig) Add(long_exchange, long_type, short_exchange, short_type, symbol string) (id string, err error) {
	rdsName := c.RdsName(long_exchange, short_exchange, symbol)
	has, err := c.HasSameExSymbol(long_exchange, short_exchange, symbol)
	if err != nil {
		return "", err
	}
	if has {
		return "", errors.New("already exists")
	}

	// 复制多仓币对配置信息
	var longExchangeSymbolConfig any
	switch long_type {
	case "spot":
		spotSymbolItem, err := (&SpotSymbolConfig{
			Exchange: long_exchange,
		}).Get(symbol)
		if err != nil {
			return "", fmt.Errorf("spot symbol config error: %s", err)
		}
		longExchangeSymbolConfig = spotSymbolItem
	case "swap":
		swapSymbolItem, err := (&SwapSymbolConfig{
			Exchange: long_exchange,
		}).Get(symbol)
		if err != nil {
			return "", fmt.Errorf("spot symbol config error: %s", err)
		}
		longExchangeSymbolConfig = swapSymbolItem
	default:
		return "", fmt.Errorf("unknown long type: %s", long_type)
	}
	// 复制空仓币对配置信息
	var shortExchangeSymbolConfig any
	if short_type == "swap" {
		swapSymbolItem, err := (&SwapSymbolConfig{
			Exchange: long_exchange,
		}).Get(symbol)
		if err != nil {
			return "", fmt.Errorf("spot symbol config error: %s", err)
		}
		shortExchangeSymbolConfig = swapSymbolItem
	} else {
		return "", fmt.Errorf("unknown short type: %s", short_type)
	}

	id = rdsName

	err = c.RedisDB.HSet(context.Background(), rdsName, "id", id).Err()
	if err != nil {
		return "", fmt.Errorf("redis set id error: %s", err)
	}
	c.set(long_exchange, short_exchange, symbol, "symbol", symbol)
	c.set(long_exchange, short_exchange, symbol, "long_exchange", long_exchange)
	c.set(long_exchange, short_exchange, symbol, "short_exchange", short_exchange)
	c.set(long_exchange, short_exchange, symbol, "long_type", long_type)
	c.set(long_exchange, short_exchange, symbol, "short_type", short_type)
	longExchangeSymbolConfigString, _ := json.Marshal(longExchangeSymbolConfig)
	c.set(long_exchange, short_exchange, symbol, "long_symbol_configs", string(longExchangeSymbolConfigString))
	shortExchangeSymbolConfigString, _ := json.Marshal(shortExchangeSymbolConfig)
	c.set(long_exchange, short_exchange, symbol, "short_symbol_configs", string(shortExchangeSymbolConfigString))
	// default value
	c.setBool(long_exchange, short_exchange, symbol, "status", false)
	c.setBool(long_exchange, short_exchange, symbol, "open_lock", false)
	c.setBool(long_exchange, short_exchange, symbol, "close_lock", false)
	c.setFloat(long_exchange, short_exchange, symbol, "open_rate", 0)
	c.setFloat(long_exchange, short_exchange, symbol, "close_rate", 0)

	c.setFloat(long_exchange, short_exchange, symbol, "single_order_value", 0)
	c.setFloat(long_exchange, short_exchange, symbol, "min_order_volume_rate", 50)
	c.setFloat(long_exchange, short_exchange, symbol, "position_value_limit", 0)

	c.setInt(long_exchange, short_exchange, symbol, "created_at", time.Now().Unix())

	// if rabbitmq.C
	if schemaCh != nil {
		item := HedgeSchemaMQ{
			Action: "create",
			Data: FundingHedgeSchemaItem{
				Id:                id,
				LongExchange:      long_exchange,
				LongType:          long_type,
				LongSymbolConfigs: string(longExchangeSymbolConfigString),

				ShortExchange:      short_exchange,
				ShortType:          short_type,
				ShortSymbolConfigs: string(shortExchangeSymbolConfigString),

				Symbol: symbol,

				MinOrderVolumeRate: 50,
				Status:             false,
				OpenLock:           false,
				CloseLock:          false,
				CreatedAt:          time.Now().Unix(),
			},
		}
		buf, _ := json.Marshal(item)
		PublishToHedgeSchema(buf)
	}
	return
}

func (c FundingHedgeSchemaConfig) Vals() (allVals []FundingHedgeSchemaItem, err error) {
	keys, err := c.Keys()
	if err != nil {
		return
	}
	allVals = []FundingHedgeSchemaItem{}
	for _, key := range keys {
		keys := strings.Split(key, "_")
		if len(keys) != 3 {
			continue
		}
		value, _ := c.Get(keys[0], keys[1], keys[2])
		allVals = append(allVals, value)
	}
	return
}

// 是否存在同交易所币对
func (c FundingHedgeSchemaConfig) HasSameExSymbol(long_exchange, short_exchange string, symbol string) (has bool, err error) {
	list, err := c.Vals()
	if err != nil {
		return false, err
	}
	for _, v := range list {
		if v.ShortExchange == short_exchange && v.Symbol == symbol {
			return true, nil
		}
		if v.LongExchange == long_exchange && v.Symbol == symbol {
			return true, nil
		}
	}
	return false, nil
}

func (c FundingHedgeSchemaConfig) Has(long_exchange string, short_exchange string, symbol string) (has bool, err error) {
	hasi, err := c.RedisDB.Exists(context.Background(), c.RdsName(long_exchange, short_exchange, symbol)).Result()
	if err != nil {
		return false, err
	}
	if hasi == 0 {
		return false, nil
	}
	return true, nil
}
func (c FundingHedgeSchemaConfig) Get(long_exchange, short_exchange, symbol string) (item FundingHedgeSchemaItem, err error) {
	has, _ := c.Has(long_exchange, short_exchange, symbol)
	if !has {
		return item, errors.New("not found")
	}
	itemVals, err1 := c.RedisDB.HGetAll(context.Background(), c.RdsName(long_exchange, short_exchange, symbol)).Result()
	if err1 != nil {
		err = err1
		return
	}
	item = FundingHedgeSchemaItem{}
	ts := reflect.TypeOf(item)
	vs := reflect.ValueOf(&item)
	for i := 0; i < ts.NumField(); i++ {
		field := ts.Field(i)
		tagJson := field.Tag.Get("json")
		for k, v := range itemVals {
			if k == tagJson {
				tn := field.Type.Kind().String()
				vf := vs.Elem().Field(i)
				switch tn {
				case "bool":
					vf.SetBool(toBool(v))
				case "float64":
					vf.SetFloat(toFloat(v))
				case "string":
					vf.SetString(v)
				case "int64":
					vf.SetInt(toInt(v))
				case "any":
					vf.Set(reflect.ValueOf(toObject(v))) //TODO
				}
				break
			}
		}
	}
	return
}
func (c FundingHedgeSchemaConfig) Set(long_exchange, short_exchange, symbol, field, value string, originVal any) (err error) {
	err = c.set(long_exchange, short_exchange, symbol, field, value)
	if err != nil {
		return err
	}
	up := HedgeSchemaMQ{
		Action: "update",
		Data: map[string]any{
			"id":             c.RdsName(long_exchange, short_exchange, symbol),
			"long_exchange":  long_exchange,
			"short_exchange": short_exchange,
			"symbol":         symbol,
			field:            originVal,
		},
	}
	buf, _ := json.Marshal(up)
	PublishToHedgeSchema(buf)
	return nil
}
func (c FundingHedgeSchemaConfig) set(long_exchange, short_exchange, symbol, field, value string) (err error) {
	key := c.RdsName(long_exchange, short_exchange, symbol)
	has, err := c.RedisDB.Exists(context.Background(), key).Result()
	if err != nil {
		return fmt.Errorf("redis Exists error: %s", err)
	}
	if has == 0 {
		// not has
		return fmt.Errorf("redis key not exists: %s", key)
	}
	err = c.RedisDB.HSet(context.Background(), key, field, value).Err()
	if err != nil {
		return
	}
	return nil
}
func (c FundingHedgeSchemaConfig) setObject(long_exchange, short_exchange, symbol, field string, value any) (err error) {
	buf, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.set(long_exchange, short_exchange, symbol, field, string(buf))
}

func (c FundingHedgeSchemaConfig) setInt(long_exchange, short_exchange, symbol, field string, value int64) (err error) {
	return c.set(long_exchange, short_exchange, symbol, field, strconv.FormatInt(value, 10))
}

func (c FundingHedgeSchemaConfig) setFloat(long_exchange, short_exchange, symbol, field string, value float64) (err error) {
	return c.set(long_exchange, short_exchange, symbol, field, floatTo(value))
}
func (c FundingHedgeSchemaConfig) setBool(long_exchange, short_exchange, symbol, field string, value bool) (err error) {
	return c.set(long_exchange, short_exchange, symbol, field, boolTo(value))
}

func (c FundingHedgeSchemaConfig) SetInt(long_exchange, short_exchange, symbol, field string, value int64) (err error) {
	return c.Set(long_exchange, short_exchange, symbol, field, strconv.FormatInt(value, 10), value)
}

func (c FundingHedgeSchemaConfig) SetFloat(long_exchange, short_exchange, symbol, field string, value float64) (err error) {
	return c.Set(long_exchange, short_exchange, symbol, field, floatTo(value), value)
}
func (c FundingHedgeSchemaConfig) SetBool(long_exchange, short_exchange, symbol, field string, value bool) (err error) {
	return c.Set(long_exchange, short_exchange, symbol, field, boolTo(value), value)
}

// 运行中的方案不允许被删除，有空仓持仓量的不能被删除
func (c FundingHedgeSchemaConfig) Del(long_exchange, short_exchange, symbol string) (err error) {
	key := c.RdsName(long_exchange, short_exchange, symbol)
	item, err := c.Get(long_exchange, short_exchange, symbol)
	if err != nil {
		return err
	}
	if item.Status {
		return fmt.Errorf("running schema can't be deleted: %s", key)
	}
	sp := SwapPositionConfig{
		Exchange: item.ShortExchange,
	}

	swapPositionItem, _ := sp.Get(item.Symbol)
	var position = swapPositionItem.SellVolume
	if position > 0 {
		return fmt.Errorf("short position volume limit schema can't be deleted: %s", key)
	}
	err = c.RedisDB.Del(context.Background(), key).Err()
	if err != nil {
		return err
	}
	msg := HedgeSchemaMQ{
		Action: "delete",
		Data: map[string]string{
			"id":             c.RdsName(long_exchange, short_exchange, symbol),
			"long_exchange":  long_exchange,
			"short_exchange": short_exchange,
			"symbol":         symbol,
		},
	}
	buf, _ := json.Marshal(msg)
	PublishToFundingHedgeSchema(buf)
	return nil
}

func toObject(val string) (obj any) {
	err := json.Unmarshal([]byte(val), &obj)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	return obj
}
