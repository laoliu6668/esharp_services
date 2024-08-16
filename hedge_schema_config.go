package esharp_services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	SpotMoreSwapLess = "spot_more_swap_less" // 现多期空
	SpotLessSwapMore = "spot_less_swap_more" // 现空期多
)

type HedgeSchemaItem struct {
	Id     string `json:"id"`     //  方案编号
	Status bool   `json:"status"` //  运行状态: on-运行中 off-已停止

	OpenLock     bool   `json:"open_lock"`     // 开仓行为锁: on-锁定 off-解锁
	CloseLock    bool   `json:"close_lock"`    // 平仓行为锁: on-锁定 off-解锁
	Symbol       string `json:"symbol"`        // c 对冲币对
	SpotExchange string `json:"spot_exchange"` // c 现货交易所
	SwapExchange string `json:"swap_exchange"` // c 期货交易所

	// group start 币对配置
	MinOrderVolume   float64 `json:"min_order_volume"`   // 最小下单数量
	MinOrderAmount   float64 `json:"min_order_amount"`   // 最小下单金额
	TradeVolumePoint int64   `json:"trade_volume_point"` // 交易数量精度
	TradePricePoint  int64   `json:"trade_price_point"`  // 交易价格精度
	TradeAmountPoint int64   `json:"trade_amount_point"` // 交易金额精度

	ContractSize          float64 `json:"contract_size"`            // 合约面值
	MaxBuyPositionVolume  float64 `json:"max_buy_position_volume"`  // 多仓持仓上限(数量)
	MaxSellPositionVolume float64 `json:"max_sell_position_volume"` // 空仓持仓上限(数量)
	MaxOpenOrderVolume    float64 `json:"max_open_order_volume"`    // 开仓单笔下单上限(数量)
	MaxCloseOrderVolume   float64 `json:"max_close_order_volume"`   // 平仓单笔下单上限(数量)
	// group end 币对配置

	Models    string  `json:"models"`     // c 对冲方案: spot_more_swap_less-现多期空 spot_less_swap_more-现空期多
	OpenRate  float64 `json:"open_rate"`  // * 开仓差率
	CloseRate float64 `json:"close_rate"` // * 平仓差率
	// SingleOrderVolume   int64   `json:"single_order_volume"`   // * 期货订单单笔张数(张)
	PositionValueLimit float64 `json:"position_value_limit"` // * 期货仓位持仓金额
	SingleOrderValue   float64 `json:"single_order_value"`   // * 期货订单单笔金额(预计)

	MinOrderVolumeRate float64 `json:"min_order_volume_rate"` // * 最小下单量比 default:50(%)

	// 现空期多
	SpotTotalBuyVolume      float64 `json:"spot_total_buy_volume"`       // 现货累积买入数量
	SpotTotalBuyValue       float64 `json:"spot_total_buy_value"`        // 现货累积买入花费
	SpotTotalSellVolume     float64 `json:"spot_total_sell_volume"`      // 现货累积卖出数量
	SpotTotalSellValue      float64 `json:"spot_total_sell_value"`       // 现货累积卖出金额
	SwapTotalSellOpenVolume float64 `json:"swap_total_sell_open_volume"` // 期货累积开空数量
	SwapTotalSellOpenValue  float64 `json:"swap_total_sell_open_value"`  // 期货累积开空花费
	SwapTotalBuyCloseVolume float64 `json:"swap_total_buy_close_volume"` // 期货累积平空数量
	SwapTotalBuyCloseValue  float64 `json:"swap_total_buy_close_value"`  // 期货累积平空金额

	// 现多期空
	SpotTotalBorrowVolume    float64 `json:"spot_total_borrow_volume"`     // 现货累积借币数量
	SpotTotalBorrowValue     float64 `json:"spot_total_borrow_value"`      // 现货累积借币花费
	SpotTotalReturnVolume    float64 `json:"spot_total_return_volume"`     // 现货累积还币数量
	SpotTotalReturnValue     float64 `json:"spot_total_return_value"`      // 现货累积还币金额
	SwapTotalBuyOpenVolume   float64 `json:"swap_total_buy_volume"`        // 期货累积买入开多数量
	SwapTotalBuyOpenValue    float64 `json:"swap_total_buy_open_value"`    // 期货累积买入开多花费
	SwapTotalSellCloseVolume float64 `json:"swap_total_sell_close_volume"` // 期货累积卖出平多数量
	SwapTotalSellCloseValue  float64 `json:"swap_total_sell_close_value"`  // 期货累积卖出平多金额

	RelOpenRate  float64 `json:"rel_open_rate"`  // 实开差率
	RelCloseRate float64 `json:"rel_close_rate"` // 实平差率
	RelPl        float64 `json:"rel_pl"`         // 实际盈亏

	CreatedAt int64 `json:"created_at"` // 创建时间
}

type HedgeSchemaMQ struct {
	Action string `json:"action"`
	Data   any    `json:"data"`
}

type HedgeSchemaConfig struct {
}

func (c HedgeSchemaConfig) RdsName(spot_exchange, swap_exchange, symbol string) string {
	return fmt.Sprintf("%s_%s_%s", spot_exchange, swap_exchange, symbol)
}

func (c HedgeSchemaConfig) Keys() ([]string, error) {
	return redisDB_H.Keys(context.Background(), "*").Result()
}
func (c HedgeSchemaConfig) Add(spot_exchange, swap_exchange, symbol, model string) (id string, err error) {
	rdsName := c.RdsName(spot_exchange, swap_exchange, symbol)
	has, err := c.HasSameExSymbol(spot_exchange, swap_exchange, symbol)
	if err != nil {
		return "", err
	}
	if has {
		return "", errors.New("already exists")
	}
	if model != SpotMoreSwapLess && model != SpotLessSwapMore {
		return "", errors.New("model error")
	}
	id = rdsName
	spotSymbolItem, err := (&SpotSymbolConfig{
		Exchange: spot_exchange,
	}).Get(symbol)
	if err != nil {
		return "", fmt.Errorf("spot symbol config error: %s", err)
	}
	tradeVolumePoint := spotSymbolItem.TradeVolumePoint
	tradePricePoint := spotSymbolItem.TradePricePoint
	tradeAmountPoint := spotSymbolItem.TradeAmountPoint
	minOrderVolume := spotSymbolItem.MinOrderVolume
	minOrderAmount := spotSymbolItem.MinOrderAmount
	// 获取期货币对配置
	swapSymbolItem, err := SwapSymbolConfig{Exchange: swap_exchange}.Get(symbol)
	if err != nil {
		return "", fmt.Errorf("swapExchange's symbol config error: %s", err)
	}
	// 有面值则不需要精度
	if swapSymbolItem.ContractSize != 0 {
		if swapSymbolItem.TradeVolumePoint < tradeVolumePoint {
			tradeVolumePoint = swapSymbolItem.TradeVolumePoint
		}
		if swapSymbolItem.TradePricePoint < tradePricePoint {
			tradePricePoint = swapSymbolItem.TradePricePoint
		}
		if swapSymbolItem.TradeAmountPoint < tradeAmountPoint {
			tradeAmountPoint = swapSymbolItem.TradeAmountPoint
		}
		if swapSymbolItem.MinOrderVolume > minOrderVolume {
			minOrderVolume = swapSymbolItem.MinOrderVolume
		}
		if swapSymbolItem.MinOrderAmount > minOrderAmount {
			minOrderAmount = swapSymbolItem.MinOrderAmount
		}
	}

	err = redisDB_H.HSet(context.Background(), rdsName, "id", id).Err()
	if err != nil {
		return "", fmt.Errorf("redis set id error: %s", err)
	}
	c.set(spot_exchange, swap_exchange, symbol, "symbol", symbol)
	c.set(spot_exchange, swap_exchange, symbol, "spot_exchange", spot_exchange)
	c.set(spot_exchange, swap_exchange, symbol, "swap_exchange", swap_exchange)
	c.set(spot_exchange, swap_exchange, symbol, "models", model)

	c.setFloat(spot_exchange, swap_exchange, symbol, "min_order_volume", minOrderVolume)   // 取最大
	c.setFloat(spot_exchange, swap_exchange, symbol, "min_order_amount", minOrderAmount)   // 取最大
	c.setInt(spot_exchange, swap_exchange, symbol, "trade_volume_point", tradeVolumePoint) // 取最小
	c.setInt(spot_exchange, swap_exchange, symbol, "trade_price_point", tradePricePoint)   // 取最小
	c.setInt(spot_exchange, swap_exchange, symbol, "trade_amount_point", tradeAmountPoint) // 取最小

	c.setFloat(spot_exchange, swap_exchange, symbol, "contract_size", swapSymbolItem.ContractSize)
	c.setFloat(spot_exchange, swap_exchange, symbol, "max_buy_position_volume", swapSymbolItem.MaxBuyPositionVolume)
	c.setFloat(spot_exchange, swap_exchange, symbol, "max_sell_position_volume", swapSymbolItem.MaxSellPositionVolume)
	c.setFloat(spot_exchange, swap_exchange, symbol, "max_open_order_volume", swapSymbolItem.MaxOpenOrderVolume)
	c.setFloat(spot_exchange, swap_exchange, symbol, "max_close_order_volume", swapSymbolItem.MaxCloseOrderVolume)

	// default value
	c.setBool(spot_exchange, swap_exchange, symbol, "status", false)
	c.setBool(spot_exchange, swap_exchange, symbol, "open_lock", false)
	c.setBool(spot_exchange, swap_exchange, symbol, "close_lock", false)
	c.setFloat(spot_exchange, swap_exchange, symbol, "open_rate", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "close_rate", 0)
	// c.setInt(spot_exchange, swap_exchange, symbol, "single_order_volume", 0)
	// c.setFloat(spot_exchange, swap_exchange, symbol, "position_volume_limit", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "single_order_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "min_order_volume_rate", 50)
	c.setFloat(spot_exchange, swap_exchange, symbol, "position_value_limit", 0)

	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_buy_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_buy_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_sell_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_sell_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_sell_open_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_sell_open_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_buy_close_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_buy_close_value", 0)

	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_borrow_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_borrow_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_return_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "spot_total_return_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_buy_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_buy_open_value", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_sell_close_volume", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "swap_total_sell_close_value", 0)

	c.setFloat(spot_exchange, swap_exchange, symbol, "rel_open_rate", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "rel_close_rate", 0)
	c.setFloat(spot_exchange, swap_exchange, symbol, "rel_pl", 0)
	c.setInt(spot_exchange, swap_exchange, symbol, "created_at", time.Now().Unix())

	// if rabbitmq.C
	if schemaCh != nil {
		item := HedgeSchemaMQ{
			Action: "create",
			Data: HedgeSchemaItem{
				Id:                    id,
				SpotExchange:          spot_exchange,
				SwapExchange:          swap_exchange,
				Symbol:                symbol,
				Models:                model,
				MinOrderVolume:        minOrderVolume,
				MinOrderAmount:        minOrderAmount,
				TradeVolumePoint:      tradeVolumePoint,
				TradePricePoint:       tradePricePoint,
				TradeAmountPoint:      tradeAmountPoint,
				ContractSize:          swapSymbolItem.ContractSize,
				MaxBuyPositionVolume:  swapSymbolItem.MaxBuyPositionVolume,
				MaxSellPositionVolume: swapSymbolItem.MaxSellPositionVolume,
				MaxOpenOrderVolume:    swapSymbolItem.MaxOpenOrderVolume,
				MaxCloseOrderVolume:   swapSymbolItem.MaxCloseOrderVolume,
				MinOrderVolumeRate:    50,
				Status:                false,
				OpenLock:              false,
				CloseLock:             false,
				CreatedAt:             time.Now().Unix(),
			},
		}
		buf, _ := json.Marshal(item)
		PublishToHedgeSchema(buf)
	}
	return
}

func (c HedgeSchemaConfig) Vals() (allVals []HedgeSchemaItem, err error) {
	keys, err := c.Keys()
	if err != nil {
		return
	}
	allVals = []HedgeSchemaItem{}
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
func (c HedgeSchemaConfig) HasSameExSymbol(spot_exchange, swap_exchange string, symbol string) (has bool, err error) {
	list, err := c.Vals()
	if err != nil {
		return false, err
	}
	for _, v := range list {
		if v.SwapExchange == swap_exchange && v.Symbol == symbol {
			return true, nil
		}
		if v.SpotExchange == spot_exchange && v.Symbol == symbol {
			return true, nil
		}
	}
	return false, nil
}

func (c HedgeSchemaConfig) Has(spot_exchange string, swap_exchange string, symbol string) (has bool, err error) {
	hasi, err := redisDB_H.Exists(context.Background(), c.RdsName(spot_exchange, swap_exchange, symbol)).Result()
	if err != nil {
		return false, err
	}
	if hasi == 0 {
		return false, nil
	}
	return true, nil
}
func (c HedgeSchemaConfig) Get(spot_exchange, swap_exchange, symbol string) (item HedgeSchemaItem, err error) {
	has, _ := c.Has(spot_exchange, swap_exchange, symbol)
	if !has {
		return item, errors.New("not found")
	}
	itemVals, err1 := redisDB_H.HGetAll(context.Background(), c.RdsName(spot_exchange, swap_exchange, symbol)).Result()
	if err1 != nil {
		err = err1
		return
	}
	item = HedgeSchemaItem{}
	for k, v := range itemVals {
		switch k {
		case "id":
			item.Id = v
		case "status":
			item.Status = toBool(v)
		case "open_lock":
			item.OpenLock = toBool(v)
		case "close_lock":
			item.CloseLock = toBool(v)
		case "symbol":
			item.Symbol = v
		case "spot_exchange":
			item.SpotExchange = v
		case "swap_exchange":
			item.SwapExchange = v
		case "min_order_volume":
			item.MinOrderVolume = toFloat(v)
		case "min_order_amount":
			item.MinOrderAmount = toFloat(v)
		case "trade_volume_point":
			item.TradeVolumePoint = toInt(v)
		case "trade_price_point":
			item.TradePricePoint = toInt(v)
		case "trade_amount_point":
			item.TradeAmountPoint = toInt(v)
		case "contract_size":
			item.ContractSize = toFloat(v)
		case "max_buy_position_volume":
			item.MaxBuyPositionVolume = toFloat(v)
		case "max_sell_position_volume":
			item.MaxSellPositionVolume = toFloat(v)
		case "max_open_order_volume":
			item.MaxOpenOrderVolume = toFloat(v)
		case "max_close_order_amount":
			item.MaxCloseOrderVolume = toFloat(v)
		case "models":
			item.Models = v
		case "open_rate":
			item.OpenRate = toFloat(v)
		case "close_rate":
			item.CloseRate = toFloat(v)
		case "single_order_value":
			item.SingleOrderValue = toFloat(v)
		case "min_order_volume_rate":
			item.MinOrderVolumeRate = toFloat(v)
		case "position_value_limit":
			item.PositionValueLimit = toFloat(v)
		case "spot_total_buy_volume":
			item.SpotTotalBuyVolume = toFloat(v)
		case "spot_total_buy_value":
			item.SpotTotalBuyValue = toFloat(v)
		case "spot_total_sell_volume":
			item.SpotTotalSellVolume = toFloat(v)
		case "spot_total_sell_value":
			item.SpotTotalSellValue = toFloat(v)
		case "swap_total_sell_open_volume":
			item.SwapTotalSellOpenVolume = toFloat(v)
		case "swap_total_sell_open_value":
			item.SwapTotalSellOpenValue = toFloat(v)
		case "swap_total_buy_close_volume":
			item.SwapTotalBuyCloseVolume = toFloat(v)
		case "swap_total_buy_close_value":
			item.SwapTotalBuyCloseValue = toFloat(v)
		case "spot_total_borrow_volume":
			item.SpotTotalBorrowVolume = toFloat(v)
		case "spot_total_borrow_value":
			item.SpotTotalBorrowValue = toFloat(v)
		case "spot_total_return_volume":
			item.SpotTotalReturnVolume = toFloat(v)
		case "spot_total_return_value":
			item.SpotTotalReturnValue = toFloat(v)
		case "swap_total_buy_volume":
			item.SwapTotalBuyOpenVolume = toFloat(v)
		case "swap_total_buy_open_value":
			item.SwapTotalBuyOpenValue = toFloat(v)
		case "swap_total_sell_close_volume":
			item.SwapTotalSellCloseVolume = toFloat(v)
		case "swap_total_sell_close_value":
			item.SwapTotalSellCloseValue = toFloat(v)
		case "rel_open_rate":
			item.RelOpenRate = toFloat(v)
		case "rel_close_rate":
			item.RelCloseRate = toFloat(v)
		case "rel_pl":
			item.RelPl = toFloat(v)
		case "created_at":
			item.CreatedAt = toInt(v)
		}

	}
	return
}
func (c HedgeSchemaConfig) Set(spot_exchange, swap_exchange, symbol, field, value string, originVal any) (err error) {
	err = c.set(spot_exchange, swap_exchange, symbol, field, value)
	if err != nil {
		return err
	}
	up := HedgeSchemaMQ{
		Action: "update",
		Data: map[string]any{
			"id":            c.RdsName(spot_exchange, swap_exchange, symbol),
			"spot_exchange": spot_exchange,
			"swap_exchange": swap_exchange,
			"symbol":        symbol,
			field:           originVal,
		},
	}
	buf, _ := json.Marshal(up)
	PublishToHedgeSchema(buf)
	return nil
}

func (c HedgeSchemaConfig) set(spot_exchange, swap_exchange, symbol, field, value string) (err error) {
	key := c.RdsName(spot_exchange, swap_exchange, symbol)
	has, err := redisDB_H.Exists(context.Background(), key).Result()
	if err != nil {
		return fmt.Errorf("redis Exists error: %s", err)
	}
	if has == 0 {
		// not has
		return fmt.Errorf("redis key not exists: %s", key)
	}
	err = redisDB_H.HSet(context.Background(), key, field, value).Err()
	if err != nil {
		return
	}
	return nil
}
func (c HedgeSchemaConfig) setInt(spot_exchange, swap_exchange, symbol, field string, value int64) (err error) {
	return c.set(spot_exchange, swap_exchange, symbol, field, strconv.FormatInt(value, 10))
}

func (c HedgeSchemaConfig) setFloat(spot_exchange, swap_exchange, symbol, field string, value float64) (err error) {
	return c.set(spot_exchange, swap_exchange, symbol, field, floatTo(value))
}
func (c HedgeSchemaConfig) setBool(spot_exchange, swap_exchange, symbol, field string, value bool) (err error) {
	return c.set(spot_exchange, swap_exchange, symbol, field, boolTo(value))
}

func (c HedgeSchemaConfig) SetInt(spot_exchange, swap_exchange, symbol, field string, value int64) (err error) {
	return c.Set(spot_exchange, swap_exchange, symbol, field, strconv.FormatInt(value, 10), value)
}

func (c HedgeSchemaConfig) SetFloat(spot_exchange, swap_exchange, symbol, field string, value float64) (err error) {
	return c.Set(spot_exchange, swap_exchange, symbol, field, floatTo(value), value)
}
func (c HedgeSchemaConfig) SetBool(spot_exchange, swap_exchange, symbol, field string, value bool) (err error) {
	return c.Set(spot_exchange, swap_exchange, symbol, field, boolTo(value), value)
}

// 运行中的方案不允许被删除，有期货持仓量的不能被删除
func (c HedgeSchemaConfig) Del(spot_exchange, swap_exchange, symbol string) (err error) {
	key := c.RdsName(spot_exchange, swap_exchange, symbol)
	item, err := c.Get(spot_exchange, swap_exchange, symbol)
	if err != nil {
		return err
	}
	if item.Status {
		return fmt.Errorf("running schema can't be deleted: %s", key)
	}
	sp := SwapPositionConfig{
		Exchange: item.SwapExchange,
	}
	swapPositionItem, _ := sp.Get(item.Symbol)
	var position float64
	if item.Models == SpotLessSwapMore {
		position = swapPositionItem.BuyVolume
	} else if item.Models == SpotMoreSwapLess {
		position = swapPositionItem.SellVolume
	} else {
		return fmt.Errorf("schema models err: %s %s", key, item.Models)
	}
	if position > 0 {
		return fmt.Errorf("position volume limit schema can't be deleted: %s", key)
	}
	err = redisDB_H.Del(context.Background(), key).Err()
	if err != nil {
		return err
	}
	msg := HedgeSchemaMQ{
		Action: "delete",
		Data: map[string]string{
			"id":            c.RdsName(spot_exchange, swap_exchange, symbol),
			"spot_exchange": spot_exchange,
			"swap_exchange": swap_exchange,
			"symbol":        symbol,
		},
	}
	buf, _ := json.Marshal(msg)
	PublishToHedgeSchema(buf)
	return nil
}

func toInt(val string) int64 {
	i, _ := strconv.ParseInt(val, 10, 64)
	return i
}

func toFloat(val string) float64 {
	f, _ := strconv.ParseFloat(val, 64)
	return f
}

func toBool(val string) bool {
	if val == "on" {
		return true
	} else {
		return false
	}
}

func boolTo(val bool) string {
	if val {
		return "on"
	} else {
		return "off"
	}
}
func floatTo(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}
