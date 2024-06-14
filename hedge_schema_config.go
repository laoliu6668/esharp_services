package esharp_services

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/laoliu6668/esharp_services/util"
)

const (
	SpotMoreSwapLess = "spot_more_swap_less" // 现多期空
	SpotLessSwapMore = "spot_less_swap_more" // 现空期多
)

type HedgeSchemaItem struct {
	Id     string `json:"id"`     //  方案编号
	Status bool   `json:"status"` //  运行状态: on-运行中 off-已停止

	OpendLock    bool   `json:"open_lock"`     // 开仓行为锁: on-锁定 off-解锁
	CloseLock    bool   `json:"close_lock"`    // 平仓行为锁: on-锁定 off-解锁
	Symbol       string `json:"symbol"`        // c 对冲币对
	SpotExchange string `json:"spot_exchange"` // c 现货交易所
	SwapExchange string `json:"swap_exchange"` // c 期货交易所

	// group start 币对配置
	MinOrderVolume        float64 `json:"min_order_volume"`         // 最小下单数量
	MinOrderAmount        float64 `json:"min_order_amount"`         // 最小下单金额
	TradeVolumePoint      int64   `json:"trade_volume_point"`       // 交易数量精度
	TradePricePoint       int64   `json:"trade_price_point"`        // 交易价格精度
	TradeAmountPoint      int64   `json:"trade_amount_point"`       // 交易金额精度
	ContractSize          float64 `json:"contract_size"`            // 合约面值
	MaxBuyPositionVolume  int64   `json:"max_buy_position_volume"`  // 多仓持仓上限(张)
	MaxSellPositionVolume int64   `json:"max_sell_position_volume"` // 空仓持仓上限(张)
	MaxOpenOrderVolume    int64   `json:"max_open_order_volume"`    // 开仓单笔下单上限(张)
	MaxCloseOrderVolume   int64   `json:"max_close_order_volume"`   // 平仓单笔下单上限(张)
	// group end 币对配置

	Models              string  `json:"models"`                // c 对冲方案: spot_more_swap_less-现多期空 spot_less_swap_more-现空期多
	OpendRate           float64 `json:"opend_rate"`            // * 开仓差率
	CloseRate           float64 `json:"close_rate"`            // * 平仓差率
	SingleOrderVolume   int64   `json:"single_order_volume"`   // * 期货订单单笔张数(张)
	PositionVolumeLimit int64   `json:"position_volume_limit"` // * 期货仓位持仓上限(张)
	SpotVolume          float64 `json:"spot_volume"`           // 现货持币数量
	SpotCost            float64 `json:"spot_cost"`             // 现货持币花费(USDT)
	SwapVolume          int64   `json:"swap_volume"`           // 期货持仓数量(张)
	SwapCost            float64 `json:"swap_cost"`             // 期货持仓花费(USDT)
	RelOpendRate        float64 `json:"rel_opend_rate"`        // 实开差率
	RelCloseRate        float64 `json:"rel_close_rate"`        // 实平差率
	RelPl               float64 `json:"rel_pl"`                // 实际盈亏
}

type HedgeSchemaConfig struct {
}

func (c *HedgeSchemaConfig) RdsName() string {
	return "id"
}

func (c *HedgeSchemaConfig) Keys() ([]string, error) {
	return redisDB_H.Keys(context.Background(), "*").Result()
}
func (c *HedgeSchemaConfig) Add(spot_exchange, swap_exchange, symbol, model string) (id string, err error) {
	has, err := c.Has(spot_exchange, swap_exchange, symbol)
	if err != nil {
		return "", err
	}
	if has {
		return "", errors.New("already exists")
	}
	if model != SpotMoreSwapLess && model != SpotLessSwapMore {
		return "", errors.New("model error")
	}

	id = util.GetUUID32()
	err = redisDB_H.HSet(context.Background(), id, "id", id).Err()
	if err != nil {
		return "", fmt.Errorf("redis set id error: %s", err)
	}
	c.Set(id, "symbol", symbol)
	c.Set(id, "spot_exchange", spot_exchange)
	c.Set(id, "swap_exchange", swap_exchange)
	c.Set(id, "models", model)

	spotSymbolItem, err := (&SpotSymbolConfig{
		Exchange: spot_exchange,
	}).Get(symbol)
	if err != nil {
		return "", fmt.Errorf("spot symbol config error: %s", err)
	}
	SwapSymbolitem, err := (&SwapSymbolConfig{
		Exchange: swap_exchange,
	}).Get(symbol)
	if err != nil {
		return "", fmt.Errorf("swap symbol config error: %s", err)
	}
	c.SetFloat(id, "min_order_volume", spotSymbolItem.MinOrderVolume)
	c.SetFloat(id, "min_order_amount", spotSymbolItem.MinOrderAmount)
	c.SetInt(id, "trade_volume_point", spotSymbolItem.TradeVolumePoint)
	c.SetInt(id, "trade_price_point", spotSymbolItem.TradePricePoint)
	c.SetInt(id, "trade_amount_point", spotSymbolItem.TradeAmountPoint)

	c.SetFloat(id, "contract_size", SwapSymbolitem.ContractSize)
	c.SetInt(id, "max_buy_position_volume", SwapSymbolitem.MaxBuyPositionVolume)
	c.SetInt(id, "max_sell_position_volume", SwapSymbolitem.MaxSellPositionVolume)
	c.SetInt(id, "max_open_order_volume", SwapSymbolitem.MaxOpenOrderVolume)
	c.SetInt(id, "max_close_order_volume", SwapSymbolitem.MaxCloseOrderVolume)

	// default value
	c.SetBool(id, "status", false)
	c.SetBool(id, "open_lock", false)
	c.SetBool(id, "close_lock", false)
	c.SetFloat(id, "opend_rate", 0)
	c.SetFloat(id, "close_rate", 0)
	c.SetInt(id, "single_order_volume", 0)
	c.SetInt(id, "position_volume_limit", 0)
	c.SetFloat(id, "spot_volume", 0)
	c.SetFloat(id, "spot_cost", 0)
	c.SetInt(id, "swap_volume", 0)
	c.SetFloat(id, "swap_cost", 0)
	c.SetFloat(id, "rel_opend_rate", 0)
	c.SetFloat(id, "rel_close_rate", 0)
	c.SetFloat(id, "rel_pl", 0)
	return
}

func (c *HedgeSchemaConfig) Vals() (allVals []HedgeSchemaItem, err error) {
	keys, err := c.Keys()
	if err != nil {
		return
	}
	allVals = []HedgeSchemaItem{}
	for _, key := range keys {
		value, _ := c.Get(key)
		allVals = append(allVals, value)
	}
	return
}

func (c *HedgeSchemaConfig) Has(spot_exchange string, swap_exchange string, symbol string) (has bool, err error) {
	list, err := c.Vals()
	if err != nil {
		return false, err
	}
	for _, v := range list {
		if v.SpotExchange == spot_exchange && v.SwapExchange == swap_exchange && v.Symbol == symbol {
			return true, nil
		}
	}
	return false, nil
}
func (c *HedgeSchemaConfig) Get(OrderId string) (item HedgeSchemaItem, err error) {
	itemVals, err1 := redisDB_H.HGetAll(context.Background(), OrderId).Result()
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
			item.MaxBuyPositionVolume = toInt(v)
		case "max_sell_position_volume":
			item.MaxSellPositionVolume = toInt(v)
		case "max_open_order_volume":
			item.MaxOpenOrderVolume = toInt(v)
		case "max_close_order_amount":
			item.MaxCloseOrderVolume = toInt(v)
		case "models":
			item.Models = v
		case "opend_rate":
			item.OpendRate = toFloat(v)
		case "close_rate":
			item.CloseRate = toFloat(v)
		case "single_order_volume":
			item.SingleOrderVolume = toInt(v)
		case "position_volume_limit":
			item.PositionVolumeLimit = toInt(v)
		case "spot_volume":
			item.SpotVolume = toFloat(v)
		case "spot_cost":
			item.SpotCost = toFloat(v)
		case "swap_volume":
			item.SwapVolume = toInt(v)
		case "swap_cost":
			item.SwapCost = toFloat(v)
		case "rel_opend_rate":
			item.RelOpendRate = toFloat(v)
		case "rel_close_rate":
			item.RelCloseRate = toFloat(v)
		case "rel_pl":
			item.RelPl = toFloat(v)
		}

	}
	return
}
func (c *HedgeSchemaConfig) Set(orderId, field, value string) (err error) {
	has, err := redisDB_H.Exists(context.Background(), orderId).Result()
	if err != nil {
		return fmt.Errorf("redis Exists error: %s", err)
	}
	if has == 0 {
		// not has
		return fmt.Errorf("redis key not exists: %s", orderId)
	}
	err = redisDB_H.HSet(context.Background(), orderId, field, value).Err()
	if err != nil {
		return
	}
	return nil
}
func (c *HedgeSchemaConfig) SetInt(orderId, field string, value int64) (err error) {
	return c.Set(orderId, field, strconv.FormatInt(value, 10))
}

func (c *HedgeSchemaConfig) SetFloat(orderId, field string, value float64) (err error) {
	return c.Set(orderId, field, floatTo(value))
}
func (c *HedgeSchemaConfig) SetBool(orderId, field string, value bool) (err error) {
	return c.Set(orderId, field, boolTo(value))
}

// 运行中的方案不允许被删除，有期货持仓量的不能被删除
func (c *HedgeSchemaConfig) Del(orderId string) (err error) {
	item, err := c.Get(orderId)
	if err != nil {
		return err
	}
	if item.Status {
		return fmt.Errorf("running schema can't be deleted: %s", orderId)
	}
	if item.SwapVolume > 0 {
		return fmt.Errorf("position volume limit schema can't be deleted: %s", orderId)
	}
	err = redisDB_H.Del(context.Background(), orderId).Err()
	if err != nil {
		return err
	}
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
