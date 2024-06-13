package esharp_services

import (
	"github.com/redis/go-redis/v9"
)

const (
	EXCHANGE_HTX = "htx"
)

var redisDB *redis.Client
var redisDB_H *redis.Client

func Init(db *redis.Client, db2 *redis.Client) {
	redisDB = db
	redisDB_H = db2
	// InitHtxUtil()
}

// func InitHtxUtil() {
// 	// 初始化火币交易所配置
// 	apiConfigs, err := (&ExchangeConfig{
// 		ExchangeName: EXCHANGE_HTX,
// 	}).GetString("api")
// 	if err != nil {
// 		panic(fmt.Errorf("%s 初始化失败：%s", "htx", err))
// 	}
// 	accConfigs, err := (&ExchangeConfig{
// 		ExchangeName: EXCHANGE_HTX,
// 	}).Get("account")
// 	if err != nil {
// 		panic(fmt.Errorf("%s 初始化失败：%s", "htx", err))
// 	}
// 	apiConfig := htx.ApiConfigModel{
// 		AccountId: util.ParseInt(accConfigs["spot_account_id"], 0),
// 		Uid:       util.ParseInt(accConfigs["uid"], 0),
// 	}
// 	json.Unmarshal([]byte(apiConfigs), &apiConfig)
// 	htx.InitConfig(apiConfig)
// }
