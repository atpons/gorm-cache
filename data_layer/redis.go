package data_layer

import (
	"context"
	"time"

	"github.com/Pacific73/gorm-cache/config"
	"github.com/Pacific73/gorm-cache/util"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

type RedisLayer struct {
	r         rueidis.Client
	client    rueidiscompat.Cmdable
	ttl       int64
	logger    config.LoggerInterface
	keyPrefix string

	batchExistSha string
	cleanCacheSha string
}

func (r *RedisLayer) Init(conf *config.CacheConfig, prefix string) error {
	if conf.RedisConfig.Mode == config.RedisConfigModeOptions {
		client, err := rueidis.NewClient(conf.RedisConfig.Options)
		if err != nil {
			panic(err)
		}
		r.client = rueidiscompat.NewAdapter(client)
		r.r = client
	} else {
		r.client = rueidiscompat.NewAdapter(conf.RedisConfig.Client)
		r.r = conf.RedisConfig.Client
	}

	r.ttl = conf.CacheTTL
	r.logger = conf.DebugLogger
	r.logger.SetIsDebug(conf.DebugMode)
	r.keyPrefix = prefix
	return r.initScripts()
}

func (r *RedisLayer) initScripts() error {
	batchKeyExistScript := `
		for idx, val in pairs(KEYS) do
			local exists = redis.call('EXISTS', val)
			if exists == 0 then
				return 0
			end
		end
		return 1`

	cleanCacheScript := `
		local keys = redis.call('keys', ARGV[1])
		for i=1,#keys,5000 do 
			redis.call('del', 'defaultKey', unpack(keys, i, math.min(i+4999, #keys)))
		end
		return 1`

	result := r.client.ScriptLoad(context.Background(), batchKeyExistScript)
	if result.Err() != nil {
		r.logger.CtxError(context.Background(), "[initScripts] init script 1 error: %v", result.Err())
		return result.Err()
	}
	r.batchExistSha = result.Val()
	r.logger.CtxInfo(context.Background(), "[initScripts] init batch exist script sha1: %s", r.batchExistSha)

	result = r.client.ScriptLoad(context.Background(), cleanCacheScript)
	if result.Err() != nil {
		r.logger.CtxError(context.Background(), "[initScripts] init script 2 error: %v", result.Err())
		return result.Err()
	}
	r.cleanCacheSha = result.Val()
	r.logger.CtxInfo(context.Background(), "[initScripts] init clean cache script sha1: %s", r.cleanCacheSha)
	return nil
}

func (r *RedisLayer) CleanCache(ctx context.Context) error {
	result := r.client.EvalSha(ctx, r.cleanCacheSha, []string{"0"}, r.keyPrefix+":*")
	if result.Err() != nil {
		r.logger.CtxError(ctx, "[CleanCache] clean cache error: %v", result.Err())
		return result.Err()
	}
	return nil
}

func (r *RedisLayer) BatchKeyExist(ctx context.Context, keys []string) (bool, error) {
	result := r.client.EvalSha(ctx, r.batchExistSha, keys)
	if result.Err() != nil {
		r.logger.CtxError(ctx, "[BatchKeyExist] eval script error: %v", result.Err())
		return false, result.Err()
	}
	return result.Bool()
}

func (r *RedisLayer) KeyExists(ctx context.Context, key string) (bool, error) {
	result := r.client.Exists(ctx, key)
	if result.Err() != nil {
		r.logger.CtxError(ctx, "[KeyExists] exists error: %v", result.Err())
		return false, result.Err()
	}
	if result.Val() == 1 {
		return true, nil
	}
	return false, nil
}

func (r *RedisLayer) GetValue(ctx context.Context, key string) (string, error) {
	return r.client.Cache(time.Duration(util.RandFloatingInt64(r.ttl))*time.Millisecond).Get(ctx, key).Result()
}

func (r *RedisLayer) BatchGetValues(ctx context.Context, keys []string) ([]string, error) {
	result, err := rueidis.MGetCache(r.r, ctx, time.Duration(util.RandFloatingInt64(r.ttl))*time.Millisecond, keys)
	if err != nil {
		r.logger.CtxError(ctx, "[BatchGetValues] mget error: %v", err.Error())
		return nil, err
	}
	strs := make([]string, 0, len(result))
	for _, obj := range result {
		if obj.Error() != nil {
			r.logger.CtxError(ctx, "[BatchGetValues] mget key result error: %v", err.Error())
			return nil, err
		}
		v, err := obj.ToString()
		if err != nil {
			r.logger.CtxError(ctx, "[BatchGetValues] mget key result to string error: %v", err.Error())
			return nil, err
		}
		strs = append(strs, v)
	}
	return strs, nil
}

func (r *RedisLayer) DeleteKeysWithPrefix(ctx context.Context, keyPrefix string) error {
	result := r.client.EvalSha(ctx, r.cleanCacheSha, []string{"0"}, keyPrefix+":*")
	return result.Err()
}

func (r *RedisLayer) DeleteKey(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *RedisLayer) BatchDeleteKeys(ctx context.Context, keys []string) error {
	return r.client.Del(ctx, keys...).Err()
}

func (r *RedisLayer) BatchSetKeys(ctx context.Context, kvs []util.Kv) error {
	if r.ttl == 0 {
		spreads := make([]interface{}, 0, len(kvs))
		for _, kv := range kvs {
			spreads = append(spreads, kv.Key)
			spreads = append(spreads, kv.Value)
		}
		return r.client.MSet(ctx, spreads...).Err()
	}
	cmds := make(rueidis.Commands, 0, len(kvs))
	for _, kv := range kvs {
		cmds = append(cmds, r.r.B().Set().Key(kv.Key).Value(kv.Value).ExSeconds(r.ttl*1000).Build())
	}
	for _, resp := range r.r.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return resp.Error()
		}
	}
	return nil
}

func (r *RedisLayer) SetKey(ctx context.Context, kv util.Kv) error {
	return r.client.Set(ctx, kv.Key, kv.Value, time.Duration(util.RandFloatingInt64(r.ttl))*time.Millisecond).Err()
}
