package com.dev.FlashSale.service;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service
public class RedisInventoryService {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> deductStockScript;

    public RedisInventoryService(StringRedisTemplate redisTemplate, DefaultRedisScript<Long> deductStockScript) {
        this.redisTemplate = redisTemplate;
        this.deductStockScript = deductStockScript;
    }

    // Load the item in the redis
    public void setStock(Long itemId, Integer stock) {
        String key = "item:stock:" + itemId;
        redisTemplate.opsForValue().set(key, String.valueOf(stock));
    }

    // execute ATOMIC LUA script
    public Long tryDeductStock(Long itemId, Integer quantity) {
        String key = "item:stock:" + itemId;

        return redisTemplate.execute(
                deductStockScript,
                Collections.singletonList(key),
                String.valueOf(quantity)
        );
    }
}