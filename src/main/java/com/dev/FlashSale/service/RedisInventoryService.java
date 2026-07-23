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

    // Helper method to sync database stock into Redis before the flash sale starts
    public void setStock(Long itemId, Integer stock) {
        String key = "item:stock:" + itemId;
        redisTemplate.opsForValue().set(key, String.valueOf(stock));
    }

    // Execute the atomic Lua Script
    public Long deductStock(Long itemId, Integer quantity) {
        String key = "item:stock:" + itemId;

        // Executes script natively in Redis memory
        return redisTemplate.execute(
                deductStockScript,
                Collections.singletonList(key),
                String.valueOf(quantity)
        );
    }
}