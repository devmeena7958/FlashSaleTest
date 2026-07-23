package com.dev.FlashSale.controller;


import com.dev.FlashSale.DTOs.OrderPlacedEvent;
import com.dev.FlashSale.DTOs.OrderRequest;

import com.dev.FlashSale.service.OrderProducer;
import com.dev.FlashSale.service.RedisInventoryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class FlashSaleController {

    private final RedisInventoryService redisInventoryService;
    private final OrderProducer orderProducer;

    public FlashSaleController(RedisInventoryService redisInventoryService, OrderProducer orderProducer) {
        this.redisInventoryService = redisInventoryService;
        this.orderProducer = orderProducer;
    }

    // High-concurrency optimized flash sale checkout
    @PostMapping("/orders/async")
    public ResponseEntity<String> placeOrderAsync(@RequestBody OrderRequest request) {
        // 1. Deduct stock atomically in Redis memory (<2ms)
        Long remainingStock = redisInventoryService.deductStock(request.itemId(), 1);

        if (remainingStock < 0) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("SOLD_OUT");
        }

        // 2. Publish event to Kafka for background processing
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent event = new OrderPlacedEvent(request.userId(), request.itemId(), orderId);
        orderProducer.sendOrderEvent(event);

        // 3. Instantly respond to user with 202 ACCEPTED
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body("Order accepted! Processing ID: " + orderId);
    }




















//
//    private final RedisInventoryService redisInventoryService;
//
//    public FlashSaleController(RedisInventoryService redisInventoryService) {
//        this.redisInventoryService = redisInventoryService;
//    }
//
//    // Warm-up endpoint to initialize Redis stock before sale starts
//    @PostMapping("/items/{itemId}/warmup")
//    public ResponseEntity<String> warmUpCache(@PathVariable Long itemId, @RequestParam Integer stock) {
//        redisInventoryService.setStock(itemId, stock);
//        return ResponseEntity.ok("Stock of " + stock + " loaded into Redis for item " + itemId);
//    }
//
//    // Step 2: High-concurrency optimized endpoint using Redis Lua
//    @PostMapping("/orders/redis")
//    public ResponseEntity<String> placeOrderRedis(@RequestBody OrderRequest request) {
//        Long remainingStock = redisInventoryService.deductStock(request.itemId(), 1);
//
//        if (remainingStock < 0) {
//            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Item is SOLD OUT!");
//        }
//
//        // Stock successfully claimed in-memory!
//        // (In Step 3, we will publish an asynchronous event to Kafka right here)
//        return ResponseEntity.status(HttpStatus.ACCEPTED)
//                .body("Stock claimed! Remaining stock: " + remainingStock);
//    }









//    private final FlashSaleNaiveService naiveService;
//    private final ItemRepository itemRepository;
//
//    public FlashSaleController(FlashSaleNaiveService naiveService, ItemRepository itemRepository) {
//        this.naiveService = naiveService;
//        this.itemRepository = itemRepository;
//    }
//
//    // Helper endpoint to seed stock for testing
//    @PostMapping("/items")
//    public ResponseEntity<Item> createItem(@RequestParam String title, @RequestParam Integer stock) {
//        return ResponseEntity.ok(itemRepository.save(new Item(title, stock)));
//    }
//
//    // Naive order creation
//    @PostMapping("/orders/naive")
//    public ResponseEntity<String> placeOrderNaive(@RequestBody OrderRecord request) {
//        String result = naiveService.createOrderNaive(request.getUserId(), request.getItemId());
//        if ("SOLD_OUT".equals(result)) {
//            return ResponseEntity.badRequest().body("Item is sold out!");
//        }
//        return ResponseEntity.ok("Order created successfully!");
//    }
}
