package com.dev.FlashSale.controller;


import com.dev.FlashSale.DTOs.OrderPlacedEvent;
import com.dev.FlashSale.DTOs.OrderRequest;

import com.dev.FlashSale.entity.Item;
import com.dev.FlashSale.repo.ItemRepository;
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
    private final ItemRepository itemRepository;

    public FlashSaleController(RedisInventoryService redisInventoryService, OrderProducer orderProducer, ItemRepository itemRepository) {
        this.redisInventoryService = redisInventoryService;
        this.orderProducer = orderProducer;
        this.itemRepository = itemRepository;
    }



    @PostMapping("/items")
    public ResponseEntity<Item> createItem(@RequestParam String title, @RequestParam Integer stock) {


        Item savedItem = itemRepository.save(new Item(title, stock));
        redisInventoryService.setStock(savedItem.getId(), savedItem.getStock());

        return ResponseEntity.ok(savedItem);

    }




    // High speed order requests
    @PostMapping("/orders/async")
    public ResponseEntity<String> placeOrderAsync(@RequestBody OrderRequest request) {
        // 1. Deduct stock atomically in Redis memory (<2ms)

        // this will trigger LUA script if stock is aval deduct 1 item and send remaking stock, and if not send -1;
        Long remainingStock = redisInventoryService.tryDeductStock(request.itemId(), 1);



        // mean redis return -1, item not found mean empty
        if (remainingStock < 0) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("SOLD_OUT");
        }

        //  Publish event to Kafka for background processing
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent event = new OrderPlacedEvent(request.userId(), request.itemId(), orderId);
        orderProducer.sendOrderEvent(event);

        //  Instantly respond to user with 202 ACCEPTED
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body("Order accepted! Processing ID: " + orderId);
    }







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
