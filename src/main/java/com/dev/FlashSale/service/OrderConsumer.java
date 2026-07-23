package com.dev.FlashSale.service;


import com.dev.FlashSale.DTOs.OrderPlacedEvent;
import com.dev.FlashSale.entity.Item;
import com.dev.FlashSale.entity.OrderRecord;
import com.dev.FlashSale.repo.ItemRepository;
import com.dev.FlashSale.repo.OrderRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderConsumer {

    private final OrderRepository orderRepository;
    private final ItemRepository itemRepository;

    public OrderConsumer(OrderRepository orderRepository, ItemRepository itemRepository) {
        this.orderRepository = orderRepository;
        this.itemRepository = itemRepository;
    }

    @KafkaListener(topics = "flash-sale-orders", groupId = "flash-sale-group")
    @Transactional
    public void processOrder(OrderPlacedEvent event) {
        // 1. Save order to PostgreSQL
        OrderRecord order = new OrderRecord(event.userId(), event.itemId());
        orderRepository.save(order);

        // 2. Decrement inventory row in PostgreSQL for accounting accuracy
        Item item = itemRepository.findById(event.itemId())
                .orElseThrow(() -> new IllegalArgumentException("Item not found: " + event.itemId()));
        item.setStock(item.getStock() - 1);
        itemRepository.save(item);

        System.out.println("Async DB write complete for Order: " + event.orderId() + " (User: " + event.userId() + ")");
    }
}