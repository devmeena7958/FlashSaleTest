package com.dev.FlashSale.service;

import com.dev.FlashSale.DTOs.OrderPlacedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderPlacedEvent event) {
        // Partition key set to itemId to preserve order processing sequence per item
        kafkaTemplate.send("flash-sale-orders", String.valueOf(event.itemId()), event);
    }
}