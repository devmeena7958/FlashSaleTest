package com.dev.FlashSale.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
public class OrderRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long userId;

    private Long itemId;

    private LocalDateTime createdAt;

    public OrderRecord(Long userId, Long itemId) {
        this.userId = userId;
        this.itemId = itemId;
        this.createdAt = LocalDateTime.now();
    }
}