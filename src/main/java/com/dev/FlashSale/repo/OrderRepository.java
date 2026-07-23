package com.dev.FlashSale.repo;

import com.dev.FlashSale.entity.OrderRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderRecord, Long> {}