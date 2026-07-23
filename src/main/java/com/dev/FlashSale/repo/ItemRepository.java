package com.dev.FlashSale.repo;

import com.dev.FlashSale.entity.Item;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ItemRepository extends JpaRepository<Item, Long> {}