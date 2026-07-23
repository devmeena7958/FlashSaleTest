package com.dev.FlashSale.DTOs;

import java.io.Serializable;

public record OrderPlacedEvent(Long userId, Long itemId, String orderId) implements Serializable {}
