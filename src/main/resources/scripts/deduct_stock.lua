---- KEYS[1]: Redis stock key (e.g., "item:stock:1")
---- ARGS[1]: Quantity requested (e.g., 1)
--
--local stockKey = KEYS[1]
--local requestedQuantity = tonumber(ARGS[1])
--
---- Read current stock from Redis
--local currentStock = tonumber(redis.call('get', stockKey))
--
---- Check if stock exists or if it's less than requested quantity
--if not currentStock or currentStock < requestedQuantity then
--	return -1 -- Indicating SOLD_OUT / Insufficient stock
--else
---- Atomically deduct stock and return remaining value
--	return redis.call('decrby', stockKey, requestedQuantity)
--end


local stockKey = KEYS[1]
local requestedQuantity = tonumber(ARGV[1])

local currentStock = tonumber(redis.call('GET', stockKey))

if not currentStock or currentStock < requestedQuantity then
	return -1
else
	return redis.call('DECRBY', stockKey, requestedQuantity)
end