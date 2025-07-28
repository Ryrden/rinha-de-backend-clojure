local items = redis.call('ZRANGEBYSCORE', _:key, _:from, _:to)
local result = {
    default = {requests = 0, amount = 0},
    fallback = {requests = 0, amount = 0}
}

for i = 1, #items do
    local ok, data = pcall(cjson.decode, items[i])
    if ok and data.amount and data.processor then
        local group = result[data.processor]
        if group then
            group.requests = group.requests + 1
            group.amount = group.amount + data.amount
        end
    end
end

return {
    result.default.requests,
    string.format('%.2f', result.default.amount),
    result.fallback.requests,
    string.format('%.2f', result.fallback.amount)
}