--Author Alertxiao
--this file demonstrates how to register ephemeral info to zookeeper server and keep alive

local zk = require "zk"
local sleep = ngx.sleep
local new_timer = ngx.timer.at
local workid = ngx.worker.id
local wait = ngx.thread.wait


local function run()
    local zkc = zk:new()

    local ok, err = zkc:connect("10.100.14.40:2181")
    --local ok, err = zkc:connect("10.80.7.123:2181")
    if not ok then
        return
    end
    --zkc:create("/test1236")
    local opt = {}
    opt[zk.EPHEMERAL] = 1
    opt[zk.SEQUENCE] = 1
    ok, err = zkc:create("/test1236/1", nil, opt)
    if not ok then
        print(err)
        return
    end
    zk::loop_keepalive()
end
    
local function loop()
    while true do
        run()
        sleep(1)
    end
end
if workid() == 0 then
    print("start loop")
    new_timer(0, loop)
end

