local zk = require "zk"
local eph = zk.EPHEMERAL
local seq = zk.SEQUENCE

zkc = zk:new()
local ok, err = zkc:connect("10.100.14.40:2181")
if ok then
    local opt = {}
    opt[seq] = 1
    opt[eph] = 1
    ok, err = zkc:create("/test1235/1", "{}",   opt)
    if ok then
        print("create ok " .. err)
        for i=1, 10 do
            print("sleep ...")
            ngx.sleep(1)
            zkc:ping()
        end
    else
        print("create err " .. err)
    end
end
