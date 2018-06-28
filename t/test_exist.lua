local zk = require "zk"

zkc = zk:new()
local ok, err = zkc:connect("10.100.14.40:2181")
if ok then
    local path = "/test1235/3"
    ok, err = zkc:exist(path)
    print (ok)
    if ok == false then
        ok, err = zkc:create(path)
        print (ok, err)
    end
end
