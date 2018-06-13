local cjson = require "cjson"
local zk = require "zk_cluster"

local config = {
    serv_list = {
        "10.100.14.40:2181",
        "10.100.14.40:2181",
        "10.100.14.40:2181",
        "10.100.14.40:2181",
        --"10.80.6.224:2181",
        --"10.80.6.225:2181",
        --"10.80.6.226:2181",
        --"10.80.6.227:2181",
    },
    timeout = 1000
}

local zc = zk:new(config)

--print(zc:get_data("/taskmgr/10.80.6.223:12349"))

local res, err = zc:get_children("/")
if res then
    print(cjson.encode(res))
else
    print(err)
end
local res, err = zc:get_children("/")
if res then
    print(cjson.encode(res))
else
    print(err)
end
local res, err = zc:get_children("/")
if res then
    print(cjson.encode(res))
else
    print(err)
end
res, err = zc:get_children("/taskmgr")
if res then
    print(cjson.encode(res)) 
    for i, v in ipairs(res) do
        print(zc:get_data("/taskmgr/" .. v))
    end
else
    print(err)
end
res, err = zc:get_children("/clips")
if res then
    print(cjson.encode(res)) 
    for i, v in ipairs(res) do
        print(zc:get_data("/clips/" .. v))
    end
else
    print(err)
end
res, err = zc:get_children("/clips")
if res then
    print(cjson.encode(res)) 
    for i, v in ipairs(res) do
        print(zc:get_data("/clips/" .. v))
    end
else
    print(err)
end

local conn = zk:new(config)
print(cjson.encode(conn:get_children("/")))
