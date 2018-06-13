local cjson = require "cjson"
local zk = require "zk_cluster"

local config = {
    serv_list = {
        "127.0.0.1:2181",
        "127.0.0.1:2182",
        "127.0.0.1:2183",
    },
    timeout = 1000,
    expire = 1,
}

local zc = zk:new(config)

local res, err = zc:get_children("/")

if res then
    print(cjson.encode(res)) 
    for i, v in ipairs(res) do
        print(zc:get_data("/" .. v))
    end
else
    print(err)
end
