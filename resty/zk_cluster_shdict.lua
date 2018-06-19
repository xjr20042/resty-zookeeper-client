-- Author AlbertXiao

local zk = require "zk"
local mlcache = require "resty.mlcache"
local tablen = table.getn
local strsub = string.sub


local function get_host(self)
    local serv_list = self.serv_list
    local index = self.robin % tablen(serv_list)  + 1
    self.robin = self.robin + 1
    return serv_list[index]
end

local function connect(self)
    local conn = zk:new()
    conn:set_timeout(self.timeout)
    for i=1, #self.serv_list do
        local host = get_host(self)
        local ok, err = conn:connect(host)
        if not ok then
            print("connect " .. host .. " error:" ..err)
        else
            self.conn = conn
            return conn
        end
    end
    return nil
end

local function get_children_imp(self, path)
    local conn = self.conn
    if not conn then
        conn = connect(self)
        if not conn then
            return nil, "connect error"
        end
    end

    local res, err = conn:get_children(path)
    if not res then
        conn:close()
        self.conn = nil
        return nil, err
    end

    return res
end

local function get_data_imp(self, path)
    local conn = self.conn
    if not conn then
        conn = connect(self)
        if not conn then
            return nil, "connect error"
        end
    end
    local res, err = conn:get_data(path)
    if not res then
        conn:close()
        self.conn = nil
        return nil, err
    end
    return res
end
local function mlcache_get_children_callback(self, path)
    -- this only runs *once* until the key expires, so
    -- do expensive operations like connecting to a remote
    -- backend here. i.e: call a MySQL server in this callback
    local res, err = get_children_imp(self, path)
    --print('getting children: ' .. path)
    return res
end
local function mlcache_get_data_callback(self, path)
    -- this only runs *once* until the key expires, so
    -- do expensive operations like connecting to a remote
    -- backend here. i.e: call a MySQL server in this callback
    local res, err = get_data_imp(self, path)
    --print('getting data: ' .. path)
    return res
end
local function common_get(self, path, get_type, user_cache)
    local use_cache = use_cache or true
    local cache = self.cache
    local res
    local err

    if use_cache then
        if get_type == 'child' then
            res, err = cache:get('child' .. path, nil, mlcache_get_children_callback, self, path) 
            if not err then
                return res
            else
                --print('get children cache error: ' .. err)
                return nil
            end
        elseif get_type == 'data' then
            res, err = cache:get('data' .. path, nil, mlcache_get_data_callback, self, path) 
            if not err then
                return res
            else
                --print('get children cache error: ' .. err)
                return nil
            end
        end
    else
        if get_type == 'child' then
            return get_children_imp(self, path)
        elseif get_type == 'data' then
            return get_data_imp(self, path)
        end
    end
end
local _M = { _VERSION = "0.02" }

-- constants

local mt = { __index = _M }

function _M.new(self, config)
    if not self.inited then
        local name = config.name or 'zk_cache'
        local timeout = config.timeout or 1000
        local expire = config.expire or 1
        local cache, err = mlcache.new(name, "zk_cache_dict", {
            lru_size = 500,    -- size of the L1 (Lua-land LRU) cache
            ttl      = expire,   --  ttl for hits
            neg_ttl  = 3600,     -- 1h ttl for misses
        })
        self.robin=0
        self.inited = true
        self.cache = cache
        --print('initt.....')
    end
    return setmetatable({serv_list=config.serv_list, timeout=timeout}, mt)
end

function _M.get_children(self, path, use_cache)
    return common_get(self, path, 'child', use_cache)
end

function _M.get_data(self, path, use_cache)
    return common_get(self, path, 'data', use_cache)
end

return _M

