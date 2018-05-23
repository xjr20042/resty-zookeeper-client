-- Author AlbertXiao


local struct = require "struct"

local tcp = ngx.socket.tcp
local pack = struct.pack
local unpack = struct.unpack
local strlen = string.len
local strsub = string.sub
local strbyte = string.byte

local _M = {_VERSION = '0.01'}

local mt = { __index = _M }

function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end

function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

function _M.connect(self, host)
    local sock = self.sock
    local req = pack(">iililic16", 44, 0, 0, 0, 0, 0, "")
    if not sock then
        print(err)
        return nil, "not initialized"
    end

    local ok, err = sock:connect(host)
    if not ok then
        print(err)
        return nil, err
    end
    local bytes, err = sock:send(req)
    if not bytes then
        print(err)
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = unpack(">i", res)
        if len then
            res, err = sock:receive(len)
            if res then
                local v, t, sid, pl,p = unpack(">iilis", res)
                self.sn = 0
                return true
            else
                return nil, err
            end
        end
    else
        print("read error :" .. err)
    end
    return nil
end

local function unpack_strings(str)
    local size = strlen(str)
    local pos = 0
    local str_set = {}
    local index = 1
    while size > pos do
        local len = unpack(">i", strsub(str, 1+pos, 4+pos))        
        local s = unpack(">c" .. len, strsub(str, 5+pos, 5+pos+len))
        str_set[index] = s
        index = index + 1
        pos = pos + len + 4
    end
    return str_set
end

function _M.get_children(self, path)
    local sock = self.sock
    if not sock then
        --print("not connected")
        return nil
    end
    local sn = self.sn + 1
    local req = pack(">iiiic" .. strlen(path) .. "b", 12+strlen(path)+1, sn, 8, strlen(path), path, strbyte(0))
    local bytes, err = sock:send(req)
    if not bytes then
        --print(err)
        return nil
    end
    local res, err = sock:receive(4)
    if res then
        local len = unpack(">i", res)
        if len then
            res, err = sock:receive(len)
            if strlen(res) > 16 then
                local sn, zxid, err, count = unpack(">ilii", res)
                self.sn = sn+1
                return unpack_strings(strsub(res, 21)) 
            else
                return nil
            end
        end
    end
end

function _M.get_data(self, path)
    local sock = self.sock
    if not sock then
        --print("not connected")
        return nil, "not initialized"
    end
    local sn = self.sn + 1
    local req = pack(">iiiic" .. strlen(path) .. "b", 12+strlen(path)+1, sn, 4, strlen(path), path, strbyte(0))
    local bytes, err = sock:send(req)
    if not bytes then
        --print(err)
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = unpack(">i", res)
        if len then
            res, err = sock:receive(len)
            if strlen(res) > 16 then
                local sn, zxid, err, len = unpack(">ilii", res)
                self.sn = sn+1
                --print(len)
                return strsub(res, 21, 21+len)
            else
                return nil
            end
        end
    end
    return nil
end

return _M

