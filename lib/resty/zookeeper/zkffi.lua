-- Author AlbertXiao

local tcp = ngx.socket.tcp
local strlen = string.len
local strsub = string.sub
local strbyte = string.byte
local strchar = string.char
local strrep = string.rep
local bit = require "bit"
local bor = bit.bor
local band = bit.band
local lshift = bit.lshift
local rshift = bit.rshift
local exiting = ngx.worker.exiting
local sleep = ngx.sleep
local now = ngx.now
--constants

--error info
local ZNODEEXISTS = -110

--xids
local WATCHER_EVENT_XID = -1
local PING_XID = -2
local AUTH_XID = -4
local SET_WATCHES_XID = -8
local CLOSE_XID = -9
-- ops
local ZOO_NOTIFY_OP = 0
local ZOO_CREATE_OP = 1
local ZOO_DELETE_OP = 2
local ZOO_EXISTS_OP = 3
local ZOO_GETDATA_OP = 4
local ZOO_SETDATA_OP = 5
local ZOO_GETACL_OP = 6
local ZOO_SETACL_OP = 7
local ZOO_GETCHILDREN_OP = 8
local ZOO_SYNC_OP = 9
local ZOO_PING_OP = 11
local ZOO_GETCHILDREN2_OP = 12
local ZOO_CHECK_OP = 13
local ZOO_MULTI_OP = 14
local ZOO_CLOSE_OP = -11
local ZOO_SETAUTH_OP = 100
local ZOO_SETWATCHES_OP = 101
--
local ZOO_EPHEMERAL = 1
local ZOO_SEQUENCE = 2

--
local ZNONODE = -101

local function set_byte4(n)
    return strchar(band(rshift(n, 24), 0xff),
                   band(rshift(n, 16), 0xff),
                   band(rshift(n, 8), 0xff),
                   band(n, 0xff))
end
local function set_byte8(n)
    return strchar(band(rshift(n, 56), 0xff),
                   band(rshift(n, 48), 0xff),
                   band(rshift(n, 40), 0xff),
                   band(rshift(n, 32), 0xff),
                   band(rshift(n, 24), 0xff),
                   band(rshift(n, 16), 0xff),
                   band(rshift(n, 8), 0xff),
                   band(n, 0xff))
end
-- local function get_byte2(data, i)
--     local a, b = strbyte(data, i, i + 1)
--     return bor(b, lshift(a, 8)), i + 2
-- end
local function get_byte4(data, i)
    local a, b, c, d = strbyte(data, i, i + 3)
    return bor(d, lshift(c, 8), lshift(b, 16), lshift(a, 24)), i + 4
end
-- local function get_byte8(data, i)
--     local a, b, c, d, e, f, g, h = strbyte(data, i, i + 7)
--     return bor(h, lshift(g, 8), lshift(f, 16), lshift(e, 24), lshift(d, 32),
--         lshift(c, 40), lshift(b, 48), lshift(a, 56)), i + 8
-- end
local _M = {
    _VERSION = '0.02',
    EPHEMERAL = ZOO_EPHEMERAL,
    SEQUENCE = ZOO_SEQUENCE,
}

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

    return sock:settimeouts(timeout, timeout, timeout)
end

function _M.connect(self, host, port)
    local sock = self.sock

    local req = set_byte4(44) .. set_byte4(0)
        .. set_byte8(0) .. set_byte4(0)
        .. set_byte8(0) .. set_byte4(0)
        .. strrep('\0', 16)
    if not sock then
        return nil, "not initialized"
    end

    local ok, err = sock:connect(host, port)
    if not ok then
        return nil, err
    end
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            if res then
                --local v = get_byte4(res, 1)
                local t = get_byte4(res, 5)
                --local sid = get_byte8(res, 9)
                --local pl = get_byte4(res, 13)
                --local p = get_byte2(res, 15)
                self.sn = 0
                self.session_timeout = t
                return true
            else
                return nil, err
            end
        end
    end
    return nil, "recv head error"
end

local function unpack_strings(str)
    local size = strlen(str)
    local pos = 0
    local str_set = {}
    local index = 1
    while size > pos do
        local len = get_byte4(strsub(str, 1+pos, 4+pos), 1)
        local s = strsub(str, 5+pos, 5+pos+len-1)
        str_set[index] = s
        index = index + 1
        pos = pos + len + 4
    end
    return str_set
end

local function build_cmd(sn, op, path)
    local pathlen = strlen(path)
    return set_byte4(12+pathlen+1) .. set_byte4(sn)
        .. set_byte4(op) .. set_byte4(pathlen) .. path .. '\0';
end

function _M.get_children(self, path)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local sn = self.sn + 1
    local req = build_cmd(sn, ZOO_GETCHILDREN_OP, path)
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, "send error"
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            if strlen(res) > 16 then
                sn = get_byte4(res, 1)
                --local zxid = get_byte8(res, 9)
                --local err = get_byte4(res, 13)
                --local count = get_byte4(res, 17)
                self.sn = sn+1
                return unpack_strings(strsub(res, 21))
            else
                return nil, "recv error"
            end
        end
    end
    return nil, "recv head error"
end

function _M.get_data(self, path)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local sn = self.sn + 1
    local req = build_cmd(sn, ZOO_GETDATA_OP, path)
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            if strlen(res) > 16 then
                sn = get_byte4(res, 1)
                --local zxid = get_byte8(res, 9)
                --local err = get_byte4(res, 13)
                len = get_byte4(res, 17)
                self.sn = sn+1
                return strsub(res, 21, 21+len-1)
            else
                return nil, "recv error"
            end
        end
    end
    return nil, "recv head error"
end

function _M.create(self, path, data, opt)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local sn = self.sn + 1
    if not data or strlen(data) == 0 then
        data = " "
    end

    local acl_scheme = "world"
    local acl_id = "anyone"
    local flag = 0
    if opt and opt[ZOO_EPHEMERAL] then
        flag = bor(flag, ZOO_EPHEMERAL)
    end
    if opt and opt[ZOO_SEQUENCE] then
        flag = bor(flag, ZOO_SEQUENCE)
    end
    local req = set_byte4(sn) .. set_byte4(ZOO_CREATE_OP) 
        .. set_byte4(strlen(path)) .. path .. set_byte4(strlen(data)) .. data
        .. set_byte4(1) .. set_byte4(0x1f) .. set_byte4(strlen(acl_scheme))
        .. acl_scheme .. set_byte4(strlen(acl_id)) .. acl_id .. set_byte4(flag)
    req = set_byte4(strlen(req)) .. req
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            if strlen(res) >= 16 then
                sn = get_byte4(res, 1)
                --local zxid = get_byte8(res, 9)
                err = get_byte4(res, 13)
                self.sn = sn+1
                if err == 0 then
                    len = get_byte4(res, 17)
                    return true, strsub(res, 21, 21+len-1)
                else
                    if err == ZNODEEXISTS then
                        err = "node exists"
                    end
                    return false, err
                end
            else
                return nil, "recv error"
            end
        end
    end
    return nil, "recv head error"
end

function _M.ping(self)
    local sock = self.sock
    local req = set_byte4(8) .. set_byte4(PING_XID) .. set_byte4(ZOO_PING_OP)
    if not sock then
        return nil, "not initialized"
    end
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            local xid = get_byte4(res, 1)
            if xid == PING_XID then
                return true
            else
                err = "unknow reponse"
            end
        end
    end
    return nil, err
end

function _M.exist(self, path)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local sn = self.sn + 1
    local req = build_cmd(sn, ZOO_EXISTS_OP, path)
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            if strlen(res) >= 16 then
                sn = get_byte4(res, 1)
                --local zxid = get_byte8(res, 5)
                err = get_byte4(res, 13)
                self.sn = sn+1
                if err == ZNONODE then
                    return false, "not exist"
                elseif err == 0 then
                    return true
                end
            else
                return nil, "recv error"
            end
        end
    end
    return nil, "recv head error"
end

function _M.get_pinginterval(self)
    --change to seconds
    return (self.session_timeout/3)/1000
end

function _M.loop_keepalive(self)
    local sleep_period = 0.1
    local last_send_time = 0
    while true do
        if exiting() then
            self:closesession()
            self:close()
            return true
        end
        if now() - last_send_time > self:get_pinginterval() then
            local ok, err = self:ping()
            if not ok then
                return nil, err
            end
            last_send_time = now()
        end
        sleep(sleep_period)
    end
end

function _M.close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:close()
end

function _M.closesession(self)
    local sock = self.sock
    local req = set_byte4(8) .. set_byte4(CLOSE_XID) .. set_byte4(ZOO_CLOSE_OP)
    if not sock then
        return nil, "not initialized"
    end
    local bytes, err = sock:send(req)
    if not bytes then
        print(err)
        return nil, err
    end
    local res, err = sock:receive(4)
    if res then
        local len = get_byte4(res, 1)
        if len then
            res, err = sock:receive(len)
            local xid = get_byte4(res, 1)
            if xid == CLOSE_XID then
                print("recv close response")
            else
                print("recv unknow response")
            end
        end
    else
        return nil, err
    end
    return bytes
end
return _M

