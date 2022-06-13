use lib '.';
use t::TestZK 'no_plan';

run_tests();

__DATA__

=== TEST 1: connect

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok

=== TEST 2: get_child

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local ct, err = zkc:get_children("/")

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok
ok

=== TEST 3: get_data

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local data, err = zkc:get_data("/")

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok
ok

=== TEST 4: exist

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local data, err = zkc:exist("/")

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok
ok

=== TEST 5: close

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local _, err = zkc:closesession("/")

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok
ok
--- error_log
recv close response

=== TEST 6: ping

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local ok, err = zkc:ping()

        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local _, err = zkc:closesession()

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok
ok
ok
--- error_log
recv close response

=== TEST 7: create

--- config
location /t {
    content_by_lua_block {

        local zk = require("zkffi") 

        local zkc = zk.new()

        local ok, err = zkc:connect("10.101.14.37", 2181)
        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local opt = {
            [zk.EPHEMERAL] = 1, 
            --[zk.SEQUENCE] = 1,
        }

        local path = "/1234"
        local data = "hello there!"
        local ok, err = zkc:create(path, data, opt)

        if ok then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local _, err = zkc:exist(path)

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local remote_data, err = zkc:get_data(path)

        if remote_data == data and not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end

        local _, err = zkc:closesession()

        if not err then
            ngx.say("ok")
        else
            ngx.say(err)
        end
    }
}
--- request
GET /t
--- response_body
ok
ok
ok
ok
ok
--- error_log
recv close response
