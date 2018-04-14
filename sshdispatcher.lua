local posix, unistd, poll, bit32
pcall(function()
    posix = require 'posix'
    unistd = require 'posix.unistd'
    poll = require 'posix.poll'
    local ok
    ok, bit32 = pcall(function() return require 'bit' end)
    if not ok then bit32 = require 'bit32' end
end)

-----------------------------------------------------------------------
-- Serializer: simple text based serializer for pure lua objects
-----------------------------------------------------------------------
-- nor functions nor internal objects are supported

local Serializer = {}
Serializer.__index = Serializer

function Serializer:new()
    return setmetatable({registered_metatables={}, registered_metatables_inv={}}, self)
end

function Serializer:register_metatable(id, metatable)
    self.registered_metatables[id] = metatable
    self.registered_metatables_inv[metatable] = id
end

function Serializer:serialize(thing)
    local buffer = {}
    local obj_id, next_id = {}, 0
    local function record(...)
        for i = 1, select('#', ...) do
            local o = select(i, ...)
            assert(type(o) == 'string', 'recorded objects must be strings')
            buffer[#buffer + 1] = o
        end
    end
    local function ser(x)
        if type(x) == 'nil' then
            record '_'
        elseif type(x) == 'number' then
            record('n', tostring(x))
        elseif type(x) == 'boolean' then
            record(x and 't' or 'f')
        elseif type(x) == 'string' then
            record('s', (x:gsub('[%% \r\n]', {['%']='%%',[' ']='%s',['\r']='%r',['\n']='%n'})))
        elseif type(x) == 'table' then
            if obj_id[x] then
                record('r', tostring(obj_id[x]))
            else
                obj_id[x] = next_id
                local mt = getmetatable(x)
                assert(not mt or self.registered_metatables_inv[mt], 'metatable must be registered before usage')
                record('{', tostring(next_id), self.registered_metatables_inv[mt] or '_')
                if mt and mt.__serialize then
                    x:__serialize(record, ser)
                else
                    -- cast some magic to avoid calling __pairs on Lua 5.2+
                    for k, v in (unpack or table.unpack){next, x} do ser(k); ser(v) end
                end
                record('}')
            end
        end
    end
    ser(thing)
    return table.concat(buffer, ' ')
end

function Serializer:deserialize(str)
    local buffer, cursor = {}, 1
    local id_obj = {}
    for w in (str..' '):gmatch '(.-) ' do buffer[#buffer + 1] = w end
    local function peek() return buffer[cursor] end
    local function pop()
        cursor = cursor + 1
        return buffer[cursor - 1]
    end
    local function deserialize()
        local w = pop()
        if w == '_' then
            return nil
        elseif w == 'n' then
            return tonumber(pop())
        elseif w == 't' then
            return true
        elseif w == 'f' then
            return true
        elseif w == 's' then
            return pop():gsub('%%(.)', {['%']='%', s=' ', r='\r', n='\n'})
        elseif w == 'r' then
            return assert(id_obj[tonumber(pop())], 'invalid object reference')
        elseif w == '{' then
            local obj = {}
            id_obj[tonumber(pop())] = obj
            local mtname, mt = pop()
            if mtname ~= '_' then
                mt = assert(self.registered_metatables[mtname], 'metatable must be registered before usage')
                setmetatable(obj, mt)
            end
            if mt and mt.__deserialize then
                -- rules for __deserialize:
                --     * end when '}' is reached, but don't consume it,
                --     * if pop or peek returns nil, throw an error.
                obj:__deserialize(peek, pop, deserialize)
            else
                while peek() ~= '}' do
                    local k = deserialize()
                    rawset(obj, k, deserialize())
                end
            end
            pop()
            return obj
        else
            return error(('invalid serialization tag %q'):format(w))
        end
    end
    return deserialize()
end

-- As an opposite of SshDispatcher:__call, this is the only function that
--     the workers need to use, just pass the callback and it will receive
--     the dispatcher arg as a parameter.
-- work args are as follow:
--     [1]: the callback that will receive the dispatcher arg as a single
--           argument, any return value or error thrown must conform with
--           the Serializer restriction.
--     serializer: a Serializer instance, a new one will be created by default.
local function work(args)
    local serializer = args.serializer or Serializer:new()
    for line in io.lines() do
        local ok, res = pcall(function()
            local arg = serializer:deserialize(line)
            assert(arg.tag == 'work', 'only jobs with tag="work" are accepted')
            return args[1](arg.arg)
        end)
        if ok then
            print(serializer:serialize{tag='work', arg=res})
        else
            print(serializer:serialize{tag='work', failed=true, arg=res})
        end
    end
end

if not posix then return {work=work, Serializer=Serializer} end

-----------------------------------------------------------------------
-- POpen: Async bidirectional interface for calling processes
-----------------------------------------------------------------------

local function popen2(path, argt, use_path)
    local master, slave = posix.openpty()
    local child = assert(posix.fork())
    if child == 0 then
        -- child
        unistd.close(master)
        unistd.dup2(slave, unistd.STDOUT_FILENO)
        unistd.dup2(slave, unistd.STDIN_FILENO);
        (use_path == false and unistd.exec or unistd.execp)(path, argt)
        os.exit()
    end
    -- parent
    unistd.close(slave)
    local fcntl, termio = require 'posix.fcntl', require 'posix.termio'
    fcntl.fcntl(master, fcntl.F_SETFL, bit32.bor(fcntl.fcntl(master, fcntl.F_GETFL), fcntl.O_NONBLOCK))
    local ios = termio.tcgetattr(master)
    ios.lflag = bit32.band(ios.lflag, bit32.bnot(termio.ECHO))
    ios.oflag = bit32.band(ios.oflag, bit32.bnot(termio.ONLCR))
    termio.tcsetattr(master, termio.TCSADRAIN, ios)
    return master, child
end

local POpen = {}
POpen.__index = POpen

-- args:
--     fileselector: FileSelector instance to use
--     on_line: Callback that will be called with a line whenever it arrives
--     on_close: Will be called once the process finishes
--     path, argt: The same arguments as in posix.unistd.exec and posix.unistd.execp
--     use_path: Use the PATH search path if path does not contain a slash (default true)
function POpen:new(args)
    local res = setmetatable({closed=false, outbuffer={}, fileselector=args.fileselector}, self)
    local inbuffer = {}
    res.on_line = args.on_line
    res.fd, res.pid = popen2(args.path, args.argt, args.use_path)
    args.fileselector:register{res.fd, IN=function()
        inbuffer[#inbuffer + 1] = assert(unistd.read(res.fd, 65536))
        if not inbuffer[#inbuffer]:find '\n' then return end
        local data = table.concat(inbuffer)
        inbuffer = {}
        for line, nl in data:gmatch '([^\n]*)(\n?)' do
            if nl ~= '' then res.on_line(line)
            elseif line ~= '' then table.insert(inbuffer, line) end
        end
    end}
    local function onclose(fd, event)
        local wait = require 'posix.sys.wait'
        wait.wait(res.pid, wait.WNOHANG)
        if args.on_close then args.on_close() end
    end
    args.fileselector:register{res.fd, ERR=onclose, HUP=onclose, NVAL=onclose}
    return res
end

function POpen:write(data)
    assert(type(data) == 'string')
    assert(not self.closed, 'once the pipe is closed it no longer accepts any data')
    if data == '' then return end
    self.outbuffer[#self.outbuffer + 1] = data
    self.fileselector:register{self.fd, OUT=function()
        data = data:sub(1 + assert(unistd.write(self.fd, table.concat(self.outbuffer))))
        if data == '' then
            self.outbuffer = {}
            self.fileselector:unregister(self.fd, 'OUT')
            if self.closed then self:sendeof() end -- shutdown requested by local end,
        else
            self.outbuffer = {data}
        end
    end}
end

function POpen:shutdown(force) -- just send eof by default
    if force then
        unistd.close(self.fd)
    elseif #self.outbuffer == 0 then
        self:sendeof()
    end
    self.closed = true
end

function POpen:sendeof()
    local termio = require 'posix.termio'
    local ios = termio.tcgetattr(self.fd)
    -- ios.cc[VEOF] should be a byte 4 = (^D)
    if not self.eof_sent then
        self.eof_sent = true
        self.closed = false -- let's unclose the output channel just for this exception :P
        self:write(string.char(ios.cc[termio.VEOF]))
        self.closed = true
    end
end

-----------------------------------------------------------------------
-- FileSelector: allows somewhat easier non-blocking IO compared to poll/select
-----------------------------------------------------------------------

local FileSelector = {}
FileSelector.__index = FileSelector

function FileSelector:new()
    return setmetatable({fds={}, callbacks={}}, self)
end

-- can be also called as :register{fd, EVENTNAME=function(fd) ... end, ...}
-- valid events are: IN, PRI, OUT, ERR, HUP and NVAL
-- for more information read: man 2 poll
function FileSelector:register(fd, event, callback)
    if type(fd) == 'table' then
        for k, v in pairs(fd) do if type(k) == 'string' then
            self:register(fd[1], k, v)
        end end
        return
    end
    self.fds[fd] = self.fds[fd] or {events={}, revents={}}
    if ({IN=1, PRI=1, OUT=1})[event] then
        self.fds[fd].events[event] = true
    elseif not ({ERR=1, HUP=1, NVAL=1})[event] then
        return error('invalid event '..event)
    end
    self.callbacks[fd] = self.callbacks[fd] or {}
    self.callbacks[fd][event] = callback
end

function FileSelector:unregister(fd, event)
    if self.fds[fd] and self.fds[fd].events[event] then
        self.fds[fd].events[event] = false
    end
    if self.callbacks[fd] and self.callbacks[fd][event] then
        self.callbacks[fd][event] = nil
    end
end

function FileSelector:interrupt()
    self.interrupted = true
end

function FileSelector:run(timeout)
    while not self.interrupted do
        local t = assert(poll.poll(self.fds, timeout or -1))
        if t == 0 then return 'TIMEDOUT' end
        for fd, pollfd in pairs(self.fds) do
            for event, activated in pairs(pollfd.revents) do
                if activated and self.callbacks[fd][event] then self.callbacks[fd][event](fd, event) end
            end
            if pollfd.revents.HUP or pollfd.revents.ERR then
                unistd.close(fd)
                self.callbacks[fd] = nil
                self.fds[fd] = nil
            elseif pollfd.revents.NVAL then
                self.callbacks[fd] = nil
                self.fds[fd] = nil
            end
        end
    end
    self.interrupted = nil
end

-----------------------------------------------------------------------
-- Future
-----------------------------------------------------------------------

local Future = {}
Future.__index = Future

function Future:new(value)
    return setmetatable({completed=value ~= nil, failed=false, value=value}, self)
end

function Future:complete(value, failed)
    assert(not self.completed, 'Futures can be completed only once')
    self.get = function() return self.failed and error(value) or value end
    self.completed, self.value = true, value
    self.failed = failed or false
end

function Future:completewith(fn)
    local ok, res = pcall(fn)
    self:complete(res, not ok) -- failed == not ok
end

-----------------------------------------------------------------------
-- SshDispatcher
-----------------------------------------------------------------------

local SshDispatcher = {}
SshDispatcher.__index = SshDispatcher

-- args:
--     servers: table of server_id -> description, where description is a table with:
--         user: username (optional),
--         host: host,
--         options: additional ssh options (passed with ssh's -o parameter if defined, optional),
--                 you may probably need StrictHostKeyChecking=no, but be careful if you use it!
--         command: command line to run in the server that starts the worker,
--         connection: this will be set by the SshDispatcher once a connection is created
--     maxservers: number of maximum servers used,
--     sshclient: path for the ssh client (default 'ssh', optional),
--     serializer: Serializer instance to be used for __call marshalling (defaults to a new instance, optional)
--     fileselector: FileSelector instance to be used (defaults to a new instance, optional)
function SshDispatcher:new(args)
    return setmetatable({servers=args.servers,
            maxservers=args.maxservers,
            sshclient=args.sshclient or 'ssh',
            serializer=args.serializer or Serializer:new(),
            fileselector=args.fileselector or FileSelector:new(),
            available={}, -- set of available pipes
            working={}, -- set of working pipes
            queue={rp=1,wp=1} -- queue of pending works in pairs {arg, future}
            }, self)
end

local onavailable

local function trymakingavailable(self)
    local working_count = 0
    for _ in pairs(self.working) do
        working_count = working_count + 1
    end
    local pipe
    if next(self.available) then
        pipe = next(self.available)
    elseif working_count < self.maxservers then
        local id, s
        for sid, server in pairs(self.servers) do
            if not server.connection then id, s = sid, server break end
        end
        if not s then return end
        local argt = {'-t', '-q'}
        if s.options then argt[#argt + 1] = '-o'; argt[#argt + 1] = s.options end
        argt[#argt + 1] = s.user and (s.user .. '@' .. s.host) or s.host
        argt[#argt + 1] = s.command
        pipe = POpen:new{fileselector=self.fileselector, on_line=function(line)
                return error('unexpected line received from a newly created pipe: ' .. line)
            end, on_close = function()
                s.connection = nil
                self.fileselector:interrupt()
                self.available[pipe] = nil
                if pipe.work then
                    self.working[pipe] = nil
                    self.servers[id] = nil
                    self.queue[self.queue.wp] = pipe.work
                    self.queue.wp = self.queue.wp + 1
                    --self.work[1]:fail('pipe closed while working')
                    --return error('pipe closed while working')
                end
                trymakingavailable(self)
            end,
            path=self.sshclient, argt=argt}
        s.connection = pipe
    end
    if pipe then onavailable(self, pipe) end
end

function onavailable(self, pipe)
    self.fileselector:interrupt()
    if self.queue.rp < self.queue.wp then
        local arg, future = (unpack or table.unpack)(self.queue[self.queue.rp])
        self.available[pipe] = nil
        self.working[pipe] = 1
        self.queue[self.queue.rp] = nil
        self.queue.rp = self.queue.rp + 1
        pipe:write(arg .. '\n')
        pipe.work = {arg, future}
        pipe.on_line = function(line)
            onavailable(self, pipe)
            future:completewith(function()
                local result = self.serializer:deserialize(line)
                assert(result.tag == 'work', 'received non work tagged result, make sure servers\' "command" is set correctly')
                if result.failed then return error(result.arg) end
                return result.arg
            end)
        end
    else
        pipe.on_line = function(line) return error('unexpected line on available pipe: ' .. line) end
        pipe.work = nil
        self.working[pipe] = nil
        self.available[pipe] = 1
    end
end

-- Calls to this function will be dispatched to the servers and run by the workers,
-- the result will be a "Future" object with a get() method that may block until the answer
-- is received; since the parameters and the response will be serialized, they must conform
-- to Serializers' restrictions.
function SshDispatcher:__call(arg)
    arg = self.serializer:serialize({tag='work', arg=arg})
    local res = Future:new()
    self.queue[self.queue.wp] = {arg, res}
    self.queue.wp = self.queue.wp + 1
    res.get = function()
        while not res.completed do self.fileselector:run() end
        return res.failed and error(res.value) or res.value
    end
    trymakingavailable(self)
    return res
end

-- close any opened servers
function SshDispatcher:shutdown()
    if next(self.working) then
        return error('shutdown is not allowed with workers working')
    end
    for id, server in pairs(self.servers) do
        if server.connection then server.connection:shutdown() end
        while server.connection do
            server.connection:shutdown()
            if self.fileselector:run(1000) == 'TIMEDOUT' then
                print('timed out while waiting for pipe gentle death')
                server.connection:shutdown(true)
            end
        end
    end
end

-----------------------------------------------------------------------
if false then -- test:
    local fs, p
    fs = FileSelector:new()
    p = POpen:new{fileselector=fs, on_line=function(line)
            assert(line == '6', '2*3 should be equal to 6... right?')
            p:shutdown()
        end, on_close=function() fs:interrupt() end,
        path='awk', argt={'{print $1 * $2}'}}
    p:write('2 3\n')
    fs:run()
end

if false then -- another test
    local d = SshDispatcher:new{servers={fideo={host='fideo', command='sed "s/@@@/`hostname`/"'}}, maxservers=1}
    local f1, f2 = d('running desde @@@'), d('running desde @@@')
    print('test:', f1.get(), f2.get())
    d:shutdown()
end

return {
    Serializer = Serializer,
    POpen = POpen,
    FileSelector = FileSelector,
    SshDispatcher = SshDispatcher,
    work = work,
}
