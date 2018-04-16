# sshdispatcher

* Have you ever needed to run a CPU intensive process across multiple machines using only SSH for connectivity?
* Maybe you also need low latency for those tasks, so opening a new connection for each one is also a no-no... How about also supporting connection pooling?
* Need native lua support?
* Tired of complex error recovery mechanisms?
* Too many dependencies? binary libraries even?

Then this is the solution for you!
```
+----------------------------------------------------+                 +----------+       LET
|                                                    |         +-----> | Server 1 | <-+   SOME-
|   d = require 'sshdispatcher'.SshDispatcher:new{   |         |       +----------+   |   ONE
|           maxservers=N, servers=myservers}         |         |                      |   ELSE
|                                                    |         |       +----------+   |   HANDLE
|   f1 = d('this call will send a string')           +---------+  +--> | Server 2 |   |   THE
|   f2 = d{'and this will send', 1, 'list'}          |--------.---+    +----------+   |   HASSLE
|   f3 = d{tables='ok', even={recursive=true}}       +------+ |Oops! Server2 is dead!,|   OF
|                                                    |      | | retry in another one! |   RE-
|   print(f1.get())                                  | <--+ | +-----------------------+   CONNEC-
|                                                    |    | |                             TIONS
|   f4 = d{['here goes f3']=f3.get(), 'work!'}       +--+ | |          +----------+       SERIALI-
|                                                    |  | | +--------> | Server 3 +--+    ZATION
|   print(f1.get(), f2.get(), f3.get(), f4.get())    |  | |            +-----^----+  |    AND
|                                                    |  | |                  |       |    COMPLEX
+----------------------------------------------------+  | +--------------------------+    ASYNC
                                                        |                    |            STUFF
                                                        +--------------------+
```

There are four modules inside `sshdispatcher`:
* `Serializer`: the stuff that converts your nice lua objects into strings that travel over ssh (and viceversa). You can even easily extend the Serializer if you wanna send tables with your custom metatables.
* `POpen`: since we call a real ssh client to communicate with the workers (inside the Servers), we need support for bidirectional subprocess communication that lua lacks of.  That's why I wrote an asyncronous one... it's very simple though, and instead of using pipes it uses a pty (to avoid buffering issues).
* `FileSelector`: If you want to write or read to or from several file descriptors (or connections for that matter) without blocking at specific ones we all know that there are very few alternatives: single connection per thread (discarded since we don't want threading dependencies), using socket.select (ugly and adds an additional dependency on socket), using asyncronous stuff like libuv (more binary dependencies), yada, yada...  Thus a posix.poll wrapper for callbacky-event-io was born!
* `Future`: as in most concurrency capable languages, this a "class" whose objects represent the results of computations that may or may not be completed when you are asking for them.  This means that when you do myfuture:get() it will block until its value is ready to be returned, if it was already computed then get will return immediately. If you need to poll its result without blocking you can always read myfuture.completed (which may be true or false).
* `SshDispatcher`: This is the glue logic that calls ssh via POpen, serializes the requests and results via Serializer, queues the requests until available workers appear, calls FileSelector:run() when someone is waiting for their's future:get() result, and returns the Future objects associated to the requests, completing them when they're done.

## How to use it
For the **client** side:
* Create a SshDispacher instance, specifying a map with your servers: `servers={ server1={host='myserver',command='...'}, server2={host='myserver2',command='...'} }`.
* Call the instance with a serializable argument, it will be sent to one of your servers.  For each call a Future object will be returned,
* Call `:get()` to the returned Futures. If everything went right the worker's result will be returned by get.
* You create as many requests as you want. Also, there's no specific order needed for get, call them whenever you need them.
* Once you're done, you may call `:shutdown()` on the SshDispatcher instance to close any open connection.

Example:
```lua
local dispatcher = require 'sshdispatcher'.SshDispatcher{
        servers = { server1={host='myserver',command='luajit worker.lua'},
                    server2={host='myserver2',command='luajit worker.lua'} },
        maxservers = 2}
local futures = {}
for i = 1, 10 do
    futures[i] = dispatcher{question="what's your hostname?", i=i}
end
for _, f in ipairs(futures) do print(f.get()) end
```

For the **server** (worker) side:
* Create a lua script that DOESN'T print ANYTHING to stdout, and that at some point it calls `work` with a callback.  If SshDispatcher feels the need to call this worker, it will start this program and communicate via stdin/stdout with it (that's why you shouldn't use print/input in it).  Here in these callbacks you'll be able to implement the workers logic, so that each time the client makes a request (via the SshDispatcher instance) that request will be sent to one of your servers, and your callback inside work will receive that very same argument. Once your work is done you may return the corresponding result.

```lua
...
require 'sshdispatcher'.work{function(arg)
    assert(arg.question=="what's your hostname")
    local f = assert(io.popen('hostname', 'r'))
    local h = read '*line'
    f:close()
    return string.format("%i's hostname is %s", arg.i, h)
end}
```
Remember the `command` setting in your servers map? (on the client side)... well, that parameter has to have a string with the command that starts this worker script you've written.

Presto!
