-- Prosody IM
-- Copyright (C) 2016-2018 Kim Alvefur
--
-- This project is MIT/X11 licensed. Please see the
-- COPYING file in the source package for more information.
--


local t_insert = table.insert;
local t_concat = table.concat;
local setmetatable = setmetatable;
local pcall = pcall;
local type = type;
local next = next;
local pairs = pairs;
local ipairs = ipairs;
local traceback = debug.traceback;
local logger = require "util.logger";
local log = logger.init("server_epoll");
local socket = require "socket";
local luasec = require "ssl";
local realtime = require "util.time".now;
local monotonic = require "util.time".monotonic;
local indexedbheap = require "util.indexedbheap";
local createtable = require "util.table".create;
local inet = require "util.net";
local inet_pton = inet.pton;
local _SOCKETINVALID = socket._SOCKETINVALID or -1;
local new_id = require "util.id".medium;
local xpcall = require "util.xpcall".xpcall;

local poller = require "util.poll"
local EEXIST = poller.EEXIST;
local ENOENT = poller.ENOENT;

local poll = assert(poller.new());

local _ENV = nil;
-- luacheck: std none

local default_config = { __index = {
	-- If a connection is silent for this long, close it unless onreadtimeout says not to
	read_timeout = 14 * 60;

	-- How long to wait for a socket to become writable after queuing data to send
	send_timeout = 180;

	-- How long to wait for a socket to become writable after creation
	connect_timeout = 20;

	-- Some number possibly influencing how many pending connections can be accepted
	tcp_backlog = 128;

	-- If accepting a new incoming connection fails, wait this long before trying again
	accept_retry_interval = 10;

	-- If there is still more data to read from LuaSockets buffer, wait this long and read again
	read_retry_delay = 1e-06;

	-- Size of chunks to read from sockets
	read_size = 8192;

	-- Timeout used during between steps in TLS handshakes
	ssl_handshake_timeout = 60;

	-- Maximum and minimum amount of time to sleep waiting for events (adjusted for pending timers)
	max_wait = 86400;
	min_wait = 1e-06;

	-- Enable extra noisy debug logging
	-- TODO disable once considered stable
	verbose = true;

	-- EXPERIMENTAL
	-- Whether to kill connections in case of callback errors.
	fatal_errors = false;

	-- Or disable protection (like server_select) for potential performance gains
	protect_listeners = true;

	-- Attempt writes instantly
	opportunistic_writes = false;
}};
local cfg = default_config.__index;

local fds = createtable(10, 0); -- FD -> conn

-- Timer and scheduling --

local timers = indexedbheap.create();

local function noop() end
local function closetimer(id)
	timers:remove(id);
end

local function reschedule(id, time)
	time = monotonic() + time;
	timers:reprioritize(id, time);
end

-- Add relative timer
local function addtimer(timeout, f, param)
	local time = monotonic() + timeout;
	if param ~= nil then
		local timer_callback = f
		function f(current_time, timer_id)
			local t = timer_callback(current_time, timer_id, param)
			return t;
		end
	end
	local id = timers:insert(f, time);
	return id;
end

-- Run callbacks of expired timers
-- Return time until next timeout
local function runtimers(next_delay, min_wait)
	-- Any timers at all?
	local elapsed = monotonic();
	local now = realtime();
	local peek = timers:peek();
	local readd;
	while peek do

		if peek > elapsed then
			break;
		end

		local _, timer, id = timers:pop();
		local ok, ret = xpcall(timer, traceback, now, id);
		if ok and type(ret) == "number"  then
			local next_time = elapsed+ret;
			-- Delay insertion of timers to be re-added
			-- so they don't get called again this tick
			if readd then
				readd[id] = { timer, next_time };
			else
				readd = { [id] = { timer, next_time } };
			end
		elseif not ok then
			log("error", "Error in timer: %s", ret);
		end

		peek = timers:peek();
	end

	if readd then
		for _, timer in pairs(readd) do
			timers:insert(timer[1], timer[2]);
		end
		peek = timers:peek();
	end

	if peek == nil then
		return next_delay;
	else
		next_delay = peek - elapsed;
	end

	if next_delay < min_wait then
		return min_wait;
	end
	return next_delay;
end

-- Socket handler interface

local interface = {};
local interface_mt = { __index = interface };

function interface_mt:__tostring()
	if self.sockname and self.peername then
		return ("FD %d (%s, %d, %s, %d)"):format(self:getfd(), self.peername, self.peerport, self.sockname, self.sockport);
	elseif self.sockname or self.peername then
		return ("FD %d (%s, %d)"):format(self:getfd(), self.sockname or self.peername, self.sockport or self.peerport);
	end
	return ("FD %d"):format(self:getfd());
end

interface.log = log;
function interface:debug(msg, ...)
	self.log("debug", msg, ...);
end

interface.noise = interface.debug;
function interface:noise(msg, ...)
	if cfg.verbose then
		return self:debug(msg, ...);
	end
end

function interface:error(msg, ...)
	self.log("error", msg, ...);
end

-- Replace the listener and tell the old one
function interface:setlistener(listeners, data)
	self:on("detach");
	self.listeners = listeners;
	self:on("attach", data);
end

-- Call a listener callback
function interface:on(what, ...)
	if not self.listeners then
		self:error("Interface is missing listener callbacks");
		return;
	end
	local listener = self.listeners["on"..what];
	if not listener then
		self:noise("Missing listener 'on%s'", what); -- uncomment for development and debugging
		return;
	end
	if not cfg.protect_listeners then
		return listener(self, ...);
	end
	local onerror = self.listeners.onerror or traceback;
	local ok, err = xpcall(listener, onerror, self, ...);
	if not ok then
		if cfg.fatal_errors then
			self:error("Closing due to error calling on%s: %s", what, err);
			self:destroy();
		else
			self:error("Error calling on%s: %s", what, err);
		end
		return nil, err;
	end
	return err;
end

-- Allow this one to be overridden
function interface:onincoming(...)
	return self:on("incoming", ...);
end

-- Return the file descriptor number
function interface:getfd()
	if self.conn then
		return self.conn:getfd();
	end
	return _SOCKETINVALID;
end

function interface:server()
	return self._server or self;
end

-- Get IP address
function interface:ip()
	return self.peername or self.sockname;
end

-- Get a port number, doesn't matter which
function interface:port()
	return self.sockport or self.peerport;
end

-- Get local port number
function interface:clientport()
	return self.sockport;
end

-- Get remote port
function interface:serverport()
	if self.sockport then
		return self.sockport;
	elseif self._server then
		self._server:port();
	end
end

-- Return underlying socket
function interface:socket()
	return self.conn;
end

function interface:set_mode(new_mode)
	self.read_size = new_mode;
end

function interface:setoption(k, v)
	-- LuaSec doesn't expose setoption :(
	local ok, ret, err = pcall(self.conn.setoption, self.conn, k, v);
	if not ok then
		self:noise("Setting option %q = %q failed: %s", k, v, ret);
		return ok, ret;
	elseif not ret then
		self:noise("Setting option %q = %q failed: %s", k, v, err);
		return ret, err;
	end
	return ret;
end

-- Timeout for detecting dead or idle sockets
function interface:setreadtimeout(t)
	if t == false then
		if self._readtimeout then
			closetimer(self._readtimeout);
			self._readtimeout = nil;
		end
		return
	end
	t = t or cfg.read_timeout;
	if self._readtimeout then
		reschedule(self._readtimeout, t);
	else
		self._readtimeout = addtimer(t, function ()
			if self:on("readtimeout") then
				self:noise("Read timeout handled");
				return cfg.read_timeout;
			else
				self:debug("Read timeout not handled, disconnecting");
				self:on("disconnect", "read timeout");
				self:destroy();
			end
		end);
	end
end

-- Timeout for detecting dead sockets
function interface:setwritetimeout(t)
	if t == false then
		if self._writetimeout then
			closetimer(self._writetimeout);
			self._writetimeout = nil;
		end
		return
	end
	t = t or cfg.send_timeout;
	if self._writetimeout then
		reschedule(self._writetimeout, t);
	else
		self._writetimeout = addtimer(t, function ()
			self:noise("Write timeout");
			self:on("disconnect", self._connected and "write timeout" or "connection timeout");
			self:destroy();
		end);
	end
end

function interface:add(r, w)
	local fd = self:getfd();
	if fd < 0 then
		return nil, "invalid fd";
	end
	if r == nil then r = self._wantread; end
	if w == nil then w = self._wantwrite; end
	local ok, err, errno = poll:add(fd, r, w);
	if not ok then
		if errno == EEXIST then
			self:debug("FD already registered in poller! (EEXIST)");
			return self:set(r, w); -- So try to change its flags
		end
		self:debug("Could not register in poller: %s(%d)", err, errno);
		return ok, err;
	end
	self._wantread, self._wantwrite = r, w;
	fds[fd] = self;
	self:noise("Registered in poller");
	return true;
end

function interface:set(r, w)
	local fd = self:getfd();
	if fd < 0 then
		return nil, "invalid fd";
	end
	if r == nil then r = self._wantread; end
	if w == nil then w = self._wantwrite; end
	local ok, err, errno = poll:set(fd, r, w);
	if not ok then
		self:debug("Could not update poller state: %s(%d)", err, errno);
		return ok, err;
	end
	self._wantread, self._wantwrite = r, w;
	return true;
end

function interface:del()
	local fd = self:getfd();
	if fd < 0 then
		return nil, "invalid fd";
	end
	if fds[fd] ~= self then
		return nil, "unregistered fd";
	end
	local ok, err, errno = poll:del(fd);
	if not ok and errno ~= ENOENT then
		self:debug("Could not unregister: %s(%d)", err, errno);
		return ok, err;
	end
	self._wantread, self._wantwrite = nil, nil;
	fds[fd] = nil;
	self:noise("Unregistered from poller");
	return true;
end

function interface:setflags(r, w)
	if not(self._wantread or self._wantwrite) then
		if not(r or w) then
			return true; -- no change
		end
		return self:add(r, w);
	end
	if not(r or w) then
		return self:del();
	end
	return self:set(r, w);
end

-- Called when socket is readable
function interface:onreadable()
	local data, err, partial = self.conn:receive(self.read_size or cfg.read_size);
	if data then
		self:onconnect();
		self:onincoming(data);
	else
		if err == "wantread" then
			self:set(true, nil);
			err = "timeout";
		elseif err == "wantwrite" then
			self:set(nil, true);
			err = "timeout";
		elseif err == "timeout" and not self._connected then
			err = "connection timeout";
		end
		if partial and partial ~= "" then
			self:onconnect();
			self:onincoming(partial, err);
		end
		if err ~= "timeout" then
			if err == "closed" then
				self:debug("Connection closed by remote");
			else
				self:debug("Read error, closing (%s)", err);
			end
			self:on("disconnect", err);
			self:destroy()
			return;
		end
	end
	if not self.conn then return; end
	if self._limit and (data or partial) then
		local cost = self._limit * #(data or partial);
		if cost > cfg.min_wait then
			self:setreadtimeout(false);
			self:pausefor(cost);
			return;
		end
	end
	if self._wantread and self.conn:dirty() then
		self:setreadtimeout(false);
		self:pausefor(cfg.read_retry_delay);
	else
		self:setreadtimeout();
	end
end

-- Called when socket is writable
function interface:onwritable()
	self:onconnect();
	if not self.conn then return; end -- could have been closed in onconnect
	local buffer = self.writebuffer;
	local data = #buffer == 1 and buffer[1] or t_concat(buffer);
	local ok, err, partial = self.conn:send(data);
	if ok then
		self:set(nil, false);
		for i = #buffer, 1, -1 do
			buffer[i] = nil;
		end
		self:setwritetimeout(false);
		self:ondrain(); -- Be aware of writes in ondrain
		return;
	elseif partial then
		self:debug("Sent %d out of %d buffered bytes", partial, #data);
		buffer[1] = data:sub(partial+1);
		for i = #buffer, 2, -1 do
			buffer[i] = nil;
		end
		self:set(nil, true);
		self:setwritetimeout();
	end
	if err == "wantwrite" or err == "timeout" then
		self:set(nil, true);
	elseif err == "wantread" then
		self:set(true, nil);
	elseif err ~= "timeout" then
		self:on("disconnect", err);
		self:destroy();
	end
end

-- The write buffer has been successfully emptied
function interface:ondrain()
	return self:on("drain");
end

-- Add data to write buffer and set flag for wanting to write
function interface:write(data)
	local buffer = self.writebuffer;
	if buffer then
		t_insert(buffer, data);
	else
		self.writebuffer = { data };
	end
	if not self._write_lock then
		if cfg.opportunistic_writes and not self._opportunistic_write then
			self._opportunistic_write = true;
			self:onwritable();
			self._opportunistic_write = nil;
			return #data;
		end
		self:setwritetimeout();
		self:set(nil, true);
	end
	return #data;
end
interface.send = interface.write;

-- Close, possibly after writing is done
function interface:close()
	if self.writebuffer and self.writebuffer[1] then
		self:set(false, true); -- Flush final buffer contents
		self:setwritetimeout();
		self.write, self.send = noop, noop; -- No more writing
		self:debug("Close after writing remaining buffered data");
		self.ondrain = interface.close;
	else
		self:debug("Closing now");
		self.write, self.send = noop, noop;
		self.close = noop;
		self:on("disconnect");
		self:destroy();
	end
end

function interface:destroy()
	self:del();
	self:setwritetimeout(false);
	self:setreadtimeout(false);
	self.onreadable = noop;
	self.onwritable = noop;
	self.destroy = noop;
	self.close = noop;
	self.on = noop;
	self.conn:close();
	self.conn = nil;
end

function interface:ssl()
	return self._tls;
end

function interface:set_sslctx(sslctx)
	self._sslctx = sslctx;
end

function interface:starttls(tls_ctx)
	if tls_ctx then self.tls_ctx = tls_ctx; end
	self.starttls = false;
	if self.writebuffer and self.writebuffer[1] then
		self:debug("Start TLS after write");
		self.ondrain = interface.starttls;
		self:set(nil, true); -- make sure wantwrite is set
	else
		if self.ondrain == interface.starttls then
			self.ondrain = nil;
		end
		self.onwritable = interface.inittls;
		self.onreadable = interface.inittls;
		self:set(true, true);
		self:setreadtimeout(cfg.ssl_handshake_timeout);
		self:setwritetimeout(cfg.ssl_handshake_timeout);
		self:debug("Prepared to start TLS");
	end
end

function interface:inittls(tls_ctx, now)
	if self._tls then return end
	if tls_ctx then self.tls_ctx = tls_ctx; end
	self._tls = true;
	self:debug("Starting TLS now");
	self:updatenames(); -- Can't getpeer/sockname after wrap()
	local ok, conn, err = pcall(luasec.wrap, self.conn, self.tls_ctx);
	if not ok then
		conn, err = ok, conn;
		self:debug("Failed to initialize TLS: %s", err);
	end
	if not conn then
		self:on("disconnect", err);
		self:destroy();
		return conn, err;
	end
	conn:settimeout(0);
	self.conn = conn;
	if conn.sni then
		if self.servername then
			conn:sni(self.servername);
		elseif self._server and type(self._server.hosts) == "table" and next(self._server.hosts) ~= nil then
			conn:sni(self._server.hosts, true);
		end
	end
	if self.extra and self.extra.tlsa and conn.settlsa then
		-- TODO Error handling
		if not conn:setdane(self.servername or self.extra.dane_hostname) then
			self:debug("Could not enable DANE on connection");
		else
			self:debug("Enabling DANE with %d TLSA records", #self.extra.tlsa);
			self:noise("DANE hostname is %q", self.servername or self.extra.dane_hostname);
			for _, tlsa in ipairs(self.extra.tlsa) do
				self:noise("TLSA: %q", tlsa);
				conn:settlsa(tlsa.use, tlsa.select, tlsa.match, tlsa.data);
			end
		end
	end
	self:on("starttls");
	self.ondrain = nil;
	self.onwritable = interface.tlshandshake;
	self.onreadable = interface.tlshandshake;
	if now then
		return self:tlshandshake()
	end
	self:setreadtimeout(cfg.ssl_handshake_timeout);
	self:setwritetimeout(cfg.ssl_handshake_timeout);
	self:set(true, true);
end

function interface:tlshandshake()
	self:setwritetimeout(false);
	self:setreadtimeout(false);
	self:noise("Continuing TLS handshake");
	local ok, err = self.conn:dohandshake();
	if ok then
		local info = self.conn.info and self.conn:info();
		if type(info) == "table" then
			self:debug("TLS handshake complete (%s with %s)", info.protocol, info.cipher);
		else
			self:debug("TLS handshake complete");
		end
		self.onwritable = nil;
		self.onreadable = nil;
		self:on("status", "ssl-handshake-complete");
		self:set(true, true);
		self:onconnect();
		self:onreadable();
	elseif err == "wantread" then
		self:noise("TLS handshake to wait until readable");
		self:set(true, false);
		self:setreadtimeout(cfg.ssl_handshake_timeout);
	elseif err == "wantwrite" then
		self:noise("TLS handshake to wait until writable");
		self:set(false, true);
		self:setwritetimeout(cfg.ssl_handshake_timeout);
	else
		self:debug("TLS handshake error: %s", err);
		self:on("disconnect", err);
		self:destroy();
	end
end

local function wrapsocket(client, server, read_size, listeners, tls_ctx, extra) -- luasocket object -> interface object
	client:settimeout(0);
	local conn_id = ("conn%s"):format(new_id());
	local conn = setmetatable({
		conn = client;
		_server = server;
		created = realtime();
		listeners = listeners;
		read_size = read_size or (server and server.read_size);
		writebuffer = {};
		tls_ctx = tls_ctx or (server and server.tls_ctx);
		tls_direct = server and server.tls_direct;
		id = conn_id;
		log = logger.init(conn_id);
		extra = extra;
	}, interface_mt);

	if extra then
		if extra.servername then
			conn.servername = extra.servername;
		end
	end

	conn:updatenames();
	return conn;
end

function interface:updatenames()
	local conn = self.conn;
	local ok, peername, peerport = pcall(conn.getpeername, conn);
	if ok and peername then
		self.peername, self.peerport = peername, peerport or 0;
	end
	local ok, sockname, sockport = pcall(conn.getsockname, conn);
	if ok and sockname then
		self.sockname, self.sockport = sockname, sockport or 0;
	end
end

-- A server interface has new incoming connections waiting
-- This replaces the onreadable callback
function interface:onacceptable()
	local conn, err = self.conn:accept();
	if not conn then
		self:debug("Error accepting new client: %s, server will be paused for %ds", err, cfg.accept_retry_interval);
		self:pausefor(cfg.accept_retry_interval);
		return;
	end
	local client = wrapsocket(conn, self, nil, self.listeners);
	client:debug("New connection %s on server %s", client, self);
	client:defaultoptions();
	if self.tls_direct then
		client:add(true, true);
		client:inittls(self.tls_ctx, true);
	else
		client:add(true, false);
		client:onconnect();
		client:onreadable();
	end
end

-- Initialization for outgoing connections
function interface:init()
	self:setwritetimeout(cfg.connect_timeout);
	self:defaultoptions();
	return self:add(true, true);
end

-- luacheck: ignore 212/self
function interface:defaultoptions()
end

function interface:pause()
	self:noise("Pause reading");
	return self:set(false);
end

function interface:resume()
	self:noise("Resume reading");
	return self:set(true);
end

-- Pause connection for some time
function interface:pausefor(t)
	self:noise("Pause for %fs", t);
	if self._pausefor then
		closetimer(self._pausefor);
		self._pausefor = nil;
	end
	if t == false then return; end
	self:set(false);
	self._pausefor = addtimer(t, function ()
		self._pausefor = nil;
		self:set(true);
		self:noise("Resuming after pause, connection is %s", not self.conn and "missing" or self.conn:dirty() and "dirty" or "clean");
		if self.conn and self.conn:dirty() then
			self:onreadable();
		end
	end);
end

function interface:setlimit(Bps)
	if Bps > 0 then
		self._limit = 1/Bps;
	else
		self._limit = nil;
	end
end

function interface:pause_writes()
	if self._write_lock then
		return
	end
	self:noise("Pause writes");
	self._write_lock = true;
	self:setwritetimeout(false);
	self:set(nil, false);
end

function interface:resume_writes()
	if not self._write_lock then
		return
	end
	self:noise("Resume writes");
	self._write_lock = nil;
	if self.writebuffer[1] then
		self:setwritetimeout();
		self:set(nil, true);
	end
end

-- Connected!
function interface:onconnect()
	self._connected = true;
	self:updatenames();
	self:debug("Connected (%s)", self);
	self.onconnect = noop;
	self:on("connect");
end

local function wrapserver(conn, addr, port, listeners, config)
	local server = setmetatable({
		conn = conn;
		created = realtime();
		listeners = listeners;
		read_size = config and config.read_size;
		onreadable = interface.onacceptable;
		tls_ctx = config and config.tls_ctx;
		tls_direct = config and config.tls_direct;
		hosts = config and config.sni_hosts;
		sockname = addr;
		sockport = port;
		log = logger.init(("serv%s"):format(new_id()));
	}, interface_mt);
	server:debug("Server %s created", server);
	server:add(true, false);
	return server;
end

local function listen(addr, port, listeners, config)
	local conn, err = socket.bind(addr, port, cfg.tcp_backlog);
	if not conn then return conn, err; end
	conn:settimeout(0);
	return wrapserver(conn, addr, port, listeners, config);
end

-- COMPAT
local function addserver(addr, port, listeners, read_size, tls_ctx)
	return listen(addr, port, listeners, {
		read_size = read_size;
		tls_ctx = tls_ctx;
		tls_direct = tls_ctx and true or false;
	});
end

-- COMPAT
local function wrapclient(conn, addr, port, listeners, read_size, tls_ctx, extra)
	local client = wrapsocket(conn, nil, read_size, listeners, tls_ctx, extra);
	if not client.peername then
		client.peername, client.peerport = addr, port;
	end
	local ok, err = client:init();
	if not ok then return ok, err; end
	if tls_ctx then
		client:starttls(tls_ctx);
	end
	return client;
end

-- New outgoing TCP connection
local function addclient(addr, port, listeners, read_size, tls_ctx, typ, extra)
	local create;
	if not typ then
		local n = inet_pton(addr);
		if not n then return nil, "invalid-ip"; end
		if #n == 16 then
			typ = "tcp6";
		else
			typ = "tcp4";
		end
	end
	if typ then
		create = socket[typ];
	end
	if type(create) ~= "function" then
		return nil, "invalid socket type";
	end
	local conn, err = create();
	if not conn then return conn, err; end
	local ok, err = conn:settimeout(0);
	if not ok then return ok, err; end
	local ok, err = conn:setpeername(addr, port);
	if not ok and err ~= "timeout" then return ok, err; end
	local client = wrapsocket(conn, nil, read_size, listeners, tls_ctx, extra)
	local ok, err = client:init();
	if not client.peername then
		-- otherwise not set until connected
		client.peername, client.peerport = addr, port;
	end
	if not ok then return ok, err; end
	client:debug("Client %s created", client);
	if tls_ctx then
		client:starttls(tls_ctx);
	end
	return client, conn;
end

local function watchfd(fd, onreadable, onwritable)
	local conn = setmetatable({
		conn = fd;
		onreadable = onreadable;
		onwritable = onwritable;
		close = function (self)
			self:del();
		end
	}, interface_mt);
	if type(fd) == "number" then
		conn.getfd = function ()
			return fd;
		end;
		-- Otherwise it'll need to be something LuaSocket-compatible
	end
	conn.id = new_id();
	conn.log = logger.init(("fdwatch%s"):format(conn.id));
	conn:add(onreadable, onwritable);
	return conn;
end;

-- Dump all data from one connection into another
local function link(from, to, read_size)
	from:debug("Linking to %s", to.id);
	function from:onincoming(data)
		self:pause();
		to:write(data);
	end
	function to:ondrain() -- luacheck: ignore 212/self
		from:resume();
	end
	from:set_mode(read_size);
	from:set(true, nil);
	to:set(nil, true);
end

-- COMPAT
-- net.adns calls this but then replaces :send so this can be a noop
function interface:set_send(new_send) -- luacheck: ignore 212
end

-- Close all connections and servers
local function closeall()
	for fd, conn in pairs(fds) do -- luacheck: ignore 213/fd
		conn:close();
	end
end

local quitting = nil;

-- Signal main loop about shutdown via above upvalue
local function setquitting(quit)
	if quit then
		quitting = "quitting";
		closeall();
	else
		quitting = nil;
	end
end

-- Main loop
local function loop(once)
	repeat
		local t = runtimers(cfg.max_wait, cfg.min_wait);
		local fd, r, w = poll:wait(t);
		if fd then
			local conn = fds[fd];
			if conn then
				if r then
					conn:onreadable();
				end
				if w then
					conn:onwritable();
				end
			else
				log("debug", "Removing unknown fd %d", fd);
				poll:del(fd);
			end
		elseif r ~= "timeout" and r ~= "signal" then
			log("debug", "epoll_wait error: %s[%d]", r, w);
		end
	until once or (quitting and next(fds) == nil);
	return quitting;
end

return {
	get_backend = function () return "epoll"; end;
	addserver = addserver;
	addclient = addclient;
	add_task = addtimer;
	timer = {
		-- API-compatible with util.timer
		add_task = addtimer;
		stop = closetimer;
		reschedule = reschedule;
		to_absolute_time = function (t)
			return t-monotonic()+realtime();
		end;
	};
	listen = listen;
	loop = loop;
	closeall = closeall;
	setquitting = setquitting;
	wrapclient = wrapclient;
	wrapserver = wrapserver;
	watchfd = watchfd;
	link = link;
	set_config = function (newconfig)
		cfg = setmetatable(newconfig, default_config);
	end;

	-- libevent emulation
	event = { EV_READ = "r", EV_WRITE = "w", EV_READWRITE = "rw", EV_LEAVE = -1 };
	addevent = function (fd, mode, callback)
		log("warn", "Using deprecated libevent emulation, please update code to use watchfd API instead");
		local function onevent(self)
			local ret = self:callback();
			if ret == -1 then
				self:set(false, false);
			elseif ret then
				self:set(mode == "r" or mode == "rw", mode == "w" or mode == "rw");
			end
		end

		local conn = setmetatable({
			getfd = function () return fd; end;
			callback = callback;
			onreadable = onevent;
			onwritable = onevent;
			close = function (self)
				self:del();
				fds[fd] = nil;
			end;
		}, interface_mt);
		conn.id = conn:getfd();
		conn.log = logger.init(("fdwatch%d"):format(conn.id));
		local ok, err = conn:add(mode == "r" or mode == "rw", mode == "w" or mode == "rw");
		if not ok then return ok, err; end
		return conn;
	end;
};
