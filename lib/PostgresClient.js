var sys=require("sys"),
	net=require("net"),
	crypto=require("crypto"),
	PostgresReader=require("./PostgresReader").PostgresReader,
	BufferQueueReader=require("bufferlib/BufferQueueReader").BufferQueueReader,
	EventEmitter=require("events").EventEmitter,
	Constants=require("./PostgresConstants"),
	Messages=require("./PostgresMessages");
	
/*
TODO:
- Handle Asynchronous messages (46.2.6)
*/

function PostgresClient(config) {
	var self=this;
	EventEmitter.call(this);
	this.hostname=config.hostname || "127.0.0.1";
	this.port=config.port || 5432;
	this.username=config.username;
	this.password=config.password;
	this.database=config.database;
	this.connected=false;
	this.closed=false;
	this.closing=false;
	this.socket=net.createConnection(this.port,this.hostname);
	this.buffer=new BufferQueueReader();
	this.outputQueue=[];
	this.handlerQueue=[createPostgresConnectHandler(this)];
	this.currentHandler=undefined;
	this.lastPreparedStatement=0;
	this.lastPortal=0;
	
	bindSocket(this.socket);
	
	function onSocketConnect() {
		var output;
		if (self.socket && !self.closed) {
				var params={
					user: self.username
				};
				if (self.database) {
					params.database=self.database;
				}
				self.socket.write(Messages.StartupMessage(params));
		}
	}
	function onSocketData(data) {
		var type,length,data,handler;
		if (!self.closed) {
			self.buffer.push(data);
			while (!self.closed && self.buffer.length>5) {
				type=self.buffer.readByte(0);
				length=self.buffer.readIntBE(1,4);
				if (self.buffer.length<1+length) {
					break;
				}
				self.buffer.skip(5);
				data=self.buffer.popBuffer(length-4);
				handler=(self.currentHandler || (self.currentHandler=self.handlerQueue.shift()) || self.mainHandler);
				try {
					if (handler.call(self,undefined,type,data)) {
						self.currentHandler=undefined;
					}
				}
				catch(e) {
					if (!self.closed) {
						self.closed=true;
						self.socket.destroy();
						self.emit("error",e);
					}
				}
			}
		}
	}
	function onSocketEnd() {
		unbindSocket(self.socket);
		self.socket=undefined;
		if (!self.closed) {
			self.closed=true;
			self.emit("end");
		}
	}
	function onSocketError(err) {
		unbindSocket(self.socket);
		self.socket=undefined;
		if (!self.closed) {
			self.closed=true;
			self.emit("error",err);
		}
	}
	function bindSocket(s) {
		s.on("connect",onSocketConnect);
		s.on("data",onSocketData);
		s.on("end",onSocketEnd);
		s.on("error",onSocketError);
	}
	function unbindSocket(s) {
		if (!s) {
			return;
		}
		s.removeListener("connect",onSocketConnect);
		s.removeListener("data",onSocketData);
		s.removeListener("end",onSocketEnd);
		s.removeListener("error",onSocketError);
	}
}
sys.inherits(PostgresClient,EventEmitter);
exports.PostgresClient=PostgresClient;

function createPostgresConnectHandler(self) {	
	return function(err,type,data) {
		if (err) {
			throw err;
		}
		var reader,authType,output;
		if (type===Constants.MessageTypes.Backend.ErrorResponse) {
			throw Messages.DecodeErrorResponse(data);
		} else if (type===Constants.MessageTypes.Backend.Authentication) {
			reader=new PostgresReader(data);
			authType=reader.popIntBE(4);
			switch (authType) {
				case Constants.AuthenticationTypes.Ok: break; //Just ignore that, a ReadyForQueue should be right up
				case Constants.AuthenticationTypes.MD5Password:{
					this.socket.write(
						Messages.PasswordMessage(
							"md5"+crypto.createHash('md5').update(
								crypto.createHash('md5').update(this.password,'binary').update(this.username,'binary').digest('hex')
								,'binary'
							).update(reader.popBuffer(4).toString('binary'),'binary').digest('hex')
						)
					);
					break;
				}
				default:{
					if (!self.closed) {
						self.closed=true;
						self.socket.end();
						self.emit("error","Unexpected authentication type "+(Constants.AuthenticationTypesLookup[authType] || authType));
					}
					return true;
					break;
				}
			}
			return false;
		} else if (type===Constants.MessageTypes.Backend.ReadyForQuery) {
			if (!self.connected) {
				self.connected=true;
				output=self.outputQueue;
				self.outputQueue=undefined;
				output.forEach(function(o) {
					self.socket.write(o);
				});
				self.emit("connect");
			}
			return true;
		} else if (
			type==Constants.MessageTypes.Backend.BackendKeyData || 
			type==Constants.MessageTypes.Backend.ParameterStatus || 
			type==Constants.MessageTypes.Backend.NoticeResponse
			)
		{
			//Ignore
		} else {
			throw new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type)));
		}
	}
}
PostgresClient.prototype.mainHandler=function(type,data) {
	//TODO
};
PostgresClient.prototype._send=function(b) {
	if (this.connected) {
		this.socket.write(b);
	} else {
		this.outputQueue.push(b);
	}
};
PostgresClient.prototype._sendMultiple=function(b) {
	if (this.connected) {
		var self=this;
		b.forEach(function(b) {
			self.socket.write(b);
		});
	} else {
		this.outputQueue.push.apply(this.outputQueue,b);
	}
};

function ParseDataRow(datarow,rowinfo) {
	var ret,
		named=rowinfo.some(function(ri) { return (ri.fieldName!=="?column?" && Math.floor(ri.fieldName)!=ri.fieldName); });
	ret=named?{}:[];
	datarow.forEach(function(data,index) {
		var typeoid,name;
		if (index>=rowinfo.length)
			return;
		typeoid=rowinfo[index].datatypeOID;
		name=rowinfo[index].fieldName;
		if (name==="?column?") {
			name=index;
		}
		if (data===null) {
			ret[name]=null;
			return;
		}
		switch (typeoid) {
			case Constants.TypeOIDs.int2:
			case Constants.TypeOIDs.int4:
			case Constants.TypeOIDs.int8:
				ret[name]=parseInt(data.toString('utf8'),10);
				break;
			case Constants.TypeOIDs.bool:
				ret[name]=(data[0]==="t".charCodeAt(0));
				break;
			case Constants.TypeOIDs.unknown:
			case Constants.TypeOIDs.text:
			case Constants.TypeOIDs.cstring:
				ret[name]=data.toString('utf8');
				break;
			case Constants.TypeOIDs.timestamptz:
				ret[name]=new Date(data.toString('utf8').replace(/(\.[0-9]+)/,"").replace(/(\+[0-9]{2})?$/,function(match) { return " GMT"+match; }));
				break;
			default:
				//TODO: How to handle unrecognized types
				ret[name]=data.toString('utf8');
				break;
				//throw new Error("Unrecognized type "+(Constants.TypeOIDsLookup[typeoid] || typeoid));
		}
	});
	return ret;
};

function createQueryHandler(rowinfoCallback,rowCallback,commandCallback,finalCallback) {
	var savedArgs=[undefined],
		returned=false,
		latestRows=undefined,
		latestRowInfo=undefined;
	return function(err,type,data) {
		if (err) {
			if (!returned) {
				returned=true;
				finalCallback(err);
			}
			return true;
		}
		//TODO: Should turn to switch statement
		if (type===Constants.MessageTypes.Backend.ErrorResponse) {
			if (!returned) {
				returned=true;
				finalCallback(Messages.DecodeErrorResponse(data));
			}
		} else if (
			type===Constants.MessageTypes.Backend.ParseComplete ||
			type===Constants.MessageTypes.Backend.BindComplete
		)
		{
			//Ignore these
		} else if (type===Constants.MessageTypes.Backend.RowDescription) {
			latestRowInfo=Messages.DecodeRowDescription(data);
			if (rowinfoCallback) {
				rowinfoCallback(latestRowInfo);
			}
			if (!rowCallback) {
				savedArgs.push(latestRows=[]);
			}
		} else if (type===Constants.MessageTypes.Backend.DataRow) {
			var p=Messages.DecodeDataRow(data);
			if (rowCallback) {
				rowCallback((latestRowInfo && !rowinfoCallback)?ParseDataRow(p,latestRowInfo):p.map(String));
			} else {
				if (!latestRows) {
					savedArgs.push(latestRows=[]);
				}
				latestRows.push(latestRowInfo?ParseDataRow(p,latestRowInfo):p.map(String));
			}
		} else if (type===Constants.MessageTypes.Backend.CommandComplete) {
			var s=Messages.DecodeCommandComplete(data);
			if (commandCallback) {
				commandCallback(s);
			} else {
				savedArgs.push(s);
				latestRowInfo=latestRows=undefined;
			}
		} else if (type===Constants.MessageTypes.Backend.EmptyQueryResponse) {
			//Don't do anything
		} else if (type===Constants.MessageTypes.Backend.PortalSuspended) {
			//Put false on the return stack instead of the CommandComplete return value to indicated the portal is not empty yet.
			savedArgs.push(false);
		} else if (type===Constants.MessageTypes.Backend.ReadyForQuery) {
			if (!returned) {
				returned=true;
				finalCallback.apply(null,savedArgs);
			}
			return true;
		} else {
			if (!returned) {
				returned=true;
				finalCallback(new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type))));
			}
			return false;
		}
	}
};
PostgresClient.prototype.simpleQuery=function(query,callback) {
	var b=Messages.Query(query);
	this.handlerQueue.push(createQueryHandler(undefined,undefined,undefined,callback));
	return this._send(b);
};

function createParseTypes(query) {
	var count=0,ret=[];
	query.replace(/\$([0-9]+)/g,function(match,index) {
		count=Math.max(count,Math.floor(index));
	});
	while (count--) {
		ret.push(Constants.TypeOIDs.text);
	}
	return ret;
}

PostgresClient.prototype.extendedQuery=function(query,params,callback) {
	var self=this,
		b=[
			Messages.Parse("",query,createParseTypes(query)),
			Messages.Bind("","",[],params.map(String),[]), //Maybe: params.map(function() { return Constants.FormatCodes.text; })
			Messages.Describe(Messages.Describe.Portal,""),
			Messages.Execute("",0),
			Messages.Sync()
		];
	this.handlerQueue.push(createQueryHandler(undefined,undefined,undefined,callback));
	this._sendMultiple(b);
};

function createParseHandler(finalCallback,name) {
	var returned=false;
	return function(err,type,data) {
		if (err) {
			throw err;
		}
		switch (type) {
			case Constants.MessageTypes.Backend.ErrorResponse:
				if (!returned) {
					returned=true;
					finalCallback(Messages.DecodeErrorResponse(data));
				}
			case Constants.MessageTypes.Backend.ParseComplete:
			case Constants.MessageTypes.Backend.BindComplete:
				break; //Ignore
			case Constants.MessageTypes.Backend.ReadyForQuery:
				if (!returned) {
					returned=true;
					finalCallback(undefined,name);
				}
				return true;
			default:
				if (!returned) {
					returned=true;
					finalCallback(new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type))));
				}
				return false;
		}
	}
};

PostgresClient.prototype.parse=function(query,callback) {
	var name="generated_statement"+(this.lastPreparedStatement++),
		b=[
			Messages.Parse(name,query,createParseTypes(query)),
			Messages.Sync()
		];
	this.handlerQueue.push(createParseHandler(callback,name));
	return this._sendMultiple(b);
};

PostgresClient.prototype.bindExecute=function(statement,params,callback) {
		b=[
			Messages.Bind("",statement,params.map(function() { return Constants.FormatCodes.text; }),params.map(String),[]),
			Messages.Describe(Messages.Describe.Portal,""),
			Messages.Execute("",0),
			Messages.Sync()
		];
	this.handlerQueue.push(createQueryHandler(undefined,undefined,undefined,callback));
	return this._sendMultiple(b);
};

function createCloseHandler(finalCallback) {
	var returned=false;
	return function(err,type,data) {
		if (err) {
			throw err;
		}
		switch (type) {
			case Constants.MessageTypes.Backend.ErrorResponse:
				if (!returned) {
					returned=true;
					finalCallback(Messages.DecodeErrorResponse(data));
				}
			case Constants.MessageTypes.Backend.CloseComplete:
				break; //Ignore
			default:
				if (!returned) {
					returned=true;
					finalCallback(new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type))));
				}
				return false;
		}
	}
};

PostgresClient.prototype.closeStatement=function(statementName,callback) {
	var name="generated_statement"+(this.lastPreparedStatement++),
		b=Messages.Close(Messages.Close.Statement,statementName);
	this.handlerQueue.push(createParseHandler(callback,name));
	return this._send(b);
};

PostgresClient.prototype.prepare=function(query) {
	var ret={execute:execute,close:close},
		preparedName=undefined,
		preparedErr=undefined,
		preparedCallbacks=[],
		closed,
		self=this;
	self.parse(query,function(err,name) {
		if (err) {
			preparedErr=err;
		} else {
			preparedName=name;
		}
		preparedCallbacks.forEach(function(f) {
			f(err,name);
		});
	});
	return ret;
	
	function execute(params,callback) {
		if (closed) {
			callback(new Error("Already closed"));
			return;
		}
		if (preparedErr) {
			callback(preparedErr);
			return;
		}
		if (preparedName) {
			self.bindExecute(preparedName,params,callback);
			return;
		}
		preparedCallbacks.push(function(err,name) {
			if (err) {
				callback(err);
			} else {
				self.bindExecute(name,params,callback);
			}
		});
	}
	
	function close(callback) {
		if (closed) {
			callback(undefined);
			return;
		}
		closed=true;
		var err=preparedErr,name=preparedName;
		preparedName=preparedErr=undefined;
		if (err) {
			//Wasn't opened in the first place
			callback(undefined);
			return;
		}
		if (name) {
			//Close it
			self.closeStatement(name,callback);
			return;
		}
		//Wait to close it
		preparedCallbacks=[function(err,name) {
			preparedName=preparedErr=undefined;
			if (err) {
				callback(undefined);
			} else {
				self.closeStatement(name,callback);
			}
		}];
	}
};

PostgresClient.prototype.destroy=function() {
	if (!this.closed) {
		this.closed=true;
		if (this.socket) {
			this.socket.end();
			this.socket.destroy();
		}
	}
};
PostgresClient.prototype.end=function() {
	if (this.closed || this.closing) {
		return;
	}
	//Graceful exit?
	this.closing=true;
	if (this.connected) {
		this.socket.write(Messages.Terminate());
		this.socket.end();
	} else {
		this.socket.end();
	}
};