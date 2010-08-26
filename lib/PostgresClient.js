/*global require, exports */
/*jslint onevar: true, undef: true, nomen: true, eqeqeq: true, regexp: true, newcap: true, immed: true */

var sys=require("sys"),
	net=require("net"),
	crypto=require("crypto"),
	PostgresEncoder=require("./PostgresEncoder").PostgresEncoder,
	PostgresReader=require("./PostgresReader").PostgresReader,
	BufferQueueReader=require("bufferlib/BufferQueueReader").BufferQueueReader,
	EventEmitter=require("events").EventEmitter,
	Constants=require("./PostgresConstants");

/*
TODO:
- COPY ... FROM STDIN/TO STDOUT support
*/

function createPostgresConnectHandler(self) {	
	return function(err,type,data) {
		if (err) {
			throw err;
		}
		var reader,authType,output;
		if (type===Constants.MessageTypes.Backend.ErrorResponse) {
			throw (new PostgresReader(data)).popErrorResponse();
		} else if (type===Constants.MessageTypes.Backend.Authentication) {
			reader=new PostgresReader(data);
			authType=reader.popIntBE(4);
			switch (authType) {
				case Constants.AuthenticationTypes.Ok: break; //Just ignore that, a ReadyForQueue should be right up
				case Constants.AuthenticationTypes.MD5Password:
					this.socket.write(
						PostgresEncoder.create().pushPasswordMessage(
							"md5"+crypto.createHash('md5').update(
								crypto.createHash('md5').update(this.password,'binary').update(this.username,'binary').digest('hex'),
								'binary'
							).update(reader.popBuffer(4).toString('binary'),'binary').digest('hex')
						).toBuffer()
					);
					break;
				default:
					if (!self.closed) {
						self.closed=true;
						self.socket.end();
						self.emit("error","Unexpected authentication type "+(Constants.AuthenticationTypesLookup[authType] || authType));
					}
					return true;
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
			type===Constants.MessageTypes.Backend.BackendKeyData || 
			type===Constants.MessageTypes.Backend.ParameterStatus || 
			type===Constants.MessageTypes.Backend.NoticeResponse
			)
		{
			//Ignore
		} else {
			throw new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type)));
		}
	};
}

function createSocketHandlers(self,unbindSocket) {
	function onSocketConnect() {
		var params;
		if (self.socket && !self.closed) {
				params={
					user: self.username
				};
				if (self.database) {
					params.database=self.database;
				}
				self.socket.write(PostgresEncoder.create().pushStartupMessage(params).toBuffer());
		}
	}
	function onSocketData(data) {
		var type,
			length,
			chunk,
			handler;
		if (!self.closed) {
			self.buffer.push(data);
			while (!self.closed && self.buffer.length>5) {
				type=self.buffer.readByte(0);
				length=self.buffer.readIntBE(1,4);
				if (self.buffer.length<1+length) {
					break;
				}
				self.buffer.skip(5);
				chunk=self.buffer.popBuffer(length-4);
				try {
					if (!self.mainHandler(type,chunk)) {
						handler=(self.currentHandler || (self.currentHandler=self.handlerQueue.shift()));
						if (handler) {
							if (handler.call(self,undefined,type,chunk)) {
								self.currentHandler=undefined;
							}
						} else {
							self.unhandledHandler(type,chunk);
						}
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
	
	return {
		connect: onSocketConnect,
		data: onSocketData,
		error: onSocketError,
		end: onSocketEnd
	};
}

function PostgresClient(config) {
	var socketHandlers;
	
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
	
	function bindSocket(s) {
		s.addListener("connect",socketHandlers.connect);
		s.addListener("data",socketHandlers.data);
		s.addListener("end",socketHandlers.end);
		s.addListener("error",socketHandlers.error);
	}
	function unbindSocket(s) {
		if (!s) {
			return;
		}
		s.removeListener("connect",socketHandlers.connect);
		s.removeListener("data",socketHandlers.data);
		s.removeListener("end",socketHandlers.end);
		s.removeListener("error",socketHandlers.error);
	}
	
	socketHandlers=createSocketHandlers(this,unbindSocket);
	bindSocket(this.socket);
}
sys.inherits(PostgresClient,EventEmitter);
exports.PostgresClient=PostgresClient;

PostgresClient.prototype.mainHandler=function(type,data) {
	var temp;
	switch (type) {
		case Constants.MessageTypes.Backend.NoticeResponse:
			this.emit("notice",(new PostgresReader(data)).popNoticeResponse());
			return true;
		case Constants.MessageTypes.Backend.ParameterStatus:
			temp=(new PostgresReader(data)).popParameterStatus();
			this.emit("parameter",temp.name,temp.value);
			return true;
		case Constants.MessageTypes.Backend.NotificationResponse:
			temp=(new PostgresReader(data)).popNotificationResponse();
			this.emit("notification",temp.pid,temp.name,temp.value);
			return true;
		default:
			return false;
	}
};
PostgresClient.prototype.unhandledHandler=function(type,data) {
	if (!this.closed) {
		this.closed=true;
		this.socket.destroy();
		this.emit("error",new Error("Unhandled message "+(Constants.MessageTypesLookup.Backend[type] || type)));
	}
}
PostgresClient.prototype.write=function(b) {
	if (this.connected) {
		this.socket.write(b);
	} else {
		this.outputQueue.push(b);
	}
};

function is_int(x) {
	return (/^[0-9]+$/).test(x);
}

function parseDataRow(datarow,rowinfo) {
	var ret,
		named=rowinfo.some(function(ri) { return (ri.fieldName!=="?column?" && !is_int(ri.fieldName)); });
	if (rowinfo===undefined) {
		return datarow.map(function(x) {
			if (x===null)
				return null;
			return String(x);
		});
	}
	ret=named?{}:[];
	datarow.forEach(function(data,index) {
		var typeoid,name;
		if (index>=rowinfo.length) {
			return;
		}
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
}

function createQueryHandler(callback) {
	var savedArgs=[undefined],
		returned=false,
		latestRows,
		latestRowInfo,
		temp,
		rowinfoCallback=callback.rowinfo,
		rowCallback=callback.row,
		copyoutCallback=callback.copyout,
		copydataCallback=callback.copydata,
		copydoneCallback=callback.copydone,
		commandCallback=callback.commandComplete,
		finalCallback=(callback.complete || callback);
	//rowinfoCallback,rowCallback,commandCallback,finalCallback
	return function(err,type,data) {
		if (err) {
			if (!returned) {
				returned=true;
				finalCallback(err);
			}
			return true;
		}
		switch (type) {
			case Constants.MessageTypes.Backend.ErrorResponse:
				if (!returned) {
					returned=true;
					finalCallback((new PostgresReader(data)).popErrorResponse());
				}
				return false;
			case Constants.MessageTypes.Backend.ParseComplete:
			case Constants.MessageTypes.Backend.BindComplete:
				return false; //Both part of the extended query responses, safe to ignore
			case Constants.MessageTypes.Backend.RowDescription:
				latestRowInfo=(new PostgresReader(data)).popRowDescription();
				if (rowinfoCallback) {
					rowinfoCallback(latestRowInfo);
				}
				if (!rowCallback) {
					latestRows=[];
				}
				return false;
			case Constants.MessageTypes.Backend.DataRow:
				temp=(new PostgresReader(data)).popDataRow();
				if (rowCallback) {
					rowCallback((latestRowInfo && !rowinfoCallback)?parseDataRow(temp,latestRowInfo):parseDataRow(temp,undefined));
				} else {
					(latestRows || (latestRows=[])).push(parseDataRow(temp,latestRowInfo));
				}
				return false;
			case Constants.MessageTypes.Backend.CommandComplete:
				temp=(new PostgresReader(data)).popCommandComplete();
				if (commandCallback) {
					commandCallback(latestRows,temp);
				} else {
					savedArgs.push(latestRows);
					savedArgs.push(temp);
				}
				latestRowInfo=latestRows=undefined;
				return false;
			case Constants.MessageTypes.Backend.PortalSuspended:
				//Same as above, but with false to indicate more is expected.
				temp=false;
				if (commandCallback) {
					commandCallback(latestRows,temp);
				} else {
					savedArgs.push(latestRows);
					savedArgs.push(temp);
				}
				latestRowInfo=latestRows=undefined;
				return false;
			case Constants.MessageTypes.Backend.EmptyQueryResponse:
				return false; //Ignore
			case Constants.MessageTypes.Backend.ReadyForQuery:
				if (!returned) {
					returned=true;
					finalCallback.apply(null,savedArgs);
				}
				return true;
			default:
				if (!returned) {
					returned=true;
					finalCallback(new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type))));
				}
				return false;
		}
	};
}
PostgresClient.prototype.simpleQuery=function(query,callback) {
	var b=PostgresEncoder.create().pushQuery(query).toBuffer();
	this.handlerQueue.push(createQueryHandler(callback));
	return this.write(b);
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
	var b=PostgresEncoder.create()
			.pushParse("",query,createParseTypes(query))
			.pushBind("","",[],params.map(String),[]) //Maybe: params.map(function() { return Constants.FormatCodes.text; })
			.pushDescribe(Constants.Describe.Portal,"")
			.pushExecute("",0)
			.pushSync()
			.toBuffer();
	this.handlerQueue.push(createQueryHandler(callback));
	this.write(b);
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
					finalCallback((new PostgresReader(data)).popErrorResponse());
				}
				return false;
			case Constants.MessageTypes.Backend.ParseComplete:
			case Constants.MessageTypes.Backend.BindComplete:
				return false;
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
	};
}

PostgresClient.prototype.parse=function(query,callback) {
	var name="generated_statement"+(this.lastPreparedStatement++),
		b=PostgresEncoder.create()
			.pushParse(name,query,createParseTypes(query))
			.pushSync()
			.toBuffer();
	this.handlerQueue.push(createParseHandler(callback,name));
	return this.write(b);
};

PostgresClient.prototype.bindExecute=function(statement,params,callback) {
	var b=PostgresEncoder.create()
			.pushBind("",statement,params.map(function() { return Constants.FormatCodes.text; }),params.map(String),[])
			.pushDescribe(Constants.Describe.Portal,"")
			.pushExecute("",0)
			.pushSync()
			.toBuffer();
	this.handlerQueue.push(createQueryHandler(callback));
	return this.write(b);
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
					finalCallback((new PostgresReader(data)).popErrorResponse());
				}
				return false;
			case Constants.MessageTypes.Backend.CloseComplete:
				return true; //Done
			default:
				if (!returned) {
					returned=true;
					finalCallback(new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type))));
				}
				return false;
		}
	};
}

PostgresClient.prototype.closeStatement=function(statementName,callback) {
	var name="generated_statement"+(this.lastPreparedStatement++),
		b=PostgresEncoder.create()
			.pushClose(Constants.Close.Statement,statementName)
			.toBuffer();
	this.handlerQueue.push(createCloseHandler(callback,name));
	return this.write(b);
};

PostgresClient.prototype.prepare=function(query) {
	var preparedName,
		preparedErr,
		preparedCallbacks=[],
		closed,
		self=this;
	
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
	return {execute:execute,close:close};
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
		this.socket.write(PostgresEncoder.create().pushTerminate().toBuffer());
		this.socket.end();
	} else {
		this.socket.end();
	}
};