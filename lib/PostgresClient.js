/*global require, exports */
/*jslint onevar: true, undef: true, nomen: true, eqeqeq: true, regexp: true, newcap: true, immed: true */

var util = require('util'),
	net=require("net"),
	crypto=require("crypto"),
	PostgresEncoder=require("./PostgresEncoder").PostgresEncoder,
	PostgresReader=require("./PostgresReader").PostgresReader,
	strtok=require("strtok"),
	EventEmitter=require("events").EventEmitter,
	Constants=require("./PostgresConstants"),
	Buffer=require("buffer").Buffer;

/*
TODO:
- Cancel query requests (?), watch out for requests that are blocking on the Node.js side.
*/

function createPostgresConnectHandler(self,connectBlock) {	
	return function(err,type,data) {
		if (err) {
			throw err;
		}
		var reader,authType;
		if (type===Constants.MessageTypes.Backend.ErrorResponse) {
			throw (new PostgresReader(data)).popErrorResponse();
		} else if (type===Constants.MessageTypes.Backend.Authentication) {
			reader=new PostgresReader(data);
			authType=reader.popIntBE(4);
			switch (authType) {
				case Constants.AuthenticationTypes.Ok: break; //Just ignore that, a ReadyForQueue should be right up
				case Constants.AuthenticationTypes.MD5Password:
					self.writeBlock(
						connectBlock,
						PostgresEncoder.create().pushPasswordMessage(
							"md5"+crypto.createHash('md5').update(
								crypto.createHash('md5').update(this.password,'binary').update(this.username,'binary').digest('hex'),
								'binary'
							).update(reader.popBuffer(4).toString('binary'),'binary').digest('hex')
						).toBuffer()
					);
					break;
				default:
					throw new Error("Unexpected authentication type "+(Constants.AuthenticationTypesLookup[authType] || authType));
			}
			return false;
		} else if (type===Constants.MessageTypes.Backend.ReadyForQuery) {
			if (!self.connected) {
				self.connected=true;
				self.endBlock(connectBlock);
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

function createSocketHandlers(self,unbindSocket,connectBlock) {
	function onSocketConnect() {
		var params;
		if (self.socket && !self.closed) {
				params={
					user: self.username
				};
				if (self.database) {
					params.database=self.database;
				}
				self.writeBlock(connectBlock,PostgresEncoder.create().pushStartupMessage(params).toBuffer());
		}
	}
	function onError(err) {
		err=err || new Error(String(err));
		if (self.socket) {
			unbindSocket(self.socket);
			self.socket.destroy();
			self.socket=undefined;
		}
		if (!self.closed) {
			self.closed=true;
			self.emit("error",err);
		}
		if (self.currentHandler) {
			self.currentHandler.call(self,err);
			self.currentHandler=undefined;
		}
		function triggerHandlerQueue(current) {
			var q=current.outputQueue,
				l=q.length,
				i,
				c;
			for (i=0; i<l; i++) {
				try {
					c=q[i];
					if (typeof(c)==="function") {
						c(err);
					} else if (typeof(c)==="object") {
						triggerHandlerQueue(c);
					}
				}
				catch(e) {
					//Do nothing
				}
			}
		}
		triggerHandlerQueue(self);
		self.handlerQueue=[];
		self.outputQueue=[];
	}
	function shiftHandlerQueue() {
		/**
		 * Returns either:
		 * - true: you can skip over this,
		 * - undefined: you can not skip over this
		 * - handler: take this handler
		 */
		function shiftHandler(current) {
			var q=current.handlerQueue,f,fq;
			while (true) {
				if (q.length===0) {
					return (current.done)?true:undefined;
				}
				f=q[0];
				if (typeof(f)==="function") {
					q.shift();
					return f;
				}
				fq=shiftHandler(f);
				if (fq===true) {
					q.shift();
				} else {
					return fq;
				}
			}
		}
		var ret=shiftHandler(self);
		return (ret===true)?undefined:ret;
	}
	
	var STATE_MAX=0,
		STATE_INIT=STATE_MAX++,
		STATE_TYPE=STATE_MAX++,
		STATE_LEN=STATE_MAX++,
		STATE_DATA=STATE_MAX++,
		state=STATE_INIT,
		type;
	
	function onToken(token) {
		var handler;
		if (self.closed) {
			return strtok.DONE;
		}
		switch (state++) {
			case STATE_INIT:
				return strtok.UINT8;
			case STATE_TYPE:
				type=token;
				return strtok.UINT32_BE;
			case STATE_LEN:
				return new strtok.BufferType(token-strtok.UINT32_BE.len);
			case STATE_DATA:
				try {
					if (!self.mainHandler(type,token)) {
						handler=(self.currentHandler || (self.currentHandler=shiftHandlerQueue()));
						if (handler) {
							if (handler.call(self,undefined,type,token)) {
								self.currentHandler=undefined;
							}
						} else {
							self.unhandledHandler(type,token);
						}
					}
				}
				catch(e) {
					onError(e);
				}
				state=STATE_TYPE;
				return strtok.UINT8;
			default:
				onError(new Error("Invalid state reached"));
				return strtok.DONE;
		}
	}
	
	function onSocketEnd() {
		unbindSocket(self.socket);
		self.socket=undefined;
		if (!self.closed) {
			self.closed=true;
			self.emit("end");
		}
		onError(new Error("Connection closed"));
	}
	function onSocketError(err) {
		onError(err);
	}
	
	return {
		connect: onSocketConnect,
		token: onToken,
		error: onSocketError,
		end: onSocketEnd
	};
}

function PostgresClient(config) {
	var socketHandlers,
		connectBlock;
	
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
	//this.buffer=new BufferQueueReader();
	this.outputQueue=[];
	this.handlerQueue=[];
	this.currentHandler=undefined;
	this.lastPreparedStatement=0;
	this.lastPortal=0;
	
	connectBlock=this.startBlock();
	connectBlock.handlerQueue.push(createPostgresConnectHandler(this,connectBlock));
	
	function bindSocket(s) {
		s.addListener("connect",socketHandlers.connect);
		//s.addListener("data",socketHandlers.data);
		strtok.parse(s,socketHandlers.token);
		s.addListener("end",socketHandlers.end);
		s.addListener("error",socketHandlers.error);
	}
	function unbindSocket(s) {
		if (!s) {
			return;
		}
		s.removeListener("connect",socketHandlers.connect);
		s.removeAllListeners("data");
		s.removeListener("end",socketHandlers.end);
		s.removeListener("error",socketHandlers.error);
	}
	
	socketHandlers=createSocketHandlers(this,unbindSocket,connectBlock);
	bindSocket(this.socket);
}
util.inherits(PostgresClient,EventEmitter);
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
	throw new Error("Unhandled message "+(Constants.MessageTypesLookup.Backend[type] || type));
};
function PostgresBlock(owner) {
	this.outputQueue=[];
	this.handlerQueue=[];
	this.owner=owner;
}
PostgresBlock.prototype.done=false;
PostgresBlock.prototype.toString=function() {
	return "[PostgreSQL Block]";
};
PostgresClient.prototype.startBlock=function(owner) {
	if (!owner) {
		owner=this;
	} else if (owner.done) {
		throw new Error("Block already ended");
	}
	var t=new PostgresBlock(owner);
	owner.handlerQueue.push(t);
	owner.outputQueue.push(t);
	return t;
};
PostgresClient.prototype.isBlockFirstOutput=function(tr) {
	var up;
	while ((up = tr.owner)) {
		if (up.outputQueue[0] !== tr) {
			return false;
		}
		if (up === this) {
			return true;
		}
		tr=up;
	}
	return false;
};
PostgresClient.prototype.endBlock=function(tr) {
	if (tr.done) {
		throw new Error("Block already ended");
	}
	tr.done=true;
	if (this.isBlockFirstOutput(tr)) {
		this.flush();
	}
};
PostgresClient.prototype.writeBlock=function(tr,buffer,handler) {
	if (tr.done) {
		throw new Error("Block already ended");
	}
	if (handler) {
		tr.handlerQueue.push(handler);
	}
	if (this.isBlockFirstOutput(tr) && tr.outputQueue.length===0) {
		this.socket.write(buffer);
	} else {
		tr.outputQueue.push(buffer);
	}
};
PostgresClient.prototype.write=function(buffer,handler) {
	this.handlerQueue.push(handler);
	if (this.outputQueue.length===0) {
		this.socket.write(buffer);
	} else {
		this.outputQueue.push(buffer);
	}
};
PostgresClient.prototype.flush=function() {
	var self=this;
	function flush(current) {
		if (Buffer.isBuffer(current)) {
			self.socket.write(current);
			return true;
		}
		var q,l,i;
		if ((l=(q=current.outputQueue).length)>0) {
			for (i=0; i<l; i++) {
				if (!flush(q[i])) {
					break;
				}
			}
			if (i===l) {
				current.outputQueue=[];
			} else {
				current.outputQueue=q.slice(i);
			}
		}
		return current.done;
	}
	flush(this);
};

function is_int(x) {
	return (/^[0-9]+$/).test(x);
}

function parseDataRow(datarow,rowinfo) {
	var ret,
		named=rowinfo.some(function(ri) { return (ri.fieldName!=="?column?" && !is_int(ri.fieldName)); });
	if (rowinfo===undefined) {
		return datarow.map(function(x) {
			if (x===null) {
				return null;
			}
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

function PostgresCopyInOperation(owner,block) {
	EventEmitter.call(this);
	this.block=block;
	this.owner=owner;
}
util.inherits(PostgresCopyInOperation,EventEmitter);
PostgresCopyInOperation.prototype.writable=true;
PostgresCopyInOperation.prototype.toString=function() {
	return "[PostgresCopyInOperation]";
};
PostgresCopyInOperation.prototype.write=function(buffer,encoding) {
	if (!this.writable) {
		throw new Error('Stream is not writable');
	}
	this.owner.writeBlock(this.block,PostgresEncoder.create().pushCopyData(Buffer.isBuffer(buffer)?buffer:new Buffer(buffer,encoding)).toBuffer());
	return true;
};
PostgresCopyInOperation.prototype.end=function(buffer,encoding) {
	if (!this.writable) {
		//Already closed
		return;
	}
	if (buffer) {
		this.write(buffer,encoding);
	}
	this.writable=false;
	this.owner.writeBlock(this.block,PostgresEncoder.create().pushCopyDone().toBuffer());
};
PostgresCopyInOperation.prototype.fail=function(msg) {
	if (!this.writable) {
		//Already closed
		return;
	}
	this.writable=false;
	this.owner.writeBlock(this.block,PostgresEncoder.create().pushCopyFail(msg,"utf8").toBuffer());
};
PostgresCopyInOperation.prototype.destroy=function() {
	return this.fail("op.destroy() called instead of op.end()");
};

function createQueryHandler(callback,blockToEnd) {
	var savedArgs=[undefined],
		returned=false,
		latestRows,
		latestRowInfo,
		temp,
		rowDescriptionCallback=callback.rowDescription,
		rowCallback=callback.dataRow,
		copyOutCallback=callback.copyOut,
		copyDataCallback=callback.copyData,
		copyDoneCallback=callback.copyDone,
		copyInCallback=callback.copyIn,
		copyInOperation,
		commandCallback=callback.commandComplete,
		finalCallback=(callback.complete || callback);
	//rowinfoCallback,rowCallback,commandCallback,finalCallback
	return function(err,type,data) {
		if (err) {
			if (!returned) {
				returned=true;
				finalCallback.call(callback,err);
			}
			return true;
		}
		switch (type) {
			case Constants.MessageTypes.Backend.ErrorResponse:
				if (!returned) {
					returned=true;
					finalCallback.call(callback,(new PostgresReader(data)).popErrorResponse());
				}
				return false;
			case Constants.MessageTypes.Backend.ParseComplete:
			case Constants.MessageTypes.Backend.BindComplete:
				return false; //Both part of the extended query responses, safe to ignore
			case Constants.MessageTypes.Backend.RowDescription:
				if (returned) {
					return false;
				}
				latestRowInfo=(new PostgresReader(data)).popRowDescription();
				if (rowDescriptionCallback) {
					rowDescriptionCallback.call(callback,latestRowInfo);
				}
				if (!rowCallback) {
					latestRows=[];
				}
				return false;
			case Constants.MessageTypes.Backend.DataRow:
				if (returned) {
					return false;
				}
				temp=(new PostgresReader(data)).popDataRow();
				if (rowCallback) {
					rowCallback.call(callback,(latestRowInfo && !rowDescriptionCallback)?parseDataRow(temp,latestRowInfo):parseDataRow(temp,undefined));
				} else {
					(latestRows || (latestRows=[])).push(parseDataRow(temp,latestRowInfo));
				}
				return false;
			case Constants.MessageTypes.Backend.CopyInResponse:
				if (copyInOperation && copyInOperation.writeable) {
					copyInOperation.writeable=false;
					copyInOperation.emit("error",new Error("Multiple CopyInResponses received"));
				}
				if (!blockToEnd || !copyInCallback) {
					//Just throw an error and let the entire thing fail, as doing this without a seperate block will mess up the message flow.
					throw new Error("FATAL ERROR: COPY IN operation executed without copyIn callback, message flow is now de-synchronized.");
				}
				if (returned) {
					this.writeBlock(blockToEnd,PostgresEncoder.create().pushCopyFail("Command already failed",'utf8').toBuffer());
					return false;
				}
				copyInCallback.call(callback,copyInOperation = new PostgresCopyInOperation(this,blockToEnd));
				return false;
			case Constants.MessageTypes.Backend.CopyOutResponse:
				if (returned) {
					return false;
				}
				temp=(new PostgresReader(data)).popCopyOutResponse();
				if (copyOutCallback) {
					copyOutCallback.call(callback,temp.format,temp.columns);
				}
				return false;
			case Constants.MessageTypes.Backend.CopyData:
				if (returned) {
					return false;
				}
				if (copyDataCallback) {
					copyDataCallback.call(callback,data);
				}
				return false;
			case Constants.MessageTypes.Backend.CopyDone:
				if (returned) {
					return false;
				}
				if (copyDoneCallback) {
					copyDoneCallback.call(callback);
				}
				return false;
			case Constants.MessageTypes.Backend.CommandComplete:
				if (returned) {
					return false;
				}
				if (copyInOperation) {
					if (copyInOperation.writable) {
						copyInOperation.writable=false;
						copyInOperation.emit("close");
					}
					copyInOperation=undefined;
				}
				temp=(new PostgresReader(data)).popCommandComplete();
				if (commandCallback) {
					commandCallback.call(callback,latestRows,temp);
				} else {
					savedArgs.push(latestRows);
					savedArgs.push(temp);
				}
				latestRowInfo=latestRows=undefined;
				return false;
			case Constants.MessageTypes.Backend.PortalSuspended:
				if (returned) {
					return false;
				}
				//Same as above, but with false to indicate more is expected.
				temp=false;
				if (commandCallback) {
					commandCallback.call(callback,latestRows,temp);
				} else {
					savedArgs.push(latestRows);
					savedArgs.push(temp);
				}
				latestRowInfo=latestRows=undefined;
				return false;
			case Constants.MessageTypes.Backend.EmptyQueryResponse:
				return false; //Ignore
			case Constants.MessageTypes.Backend.ReadyForQuery:
				if (blockToEnd) {
					this.endBlock(blockToEnd);
				}
				if (!returned) {
					returned=true;
					finalCallback.apply(callback,savedArgs);
				}
				return true;
			default:
				if (!returned) {
					returned=true;
					finalCallback.call(callback,new Error("Unexpected message type "+(Constants.MessageTypesLookup.Backend[type] || String.fromCharCode(type))));
				}
				return false;
		}
	};
}
PostgresClient.prototype.simpleQuery=function(query,block,callback) {
	var b=PostgresEncoder.create().pushQuery(query).toBuffer(),h;
	if (!callback) {
		callback=block;
		block=undefined;
	}
	if (callback.copyIn) {
		//Create a new (sub-)block that will automatically be ended by the query handler once the copy query is done.
		h=createQueryHandler(callback,(block=this.startBlock(block)));
	} else {
		//Normal handler
		h=createQueryHandler(callback,undefined);
	}
	if (!block) {
		return this.write(b,h);
	} else {
		return this.writeBlock(block,b,h);
	}
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

PostgresClient.prototype.extendedQuery=function(query,params,block,callback) {
	var b=PostgresEncoder.create()
			.pushParse("",query,createParseTypes(query))
			.pushBind("","",[],params.map(String),[]) //Maybe: params.map(function() { return Constants.FormatCodes.text; })
			.pushDescribe(Constants.Describe.Portal,"")
			.pushExecute("",0)
			.pushSync()
			.toBuffer(),
		h;
	if (!callback) {
		callback=block;
		block=undefined;
	}
	if (callback.copyIn) {
		//Create a new (sub-)block that will automatically be ended by the query handler once the copy query is done.
		h=createQueryHandler(callback,(block=this.startBlock(block)));
	} else {
		//Normal handler
		h=createQueryHandler(callback,undefined);
	}
	if (!block) {
		return this.write(b,h);
	} else {
		return this.writeBlock(block,b,h);
	}
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
	return this.write(b,createParseHandler(callback,name));
};
//TODO: Allow blockal
PostgresClient.prototype.bindExecute=function(statement,params,block,callback) {

	var b=PostgresEncoder.create()
			.pushBind("",statement,params.map(function() { return Constants.FormatCodes.text; }),params.map(String),[])
			.pushDescribe(Constants.Describe.Portal,"")
			.pushExecute("",0)
			.pushSync()
			.toBuffer(),
		h;
	if (!callback) {
		callback=block;
		block=undefined;
	}
	if (callback.copyIn) {
		//Create a new (sub-)block that will automatically be ended by the query handler once the copy query is done.
		h=createQueryHandler(callback,(block=this.startBlock(block)));
	} else {
		//Normal handler
		h=createQueryHandler(callback,undefined);
	}
	if (!block) {
		return this.write(b,h);
	} else {
		return this.writeBlock(block,b,h);
	}
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
	return this.write(b,createCloseHandler(callback,name));
};

PostgresClient.prototype.prepare=function(query) {
	var preparedName,
		preparedErr,
		preparedCallbacks=[],
		closed,
		self=this;
	
	function execute(params,block,callback) {
		if (closed) {
			callback(new Error("Already closed"));
			return;
		}
		if (preparedErr) {
			callback(preparedErr);
			return;
		}
		if (preparedName) {
			self.bindExecute(preparedName,params,block,callback);
			return;
		}
		preparedCallbacks.push(function(err,name) {
			if (err) {
				callback(err);
			} else {
				self.bindExecute(name,params,block,callback);
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