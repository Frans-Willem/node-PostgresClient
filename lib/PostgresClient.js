/*global require, exports */
/*jslint onevar: true, undef: true, nomen: true, eqeqeq: true, regexp: true, newcap: true, immed: true */

var sys=require("sys"),
	net=require("net"),
	crypto=require("crypto"),
	PostgresEncoder=require("./PostgresEncoder").PostgresEncoder,
	PostgresReader=require("./PostgresReader").PostgresReader,
	strtok=require("strtok"),
	EventEmitter=require("events").EventEmitter,
	Constants=require("./PostgresConstants")

/*
TODO:
- COPY ... FROM STDIN/TO STDOUT support
- Check to see transactions are supported over all vommands
*/

function createPostgresConnectHandler(self,connectTransaction) {	
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
					self.writeTransaction(
						connectTransaction,
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
				self.endTransaction(connectTransaction);
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

function createSocketHandlers(self,unbindSocket,connectTransaction) {
	function onSocketConnect() {
		var params;
		if (self.socket && !self.closed) {
				params={
					user: self.username
				};
				if (self.database) {
					params.database=self.database;
				}
				self.writeTransaction(connectTransaction,PostgresEncoder.create().pushStartupMessage(params).toBuffer());
		}
	}
	function shiftHandlerQueue() {
		var q=self.handlerQueue,f,fq;
		while (true) {
			if (q.length===0) {
				return undefined;
			}
			f=q[0];
			if (typeof(f)==="function") {
				return q.shift();
			}
			//If it's not a function, then it'll be a transaction, see if the transaction has any handlers left
			if ((fq=f.handlerQueue).length>0) {
				return fq.shift();
			}
			//Can we skip over this transaction?
			if (f.done) {
				q.shift();
			} else {
				return undefined;
			}
		}
	}
	var STATE_MAX=0;
	var STATE_INIT=STATE_MAX++;
	var STATE_TYPE=STATE_MAX++;
	var STATE_LEN=STATE_MAX++;
	var STATE_DATA=STATE_MAX++;
	var state=STATE_INIT;
	var type;
	function onToken(token) {
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
					if (!self.closed) {
						self.closed=true;
						self.socket.destroy();
						self.emit("error",e);
					}
				}
				state=STATE_TYPE;
				return strtok.UINT8;
			default:
				if (!self.closed) {
					self.closed=true;
					self.socket.destroy();
					self.emit("error",new Error("Invalid state reached"));
				}
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
		token: onToken,
		error: onSocketError,
		end: onSocketEnd
	};
}

function PostgresClient(config) {
	var socketHandlers,
		connectTransaction;
	
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
	
	connectTransaction=this.startTransaction();
	connectTransaction.handlerQueue.push(createPostgresConnectHandler(this,connectTransaction));
	
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
	
	socketHandlers=createSocketHandlers(this,unbindSocket,connectTransaction);
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
function PostgresTransaction() {
	this.outputQueue=[];
	this.handlerQueue=[];
}
PostgresTransaction.prototype.done=false;
PostgresTransaction.prototype.toString=function() {
	return "[PostgreSQL Transaction]";
};
PostgresClient.prototype.startTransaction=function() {
	var t=new PostgresTransaction();
	this.handlerQueue.push(t);
	this.outputQueue.push(t);
	return t;
};
PostgresClient.prototype.endTransaction=function(tr) {
	if (tr.done) {
		throw new Error("Transaction already ended");
	}
	tr.done=true;
	if (this.outputQueue[0]===tr) {
		this.flush();
	}
};
PostgresClient.prototype.writeTransaction=function(tr,buffer,handler) {
	if (tr.done) {
		throw new Error("Transaction already ended");
	}
	if (handler) {
		tr.handlerQueue.push(handler);
	}
	if (this.outputQueue[0]===tr) {
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
	var i,l=this.outputQueue.length,c,self=this,q;
	for (i=0; i<l; i++) {
		c=this.outputQueue[i];
		if (Buffer.isBuffer(c)) {
			this.socket.write(c);
		} else {
			if (c.outputQueue.length) {
				c.outputQueue.forEach(function(c) {
					self.socket.write(c);
				});
				c.outputQueue=[];
			}
			if (!c.done)
				break;
		}
	}
	if (i===l) {
		this.outputQueue=[];
	} else {
		this.outputQueue=this.outputQueue.slice(i);
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

function createQueryHandler(callback,transactionToEnd) {
	var savedArgs=[undefined],
		returned=false,
		latestRows,
		latestRowInfo,
		temp,
		rowDescriptionCallback=callback.rowDescription,
		rowCallback=callback.dataRow,
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
				latestRowInfo=(new PostgresReader(data)).popRowDescription();
				if (rowDescriptionCallback) {
					rowDescriptionCallback.call(callback,latestRowInfo);
				}
				if (!rowCallback) {
					latestRows=[];
				}
				return false;
			case Constants.MessageTypes.Backend.DataRow:
				temp=(new PostgresReader(data)).popDataRow();
				if (rowCallback) {
					rowCallback.call(callback,(latestRowInfo && !rowDescriptionCallback)?parseDataRow(temp,latestRowInfo):parseDataRow(temp,undefined));
				} else {
					(latestRows || (latestRows=[])).push(parseDataRow(temp,latestRowInfo));
				}
				return false;
			case Constants.MessageTypes.Backend.CopyOutResponse:
				temp=(new PostgresReader(data)).popCopyOutResponse();
				if (copyoutCallback) {
					copyoutCallback.call(callback,temp.format,temp.columns);
				}
				return false;
			case Constants.MessageTypes.Backend.CopyData:
				if (copyoutCallback) {
					copyoutCallback.call(callback,data);
				}
				return false;
			case Constants.MessageTypes.Backend.CopyDone:
				if (copydoneCallback) {
					copydoneCallback.call(callback);
				}
				return false;
			case Constants.MessageTypes.Backend.CommandComplete:
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
				if (transactionToEnd) {
					this.endTransaction(transactionToEnd);
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
};
PostgresClient.prototype.simpleQuery=function(query,transaction,callback) {
	var b=PostgresEncoder.create().pushQuery(query).toBuffer(),h;
	if (!callback) {
		callback=transaction;
		transaction=undefined;
	}
	if (!transaction) {
		return this.write(b,createQueryHandler(callback,undefined));
	} else {
		//if transaction===true, allocate a new transaction for this statement, and let the query handler close it when ready for the next query.
		h=createQueryHandler(callback,(transaction===true)?(transaction=this.startTransaction()):transaction);
		return this.writeTransaction(transaction,b);
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
};

PostgresClient.prototype.extendedQuery=function(query,params,transaction,callback) {
	var b=PostgresEncoder.create()
			.pushParse("",query,createParseTypes(query))
			.pushBind("","",[],params.map(String),[]) //Maybe: params.map(function() { return Constants.FormatCodes.text; })
			.pushDescribe(Constants.Describe.Portal,"")
			.pushExecute("",0)
			.pushSync()
			.toBuffer();
	if (!callback) {
		callback=transaction;
		transaction=undefined;
	}
	if (!transaction) {
		return this.write(b,createQueryHandler(callback,undefined));
	} else {
		//if transaction===true, allocate a new transaction for this statement, and let the query handler close it when ready for the next query.
		h=createQueryHandler(callback,(transaction===true)?(transaction=this.startTransaction()):transaction);
		return this.writeTransaction(transaction,b,h);
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
//TODO: Allow transactional
PostgresClient.prototype.bindExecute=function(statement,params,transaction,callback) {

	var b=PostgresEncoder.create()
			.pushBind("",statement,params.map(function() { return Constants.FormatCodes.text; }),params.map(String),[])
			.pushDescribe(Constants.Describe.Portal,"")
			.pushExecute("",0)
			.pushSync()
			.toBuffer();
	if (!callback) {
		callback=transaction;
		transaction=undefined;
	}
	if (!transaction) {
		return this.write(b,createQueryHandler(callback,undefined));
	} else {
		//if transaction===true, allocate a new transaction for this statement, and let the query handler close it when ready for the next query.
		h=createQueryHandler(callback,(transaction===true)?(transaction=this.startTransaction()):transaction);
		return this.writeTransaction(transaction,b,h);
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