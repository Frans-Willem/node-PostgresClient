/*global require, exports */
/*jslint onevar: true, undef: true, nomen: true, eqeqeq: true, regexp: true, newcap: true, immed: true */

var Buffer=require("buffer").Buffer, //Technically Node 0.2.0 should make this global, but lets make this work on Node 1.9 too :)
	BufferBuilder=require("bufferlib/BufferBuilder").BufferBuilder,
	Constants=require("./PostgresConstants"),
	sys=require("sys");

function PostgresEncoder() {
	BufferBuilder.call(this);
	this.frames=[];
}
sys.inherits(PostgresEncoder,BufferBuilder);
exports.PostgresEncoder=PostgresEncoder;
PostgresEncoder.create=function() {
	return new PostgresEncoder();
};
PostgresEncoder.prototype.pushPostgresEncoder=PostgresEncoder;
//key=value hashed object
PostgresEncoder.prototype.pushHash=function(obj,encoding) {
	var self=this;
	Object.keys(obj).forEach(function(k) {
		self.pushStringZero(k,encoding);
		self.pushStringZero(String(obj[k]),encoding);
	});
	self.pushStringZero("",encoding);
	return this;
};
PostgresEncoder.prototype.pushFrameStart=function(header) {
	if (header !== undefined) {
		this.pushByte(header);
	}
	var start=this.length,
		value,
		size=4;
	this.length+=size;
	this.data.push(function(out,offset) {
		if (typeof(value)!=="number") {
			throw new Error("Not all frames have been properly closed");
		}
		var i,shift=size*8;
		for (i=0; i<size; i++) {
			out[offset+i]=(value >>> (shift-=8)) & 0xFF;
		}
		return size;
	});
	this.frames.push(function(end) {
		value=end-start;
	});
	return this;
};
PostgresEncoder.prototype.pushFrameEnd=function() {
	if (this.frames.length<1) {
		throw new Error("Call pushFrameStart first!");
	}
	this.frames.pop()(this.length);
	return this;
};
PostgresEncoder.prototype.pushBind=function(theNameOfTheDestinationPortal,theNameOfTheSourcePreparedStatement,theParameterFormatCodes,theParameters,theResultColumnFormatCodes) {
	var self=this;
	this.pushFrameStart(Constants.MessageTypes.Frontend.Bind);
	this.pushStringZero(String(theNameOfTheDestinationPortal));
	this.pushStringZero(String(theNameOfTheSourcePreparedStatement));
	this.pushIntBE(theParameterFormatCodes.length,2);
	theParameterFormatCodes.forEach(function(fc) {
		self.pushIntBE(Math.floor(fc),2);
	});
	this.pushIntBE(theParameters.length,2);
	theParameters.forEach(function(par,index) {
		if (par===null) {
			self.pushIntBE(-1>>>0,4);
		} else {
			par=String(par);
			self.pushIntBE(Buffer.byteLength(par,'utf8'),4);
			self.pushString(par,'utf8');
		}
	});
	this.pushIntBE(theResultColumnFormatCodes.length,2);
	theResultColumnFormatCodes.forEach(function(x) {
		self.pushIntBE(Math.floor(x),2);
	});
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushCancelRequest=function(theCancelRequestCode,theProcessID,theSecretKey) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.CancelRequest);
	this.pushIntBE(theCancelRequestCode,4);
	this.pushIntBE(theProcessID,4);
	this.pushIntBE(theSecretKey,4);
	return this.pushFrameEnd();
};

PostgresEncoder.prototype.pushClose=function(type,name) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.Close);
	this.pushByte(type);
	this.pushStringZero(name,'utf8');
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushCopyData=function(buffer) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.CopyData);
	this.pushBuffer(buffer);
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushCopyDone=function() {
	return this.pushFrameStart(Constants.MessageTypes.Frontend.CopyDone).pushFrameEnd();
};
PostgresEncoder.prototype.pushCopyFail=function(msg,encoding) {
	return this.pushFrameStart(Constants.MessageTypes.Frontend.CopyFail).pushStringZero(msg,encoding).pushFrameEnd();
};
PostgresEncoder.prototype.pushDescribe=function(type,name) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.Describe);
	this.pushByte(type);
	this.pushStringZero(name,'utf8');
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushExecute=function(nameOfThePortal,maximumNumberOfRowsToReturn) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.Execute);
	this.pushStringZero(nameOfThePortal,'utf8');
	this.pushIntBE(maximumNumberOfRowsToReturn,4);
	return this.pushFrameEnd();
};
//		Flush: Byte1('H'),
//		FunctionCall: Byte1('F'),
PostgresEncoder.prototype.pushParse=function(theNameOfTheDestinationPreparedStatement,theQueryStringToBeParsed,theParameterDataTypes) {
	var self=this;
	this.pushFrameStart(Constants.MessageTypes.Frontend.Parse);
	this.pushStringZero(theNameOfTheDestinationPreparedStatement,'utf8');
	this.pushStringZero(theQueryStringToBeParsed,'utf8');
	this.pushIntBE(theParameterDataTypes.length,2);
	theParameterDataTypes.forEach(function(t) {
		self.pushIntBE(Math.floor(t),4);
	});
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushPasswordMessage=function(password) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.PasswordMessage);
	this.pushStringZero(password,'utf8');
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushQuery=function(query) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.Query);
	this.pushStringZero(query,'utf8');
	return this.pushFrameEnd();
};
//		SSLRequest: undefined,
PostgresEncoder.prototype.pushStartupMessage=function(params) {
	this.pushFrameStart(Constants.MessageTypes.Frontend.StartupMessage);
	this.pushIntBE((3<<16)+0,4);
	this.pushHash(params,'utf8');
	return this.pushFrameEnd();
};
PostgresEncoder.prototype.pushSync=function() {
	return this.pushFrameStart(Constants.MessageTypes.Frontend.Sync).pushFrameEnd();
};
PostgresEncoder.prototype.pushTerminate=function() {
	return this.pushFrameStart(Constants.MessageTypes.Frontend.Terminate).pushFrameEnd();
};