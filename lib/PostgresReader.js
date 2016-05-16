/*global require, exports */
/*jslint onevar: true, undef: true, nomen: true, eqeqeq: true, regexp: true, newcap: true, immed: true */

var BufferReader=require("bufferlib/BufferReader").BufferReader,
	Constants=require("./PostgresConstants"),
	util = require('util');

function PostgresReader(buffer) {
	BufferReader.call(this,buffer);
}
util.inherits(PostgresReader,BufferReader);
exports.PostgresReader=PostgresReader;

PostgresReader.prototype.popMultiStringZero=function(encoding) {
	var ret=[],cur;
	while ((cur=this.popStringZero(encoding)).length>0) {
		ret.push(cur);
	}
	return ret;
};
PostgresReader.prototype.popAuthentication=function() {
	var ret={},
		type=ret.type=this.popIntBE(4);
	ret.typeName=Constants.AuthenticationTypesLookup[type];
	switch (type) {
		case Constants.AuthenticationTypes.Ok:
		case Constants.AuthenticationTypes.KerberosV5:
		case Constants.AuthenticationTypes.CleartextPassword:
		case Constants.AuthenticationTypes.SCMCredential:
		case Constants.AuthenticationTypes.GSS:
		case Constants.AuthenticationTypes.SSPI:
			break;
		case Constants.AuthenticationTypes.MD5Password:
			ret.salt=this.popBuffer(4);
			break;
		case Constants.AuthenticationTypes.GSSContinue:
			ret.data=this.popBuffer(this.length);
			break;
	}
	return ret;
};
PostgresReader.prototype.popBackendKeyData=function() {
	return {
		pid: this.popIntBE(4),
		key: this.popIntBE(4)
	};
};
//BindComplete
//CloseComplete
PostgresReader.prototype.popCommandComplete=function(encoding) {
	return this.popStringZero(encoding);
};
//CopyData
//CopyDone
PostgresReader.prototype.popCopyInResponse=function() {
	var numcolumns,ret={
		format: this.popByte(),
		columns: []
	};
	numcolumns=this.popIntBE(2);
	while (numcolumns--) {
		ret.columns.push(this.popIntBE(2));
	}
	return ret;
};
PostgresReader.prototype.popCopyOutResponse=PostgresReader.prototype.popCopyInResponse;
PostgresReader.prototype.popDataRow=function() {
	var ret=[],cols=this.popIntBE(2),len;
	while (cols--) {
		len=this.popIntBE(4);
		if (len>>0 === -1) {
			ret.push(null);
		} else {
			ret.push(this.popBuffer(len));
		}
	}
	return ret;
};
PostgresReader.prototype.popErrorResponse=function() {
	var fields={},type,value,e;
	while ((type = this.popByte())!==0) {
		type=Constants.MessageFieldsLookup[type];
		value=this.popStringZero('utf8');
		if (type) {
			fields[type]=value;
		}
	}
	e=new Error(fields.Message);
	Object.keys(fields).forEach(function(k) {
		e[k]=fields[k];
	});
	return e;
};
//EmptyQueryResponse
PostgresReader.prototype.popFunctionCallResponse=function() {
	var len=this.popIntBE(4);
	if (len>>0 === -1) {
		return null;
	} else {
		return this.popBuffer(len);
	}
};
//NoData: Byte1('B'),
PostgresReader.prototype.popNoticeResponse=function() {
	var fields={},type,value;
	while ((type = this.popByte())!==0) {
		type=Constants.MessageFieldsLookup[type];
		value=this.popStringZero('utf8');
		if (type) {
			fields[type]=value;
		}
	}
	return fields;
};
PostgresReader.prototype.popNotificationResponse=function(encoding) {
	return {
		pid: this.popIntBE(4),
		name: this.popStringZero(encoding),
		payload: this.popStringZero(encoding)
	};
};
PostgresReader.prototype.popParameterDescription=function() {
	var ret=[],cols=this.popIntBE(2);
	while (cols--) {
		ret.push(this.popIntBE(4));
	}
	return ret;
};
PostgresReader.prototype.popParameterStatus=function(encoding) {
	return {
		name: this.popStringZero(encoding),
		value: this.popStringZero(encoding)
	};
};
//ParseComplete
//PortalSuspend
//ReadyForQuery
PostgresReader.prototype.popRowDescription=function() {
	var nFields=this.popIntBE(2),
		fields=[];
	while (nFields--) {
		fields.push({
			fieldName: this.popStringZero('utf8'),
			tableOID: this.popIntBE(4),
			columnAttribute: this.popIntBE(2),
			datatypeOID: this.popIntBE(4),
			datatypesize: this.popIntBE(2),
			typemodifier: this.popIntBE(4),
			formatcode: this.popIntBE(2)
		});
	}
	return fields;
};
