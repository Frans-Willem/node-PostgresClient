var Constants=require("./PostgresConstants"),
	PostgresReader=require("./PostgresReader").PostgresReader,
	PostgresEncoder=require("./PostgresEncoder").PostgresEncoder;

exports.Bind=function(theNameOfTheDestinationPortal,theNameOfTheSourcePreparedStatement,theParameterFormatCodes,theParameters,theResultColumnFormatCodes) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.Bind);
	encoder.pushStringZero(String(theNameOfTheDestinationPortal));
	encoder.pushStringZero(String(theNameOfTheSourcePreparedStatement));
	encoder.pushIntBE(theParameterFormatCodes.length,2);
	theParameterFormatCodes.forEach(function(fc) {
		encoder.pushIntBE(Math.floor(fc),2);
	});
	encoder.pushIntBE(theParameters.length,2);
	theParameters.forEach(function(par,index) {
		if (par===null) {
			encoder.pushIntBE(-1>>>0,4);
		} else {
			par=String(par);
			encoder.pushIntBE(Buffer.byteLength(par,'utf8'),4);
			encoder.pushString(par,'utf8');
		}
	});
	encoder.pushIntBE(theResultColumnFormatCodes.length,2);
	theResultColumnFormatCodes.forEach(function(x) {
		encoder.pushIntBE(Math.floor(x),2);
	});
	return encoder.frame();
};
exports.CancelRequest=function(theCancelRequestCode,theProcessID,theSecretKey) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.CancelRequest);
	encoder.pushIntBE(theCancelRequestCode,4);
	encoder.pushIntBE(theProcessID,4);
	encoder.pushIntBE(theSecretKey,4);
	return encoder.frame();
};
exports.Close=function(type,name) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.Close);
	encoder.pushByte(type);
	encoder.pushStringZero(name,'utf8');
	return encoder.frame();
}
exports.Close.Portal="P".charCodeAt(0);
exports.Close.Statement="S".charCodeAt(0);
//		CopyData: Byte1('d'),
//		CopyDone: Byte1('c'),
//		CopyFail: Byte1('f'),
exports.Describe=function(type,name) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.Describe);
	encoder.pushByte(type);
	encoder.pushStringZero(name,'utf8');
	return encoder.frame();
}
exports.Describe.Portal="P".charCodeAt(0);
exports.Describe.Statement="S".charCodeAt(0);
exports.Execute=function(nameOfThePortal,maximumNumberOfRowsToReturn) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.Execute);
	encoder.pushStringZero(nameOfThePortal,'utf8');
	encoder.pushIntBE(maximumNumberOfRowsToReturn,4);
	return encoder.frame();
}
//		Flush: Byte1('H'),
//		FunctionCall: Byte1('F'),
exports.Parse=function(theNameOfTheDestinationPreparedStatement,theQueryStringToBeParsed,theParameterDataTypes) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.Parse);
	encoder.pushStringZero(theNameOfTheDestinationPreparedStatement,'utf8');
	encoder.pushStringZero(theQueryStringToBeParsed,'utf8');
	encoder.pushIntBE(theParameterDataTypes.length,2);
	theParameterDataTypes.forEach(function(t) {
		encoder.pushIntBE(Math.floor(t),4);
	});
	return encoder.frame();
}
exports.PasswordMessage=function(password) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.PasswordMessage);
	encoder.pushStringZero(password,'utf8');
	return encoder.frame();
};
exports.Query=function(query) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.Query);
	encoder.pushStringZero(query,'utf8');
	return encoder.frame();
};
//		SSLRequest: undefined,
exports.StartupMessage=function(params) {
	var encoder=new PostgresEncoder(Constants.MessageTypes.Frontend.StartupMessage);
	encoder.pushIntBE((3<<16)+0,4);
	encoder.pushHash(params,'utf8');
	return encoder.frame();
};
exports.Sync=function() {
	return (new PostgresEncoder(Constants.MessageTypes.Frontend.Sync)).frame();
}
exports.Terminate=function() {
	return (new PostgresEncoder(Constants.MessageTypes.Frontend.Terminate)).frame();
};

exports.DecodeRowDescription=function(data) {
	var reader=new PostgresReader(data);
	var nFields=reader.popIntBE(2);
	var fields=[];
	while (nFields--) {
		fields.push({
			fieldName: reader.popStringZero('utf8'),
			tableOID: reader.popIntBE(4),
			columnAttribute: reader.popIntBE(2),
			datatypeOID: reader.popIntBE(4),
			datatypesize: reader.popIntBE(2),
			typemodifier: reader.popIntBE(4),
			formatcode: reader.popIntBE(2)
		});
	}
	return fields;
};
exports.DecodeErrorResponse=function(data) {
	var reader=new PostgresReader(data);
	var fields={},type,value;
	while ((type = reader.popByte())!==0) {
		type=Constants.MessageFieldsLookup[type];
		value=reader.popStringZero('utf8');
		if (type) {
			fields[type]=value;
		}
	}
	var e=new Error(fields.Message);
	Object.keys(fields).forEach(function(k) {
		e[k]=fields[k];
	});
	return e;	
};
exports.DecodeDataRow=function(data) {
	var reader=new PostgresReader(data),
		numColumns=reader.popIntBE(2),
		columns=[],
		colLength;
	while (numColumns--) {
		colLength=reader.popIntBE(4);
		if (colLength>>0 === -1) {
			columns.push(null);
		} else {
			columns.push(reader.popBuffer(colLength));
		}
	}
	return columns;
};
exports.DecodeCommandComplete=function(data) {
	var reader=new PostgresReader(data);
	return reader.popStringZero('utf8');
};