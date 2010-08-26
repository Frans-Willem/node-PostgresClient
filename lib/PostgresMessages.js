var PostgresReader=require("./PostgresReader").PostgresReader,
	PostgresEncoder=require("./PostgresEncoder").PostgresEncoder;

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