/*global require, exports */
/*jslint onevar: true, undef: true, nomen: true, eqeqeq: true, regexp: true, newcap: true, immed: true */

function byte1(b) {
	return b.charCodeAt(0);
}

function createLookup(obj) {
	var ret={};
	Object.keys(obj).forEach(function(key) {
		ret[obj[key]]=key;
	});
	return ret;
}
exports.MessageTypes={
	Backend:{
		Authentication: byte1('R'),
		BackendKeyData: byte1('K'),
		BindComplete: byte1('2'),
		CloseComplete: byte1('3'),
		CommandComplete: byte1('C'),
		CopyData: byte1('d'),
		CopyDone: byte1('c'),
		CopyInResponse: byte1('G'),
		CopyOutResponse: byte1('H'),
		DataRow: byte1('D'),
		EmptyQueryResponse: byte1('I'),
		ErrorResponse: byte1('E'),
		FunctionCallResponse: byte1('V'),
		NoData: byte1('B'),
		NoticeResponse: byte1('N'),
		NotificationResponse: byte1('A'),
		ParameterDescription: byte1('t'),
		ParameterStatus: byte1('S'),
		ParseComplete: byte1('1'),
		PortalSuspend: byte1('s'),
		ReadyForQuery: byte1('Z'),
		RowDescription: byte1('T')
	},
	Frontend:{
		Bind: byte1('B'),
		CancelRequest: undefined,
		Close: byte1('C'),
		CopyData: byte1('d'),
		CopyDone: byte1('c'),
		CopyFail: byte1('f'),
		Describe: byte1('D'),
		Execute: byte1('E'),
		Flush: byte1('H'),
		FunctionCall: byte1('F'),
		Parse: byte1('P'),
		PasswordMessage: byte1('p'),
		Query: byte1('Q'),
		SSLRequest: undefined,
		StartupMessage: undefined,
		Sync: byte1('S'),
		Terminate: byte1('X')
	}	
};
exports.MessageTypesLookup={};
Object.keys(exports.MessageTypes).forEach(function(key) {
	exports.MessageTypesLookup[key]=createLookup(exports.MessageTypes[key]);
});

exports.MessageFields={
	Severity: byte1('S'),
	Code: byte1('C'),
	Message: byte1('M'),
	Detail: byte1('D'),
	Hint: byte1('H'),
	Position: byte1('P'),
	"Internal position": byte1('p'),
	"Internal query": byte1('q'),
	Where: byte1('W'),
	File: byte1('F'),
	Line: byte1('L'),
	Routine: byte1('R')
};
exports.MessageFieldsLookup=createLookup(exports.MessageFields);

exports.AuthenticationTypes={
	Ok: 0,
	KerberosV5: 2,
	CleartextPassword: 3,
	MD5Password: 5,
	SCMCredential: 6,
	GSS: 7,
	GSSContinue: 8,
	SSPI: 9
};
exports.AuthenticationTypesLookup=createLookup(exports.AuthenticationTypes);

exports.FormatCodes={
	text: 0,
	binary: 1
};
exports.FormatCodesLookup=createLookup(exports.FormatCodes);

exports.TypeOIDs={
	"bool": 16,
	"bytea": 17,
	"char": 18,
	"name": 19,
	"int8": 20,
	"int2": 21,
	"int2vector": 22,
	"int4": 23,
	"regproc": 24,
	"text": 25,
	"oid": 26,
	"tid": 27,
	"xid": 28,
	"cid": 29,
	"oidvector": 30,
	"pg_type": 71,
	"pg_attribute": 75,
	"pg_proc": 81,
	"pg_class": 83,
	"xml": 142,
	"_xml": 143,
	"smgr": 210,
	"point": 600,
	"lseg": 601,
	"path": 602,
	"box": 603,
	"polygon": 604,
	"line": 628,
	"_line": 629,
	"float4": 700,
	"float8": 701,
	"abstime": 702,
	"reltime": 703,
	"tinterval": 704,
	"unknown": 705,
	"circle": 718,
	"_circle": 719,
	"money": 790,
	"_money": 791,
	"macaddr": 829,
	"inet": 869,
	"cidr": 650,
	"_bool": 1000,
	"_bytea": 1001,
	"_char": 1002,
	"_name": 1003,
	"_int2": 1005,
	"_int2vector": 1006,
	"_int4": 1007,
	"_regproc": 1008,
	"_text": 1009,
	"_oid": 1028,
	"_tid": 1010,
	"_xid": 1011,
	"_cid": 1012,
	"_oidvector": 1013,
	"_bpchar": 1014,
	"_varchar": 1015,
	"_int8": 1016,
	"_point": 1017,
	"_lseg": 1018,
	"_path": 1019,
	"_box": 1020,
	"_float4": 1021,
	"_float8": 1022,
	"_abstime": 1023,
	"_reltime": 1024,
	"_tinterval": 1025,
	"_polygon": 1027,
	"aclitem": 1033,
	"_aclitem": 1034,
	"_macaddr": 1040,
	"_inet": 1041,
	"_cstring": 1263,
	"bpchar": 1042,
	"varchar": 1043,
	"date": 1082,
	"time": 1083,
	"timestamp": 1114,
	"_timestamp": 1115,
	"_date": 1182,
	"_time": 1183,
	"timestamptz": 1184,
	"_timestamptz": 1185,
	"interval": 1186,
	"_interval": 1187,
	"_numeric": 1231,
	"timetz": 1266,
	"_timetz": 1270,
	"bit": 1560,
	"_bit": 1561,
	"varbit": 1562,
	"_varbit": 1563,
	"numeric": 1700,
	"refcursor": 1790,
	"_refcursor": 2201,
	"regprocedure": 2202,
	"regoper": 2203,
	"regoperator": 2204,
	"regclass": 2205,
	"regtype": 2206,
	"_regprocedure": 2207,
	"_regoper": 2208,
	"_regoperator": 2209,
	"_regclass": 2210,
	"_regtype": 2211,
	"uuid": 2950,
	"_uuid": 2951,
	"tsvector": 3614,
	"gtsvector": 3642,
	"tsquery": 3615,
	"regconfig": 3734,
	"regdictionary": 3769,
	"_tsvector": 3643,
	"_gtsvector": 3644,
	"_tsquery": 3645,
	"_regconfig": 3735,
	"_regdictionary": 3770,
	"txid_snapshot": 2970,
	"_txid_snapshot": 2949,
	"record": 2249,
	"_record": 2287,
	"cstring": 2275,
	"any": 2276,
	"anyarray": 2277,
	"void": 2278,
	"trigger": 2279,
	"language_handler": 2280,
	"internal": 2281,
	"opaque": 2282,
	"anyelement": 2283,
	"anynonarray": 2776,
	"anyenum": 3500
};
exports.TypeOIDsLookup=createLookup(exports.TypeOIDs);

exports.Describe=exports.Close={
	Portal: byte1('P'),
	Statement: byte1('S')
};