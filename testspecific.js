var PostgresClient=require("./lib/PostgresClient").PostgresClient;
var Config=require("../Config");
var sys=require("sys");
var net=require("net");
var PostgresEncoder=require("./lib/PostgresEncoder").PostgresEncoder;
var Constants=require("./lib/PostgresConstants");

var client=new PostgresClient(Config.dbInfo);
client.on("error",function(err) {
	sys.puts("Error: "+err+" "+err.stack);
});
client.on("connect",function() {
	sys.puts("Connect");
});
client.on("end",function() {
	sys.puts("End");
});

/*var frame=PostgresEncoder.prototype.frame;
PostgresEncoder.prototype.frame=function() {
	sys.puts("Framing: "+Constants.MessageTypesLookup.Frontend[this.header]);
	var out=new Buffer(this.length);
	this.data.forEach(function(d) {
		var len=d(out,0);
		var cur=out.slice(0,len);
		var ascii=Array.prototype.every.call(cur,function(x) { return x>=30 && x<200; });
		sys.puts("\t"+(ascii?cur.toString('utf8'):sys.inspect(cur)));
	});
	return frame.apply(this,arguments);
}*/

client.socket.write=function(data) {
	sys.puts("<< "+sys.inspect(data));
	return this.__proto__.write.apply(this,arguments);
}

var onData=client.socket.listeners('data')[0];
client.socket.listeners('data')[0]=function(data) {
	sys.puts(">> "+sys.inspect(data));
	return onData.apply(this,arguments);
}

//Parametrized queries
client.extendedQuery("SELECT int4($1) as first, int4($2) as second, int4($3) as third",[4,5,6],function(err,x,rows,result) {
	if (err) {
		sys.puts("Extended query failed: "+err);
		return;
	}
	sys.puts("Extended query results: "+sys.inspect(rows)+", "+result);
});