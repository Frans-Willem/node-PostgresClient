var PostgresClient=require("./lib/PostgresClient").PostgresClient;
var Config=require("../Config");
var sys=require("sys");
var net=require("net");

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

//Simple queries
client.simpleQuery("SELECT 1,2,3",function(err,rows,result) {
	if (err) {
		sys.puts("Simple query failed: "+err);
		return;
	}
	sys.puts("Simple query results: "+sys.inspect(rows)+", "+result);
});
//Parametrized queries
client.extendedQuery("SELECT int4($1) as first, int4($2) as second, int4($3) as third",[4,5,6],function(err,rows,result) {
	if (err) {
		sys.puts("Extended query failed: "+err);
		return;
	}
	sys.puts("Extended query results: "+sys.inspect(rows)+", "+result);
});
//Prepared statements extended
client.parse("SELECT $1, $2, $3",function(err,statementName) {
	if (err) {
		sys.puts("Unable to parse prepared statement");
		return;
	}
	client.bindExecute(statementName,[7,8,9],function(err,rows,result) {
		//Clean up the statement after we've used it.
		client.closeStatement(statementName,function() {});
		if (err) {
			sys.puts("Unable to execute statement: "+err);
			return;
		}
		sys.puts("Extended prepared query results: "+sys.inspect(rows)+", "+result);
	});
});

//Prepared statements simple
var preparedStatement=client.prepare("SELECT int4($1)+int4($2), $3;");

preparedStatement.execute([10,11,12],function(err,rows,result) {
	//Close it, we're no longer using it.
	preparedStatement.close(function() {});
	if (err) {
		sys.puts("Unable to execute prepared statement: "+err);
		return;
	}
	sys.puts("Simple prepared query results: "+sys.inspect(rows)+", "+result);
});