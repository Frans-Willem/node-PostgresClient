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

//LISTEN & NOTIFY
client.simpleQuery("LISTEN seashell;",function(err,rows,result) {
	if (err) {
		sys.puts("Unable to LISTEN");
	} else {
		//Not sending a payload as only Postgres 9.0+ support it.
		client.simpleQuery("NOTIFY seashell;",function(err,rows,result) {
			if (err) {
				sys.puts("Can't NOTIFY");
			} else {
				sys.puts("We should get a notification")
			}
		});
	}
});

client.on("notification",function(processid,name,payload) {
	sys.puts("Got a notification!");
	sys.puts("\tname: "+name);
	sys.puts("\tpayload: "+payload);
});

client.on("notice",function(notice) {
	sys.puts("Got a notice:");
	sys.puts(sys.inspect(notice));
	//Example:
	//  {"Message":"Hello world","Severity":"NOTICE"}
	//See http://developer.postgresql.org/pgdocs/postgres/protocol-error-fields.html for all fields
});

//CREATE/DELETE/INSERT/DROP support
client.simpleQuery("CREATE TABLE testtable (first int4, second int4);",function(err,rows,result) {
	if (err) {
		sys.puts("CREATE TABLE Error: "+err);
		return;
	}
	sys.puts("CREATE TABLE Callback Arguments:");
	sys.puts("\trows: "+sys.inspect(rows));
	sys.puts("\tresult: "+sys.inspect(result));
	client.simpleQuery("INSERT INTO testtable (first,second) VALUES(1,2);INSERT INTO testtable (first,second) VALUES (3,4),(5,6);",function(err,rows1,result1,rows2,result2) {
		if (err) {
			sys.puts("INSERT INTO Error: "+err);
			return;
		}
		sys.puts("INSERT INTO Callback Arguments:");
		sys.puts("\trows1: "+sys.inspect(rows1));
		sys.puts("\tresult1: "+sys.inspect(result1));
		sys.puts("\trows2: "+sys.inspect(rows2));
		sys.puts("\tresult2: "+sys.inspect(result2));
		
		client.simpleQuery("SELECT * FROM testtable;",function(err,rows,result) {
			if (err) {
				sys.puts("SELECT Error: "+err);
				return;
			}
			sys.puts("SELECT Callback Arguments:");
			sys.puts("\trows: "+sys.inspect(rows));
			sys.puts("\tresult: "+sys.inspect(result));
			client.simpleQuery("DROP TABLE testtable;",function(err,rows,result) {
				if (err) {
					sys.puts("DROP TABLE Error: "+err);
					return;
				}
				sys.puts("DROP TABLE Callback Arguments:");
				sys.puts("\trows: "+sys.inspect(rows));
				sys.puts("\tresult: "+sys.inspect(result));
			});
		});
	});
});