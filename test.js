var PostgresClient=require("./lib/PostgresClient").PostgresClient;
var Config=require("../Config");
var util = require('util');
var net=require("net");

var client=new PostgresClient(Config.dbInfo);
client.on("error",function(err) {
	util.puts("Error: "+err+" "+err.stack);
});
client.on("connect",function() {
	util.puts("Connect");
});
client.on("end",function() {
	util.puts("End");
});

//Simple queries
client.simpleQuery("SELECT 1,2,3",function(err,rows,result) {
	if (err) {
		util.puts("Simple query failed: "+err);
		return;
	}
	util.puts("Simple query results: "+util.inspect(rows)+", "+result);
});
//Parametrized queries
client.extendedQuery("SELECT int4($1) as first, int4($2) as second, int4($3) as third",[4,5,6],function(err,rows,result) {
	if (err) {
		util.puts("Extended query failed: "+err);
		return;
	}
	util.puts("Extended query results: "+util.inspect(rows)+", "+result);
});
//Prepared statements extended
client.parse("SELECT $1, $2, $3",function(err,statementName) {
	if (err) {
		util.puts("Unable to parse prepared statement");
		return;
	}
	client.bindExecute(statementName,[7,8,9],function(err,rows,result) {
		//Clean up the statement after we've used it.
		client.closeStatement(statementName,function() {});
		if (err) {
			util.puts("Unable to execute statement: "+err);
			return;
		}
		util.puts("Extended prepared query results: "+util.inspect(rows)+", "+result);
	});
});

//Prepared statements simple
var preparedStatement=client.prepare("SELECT int4($1)+int4($2), $3;");

preparedStatement.execute([10,11,12],function(err,rows,result) {
	//Close it, we're no longer using it.
	preparedStatement.close(function() {});
	if (err) {
		util.puts("Unable to execute prepared statement: "+err);
		return;
	}
	util.puts("Simple prepared query results: "+util.inspect(rows)+", "+result);
});

//LISTEN & NOTIFY
client.simpleQuery("LISTEN seashell;",function(err,rows,result) {
	if (err) {
		util.puts("Unable to LISTEN");
	} else {
		//Not sending a payload as only Postgres 9.0+ support it.
		client.simpleQuery("NOTIFY seashell;",function(err,rows,result) {
			if (err) {
				util.puts("Can't NOTIFY");
			} else {
				util.puts("We should get a notification")
			}
		});
	}
});

client.on("notification",function(processid,name,payload) {
	util.puts("Got a notification!");
	util.puts("\tname: "+name);
	util.puts("\tpayload: "+payload);
});

client.on("notice",function(notice) {
	util.puts("Got a notice:");
	util.puts(util.inspect(notice));
	//Example:
	//  {"Message":"Hello world","Severity":"NOTICE"}
	//See http://developer.postgresql.org/pgdocs/postgres/protocol-error-fields.html for all fields
});

//CREATE/DELETE/INSERT/DROP support
client.simpleQuery("CREATE TABLE testtable (first int4, second int4);",function(err,rows,result) {
	if (err) {
		util.puts("CREATE TABLE Error: "+err);
		return;
	}
	util.puts("CREATE TABLE Callback Arguments:");
	util.puts("\trows: "+util.inspect(rows));
	util.puts("\tresult: "+util.inspect(result));
	client.simpleQuery("INSERT INTO testtable (first,second) VALUES(1,2);INSERT INTO testtable (first,second) VALUES (3,4),(5,6);",function(err,rows1,result1,rows2,result2) {
		if (err) {
			util.puts("INSERT INTO Error: "+err);
			return;
		}
		util.puts("INSERT INTO Callback Arguments:");
		util.puts("\trows1: "+util.inspect(rows1));
		util.puts("\tresult1: "+util.inspect(result1));
		util.puts("\trows2: "+util.inspect(rows2));
		util.puts("\tresult2: "+util.inspect(result2));
		
		client.simpleQuery("SELECT * FROM testtable;",function(err,rows,result) {
			if (err) {
				util.puts("SELECT Error: "+err);
				return;
			}
			util.puts("SELECT Callback Arguments:");
			util.puts("\trows: "+util.inspect(rows));
			util.puts("\tresult: "+util.inspect(result));
			client.simpleQuery("DROP TABLE testtable;",function(err,rows,result) {
				if (err) {
					util.puts("DROP TABLE Error: "+err);
					return;
				}
				util.puts("DROP TABLE Callback Arguments:");
				util.puts("\trows: "+util.inspect(rows));
				util.puts("\tresult: "+util.inspect(result));
			});
		});
	});
});