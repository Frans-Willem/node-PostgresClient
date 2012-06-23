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

var bl=client.startBlock();


setTimeout(function() {
	util.puts("GO!");
	client.endBlock(bl);
},1000);

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
		
		client.simpleQuery("COPY testtable FROM STDIN DELIMITER '\t';",{
			copyIn: function(handler) {
				handler.write("10\t12\n");
				handler.write("18\t20\n");
				handler.end();
			},
			complete: function(err,rows,result) {
				if (err) {
					util.puts("COPY FROM Error: "+err);
					return;
				}
				util.puts("COPY FROM Callback Arguments:");
				util.puts("\trows: "+util.inspect(rows));
				util.puts("\tresult: "+util.inspect(result));
				client.simpleQuery("COPY testtable TO STDOUT DELIMITER '\t';",{
				copyOut:function(format,columns) {
					util.puts("CopyOut: "+format);
				},
				copyData:function(data) {
					util.puts("CopyData: "+data.toString('utf8'));
				},
				copyDone:function() {
					util.puts("CopyDone");
				},
				complete: function(err,rows,result) {
					if (err) {
						util.puts("COPY Error: "+err);
						return;
					}
					util.puts("COPY Callback Arguments:");
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
				}
			});
			}
		});
	});
});