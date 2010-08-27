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

var bl=client.startBlock();


setTimeout(function() {
	sys.puts("GO!");
	client.endBlock(bl);
},1000);

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
		
		client.simpleQuery("COPY testtable FROM STDIN DELIMITER '\t';",{
			copyIn: function(handler) {
				handler.write("10\t12\n");
				handler.write("18\t20\n");
				handler.end();
			},
			complete: function(err,rows,result) {
				if (err) {
					sys.puts("COPY FROM Error: "+err);
					return;
				}
				sys.puts("COPY FROM Callback Arguments:");
				sys.puts("\trows: "+sys.inspect(rows));
				sys.puts("\tresult: "+sys.inspect(result));
				client.simpleQuery("COPY testtable TO STDOUT DELIMITER '\t';",{
				copyOut:function(format,columns) {
					sys.puts("CopyOut: "+format);
				},
				copyData:function(data) {
					sys.puts("CopyData: "+data.toString('utf8'));
				},
				copyDone:function() {
					sys.puts("CopyDone");
				},
				complete: function(err,rows,result) {
					if (err) {
						sys.puts("COPY Error: "+err);
						return;
					}
					sys.puts("COPY Callback Arguments:");
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
				}
			});
			}
		});
	});
});