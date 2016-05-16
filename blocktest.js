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

var tr0=client.startTransaction();
var tr1=client.startTransaction();
var tr2=client.startTransaction();

client.simpleQuery("SELECT 1;",tr0,function(err,rows,result) {
	if (err) {
		util.puts("Error:"+err);
		client.endTransaction(tr0);
		return;
	}
	util.puts("Got "+rows[0][0]);
	client.simpleQuery("SELECT 2;",tr0,function(err,rows,result) {
		if (err) {
			util.puts("Error:"+err);
			client.endTransaction(tr0);
			return;
		}
		util.puts("Got "+rows[0][0]);
		client.simpleQuery("SELECT 3;",tr0,function(err,rows,result) {
			if (err) {
				util.puts("Error:"+err);
				client.endTransaction(tr0);
				return;
			}
			util.puts("Got "+rows[0][0]);
			setTimeout(function() {
				util.puts("Ending transaction 0");
				client.endTransaction(tr0);
			},500);
		});
	});
});

client.simpleQuery("SELECT 4;",tr1,function(err,rows,result) {
	if (err) {
		util.puts("Error:"+err);
		client.endTransaction(tr1);
		return;
	}
	util.puts("Got "+rows[0][0]);
	var tr1_1=client.startTransaction(tr1);
	try {
		client.simpleQuery("SELECT 5",tr1_1,function(err,rows,result) {
			if (err) {
				util.puts("Error:"+err);
				client.endTransaction(tr1_1);
				return;
			}
			util.puts("Got "+rows[0][0]);
			client.simpleQuery("SELECT 6;",tr1_1,function(err,rows,result) {
				if (err) {
					util.puts("Error:"+err);
					client.endTransaction(tr1_1);
					return;
				}
				util.puts("Got "+rows[0][0]);
				setTimeout(function() {
					util.puts("Ending transaction 1.1");
					client.endTransaction(tr1_1);
				},500);
			});
		});
	}
	catch(e) {
		util.puts("Caught");
		util.puts(e.stack);
	}
	client.simpleQuery("SELECT 7;",tr1,function(err,rows,result) {
		if (err) {
			util.puts("Error:"+err);
			client.endTransaction(tr1);
			return;
		}
		util.puts("Got "+rows[0][0]);
		client.simpleQuery("SELECT 8;",tr1,function(err,rows,result) {
			if (err) {
				util.puts("Error:"+err);
				client.endTransaction(tr1);
				return;
			}
			util.puts("Got "+rows[0][0]);
			setTimeout(function() {
				util.puts("Ending transaction 1");
				client.endTransaction(tr1);
			},500);
		});
	});
});

client.simpleQuery("SELECT 9;",tr2,function(err,rows,result) {
	if (err) {
		util.puts("Error:"+err);
		client.endTransaction(tr2);
		return;
	}
	util.puts("Got "+rows[0][0]);
	setTimeout(function() {
		util.puts("Ending transaction 2");
		client.endTransaction(tr2);
	},500);
});

client.simpleQuery("SELECT 10",function(err,rows,result) {
	if (err) {
		util.puts("Error:"+err);
		client.end();
		return;
	}
	util.puts("Got "+rows[0][0]);
	client.end();
});