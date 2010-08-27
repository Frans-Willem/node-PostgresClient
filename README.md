# node-PostgresClient
This module is an implementation of the PostgreSQL Frontend-Backend protocol using Node.js' net and event libraries.
## Dependencies
node-PostgresClient depends on the following modules:

* [node-BufferLib](http://github.com/Frans-Willem/node-BufferLib) by [me](http://github.com/Frans-Willem/)
* [node-strtok](http://github.com/pgriess/node-strtok) by [Peter Griess](http://github.com/pgriess)

## Features supported
* Simple queries
* Parametrized queries
* Prepared statements
* LISTEN/NOTIFY
* COPY ... FROM STDIN, COPY ... TO STDOUT
* Blocks (Transactions)

## Multiple queries
Although multiple queries of the form:

	"SELECT 1; SELECT 2; SELECT 3;"

are supported in simpleQuery, it is _highly_ discouraged.
If one of the queries fails, all results from that command will be discarded.
## Creating a new client

	var PostgresClient=require("postgresclient").PostgresClient;
	var options={
		hostname: "localhost",
		database: "database",
		username: "username",
		password: "********"
	};
	var db=new PostgresClient(options);
## Events
### 'connect'
arguments: none
Succesfully connected to the PostgreSQL server.
### 'error'
arguments: error
Error, no more events will be thrown after this, and no more queries will be accepted.
### 'end'
arguments: none
Graceful connection close, no more events will be thrown after this, and no more queries will be accepted.
### 'notice'
arguments: notice
A notice was received from the server, for example when going down for a reboot.
The notice object can contain any of [these](http://developer.postgresql.org/pgdocs/postgres/protocol-error-fields.html) fields (by name, not by code).
See NoticeResponse command from the protocol.
### 'parameter'
arguments: name, value
See ParameterStatus command from the protocol.
### 'notification'
arguments: server_pid,name,payload
A NOTIFY was triggered on a name that was LISTEN'ed for.
See NotificationResponse protocol command.
## Simple queries
Simple queries without parameters can be sent like this:

	db.simpleQuery("SELECT 1;",function(err,rows,result) {
		if (err) {
			sys.puts("OH NOES! "+err);
			return;
		}
		sys.puts("Output from "+result);
		sys.puts(sys.inspect(rows));
	});

Note that the callback can also be an object as described in Query callbacks.
## Extended queries
For queries with parameters, you should use extendedQuery, like this:

	db.simpleQuery("SELECT $1::int AS one, $2::int AS two, $3::int AS three;",[1,2,3],function(err,rows,result) {
		if (err) {
			sys.puts("OH NOES! "+err);
			return;
		}
		sys.puts("Output from "+result);
		sys.puts(sys.inspect(rows));
	});

Again, the callback can also be an object.
Also, please note that although this example specifies type ('::int') and names (' AS one') for each parameter, this is only needed when PostgreSQL can't find out on its own.
In normal queries, '$1' and '$2' will usually suffice.
## Prepared statements
### Raw method
The normal method for creating and using prepared statements is as follows:

	db.parse("SELECT $1::int AS one;",function(err,name) {
		if (err) {
			sys.puts("Error parsing statement: "+err);
			return;
		}
		db.bindExecute(name,[1],function(err,rows,result) {
			if (err) {
				sys.puts("Error binding statement: "+err);
				return;
			}
			sys.puts("Output from "+result);
			sys.puts(sys.inspect(rows));
			
			db.closeStatement(name,function(err) {
				if (err) {
					sys.puts("Unable to close prepared statement");
				} else {
					sys.puts("Closed prepared statement");
				}
			});
		});
	});

For bindExecute, callback can also be an object. parse and closeStatement should have the form of the example above.
The name given to the prepared statement will be automatically generated, and you should not depend on it having a certain value, but rather just store it after the .parse callback.
If you no longer intend to use a prepared statement, you should call closeStatement to allow the database to discard it.
If you intend to use the prepared statement over the course of the database connection, you can safely ignore closeStatement, as it will be cleaned up automatically when the connection is closed.
### Helper
Because the above method is rather cumbersome, a helper function was added:

	var p=db.prepare("SELECT $1::int AS one;");
	p.execute([1],function(err,rows,result) {
		/* this should be obvious by now */
	});
	p.close(function(err) {});
	
db.prepare will create a helper object with the execute and close functions as wrapper for bindExecute and closeStatement.
You do not have to wait for the parsing to be done, all queries or close functions will be held until the parse is complete.
## LISTEN/NOTIFY
LISTEN and NOTIFY can be sent as simple, extended, or prepared queries. A response from a LISTEN command will be emitted as a 'notification' event.
## COPY ... FROM STDIN, COPY ... TO STDOUT
COPY ... FROM STDIN and COPY ... TO STDOUT are supported by simple, extended, or prepared queries.
### COPY ... TO STDOUT
For a COPY TO operation, simply implement the copyOut, copyData, or copyDone callbacks.
### COPY ... FROM STDIN
For a COPY FROM operation, you _must_ implement a copyIn callback.
Because special care must be taken with CopyIn commands to not send anything else until the command is done, you have to tell the client that a CopyIn is expected by specifying a copyIn callback.

If you're not sure if a query contains COPY ... FROM STDIN (for example when the query comes from user input), you should _always_ implement the copyIn callback, and call stream.fail("Not prepared"); if you're not prepared to handle the COPY command.

If you do a COPY ... FROM STDIN command without specifying a copyIn callback, the client will automatically disconnect to prevent any desynchronization of the protocol.
## Blocks (Transactions)
Sometimes you need to make sure a bunch of queries are executed without interruptions.
Normally this can be achieved by simply doing the queries in succession, but due to the asynchronous evented nature of Node.js, this is not always possible.
This is why node-PostgresClient has blocks. A block is a promise that between the start and end of the block, _only_ queries belonging to that transaction will be executed.
Please note that you should always make sure a block is properly ended. If you don't, queries will be paused forever, and the database will be unusable.
For an example of this, see blocktest.js
### Starting

	bl=db.startBlock();

### Ending

	db.endBlock(bl);

### Sending

	db.simpleQuery("SELECT 1;",bl,callback);
	db.extendedQuery("SELECT $1::int;",[1],bl,callback);
	db.bindExecute(statement,[1],bl,callback);
	db.prepare("SELECT $1::int;").execute([1],bl,callback);
### Sub blocks
Blocks can also be part of other blocks. e.g. the following workflow:

	open transaction 1
	send Q6 without a transaction
	send Q1 to transaction 1
	open transaction 1.1
	send Q2 to transaction 1.1
	send Q4 to transaction 1
	send Q3 to transaction 1.1
	end transaction 1.1
	send Q5 to transaction 1
	end transaction 1

Will result in the following order of queries being executed:

	Q1,Q2,Q3,Q4,Q5,Q6
	
You can open a subtransaction like this:

	var subblock=db.startBlock(parentblock);

### Transactions
An example of a transaction and proper error handling would look like this:

	var block=db.startBlock();
	db.simpleQuery("BEGIN TRANSACTION;",block,function(err,rows,result) {
		if (err) {
			sys.puts("BEGIN failed");
			db.endBlock(block);
			return;
		}
		db.simpleQuery("UPDATE table SET x=x+1 WHERE y=0;",block,function(err,rows,result) {
			if (err) {
				sys.puts("UPDATE(1) failed");
				db.simpleQuery("ROLLBACK TRANSACTION;",block,function(){});
				db.endBlock(block);
				return;
			}
			db.simpleQuery("UPDATE table SET z=z+1 WHERE z=1;",block,function(err,rows,result) {
				if (err) {
					sys.puts("UPDATE(2) failed");
					db.simpleQuery("ROLLBACK TRANSACTION;",block,function(){});
					db.endBlock(block);
					return;
				} else {
					db.simpleQuery("COMMIT TRANSACTION;",block,function(err,rows,result) {
						if (err) {
							sus.puts("COMMIT failed");
							db.simpleQuery("ROLLBACK TRANSACTION;",block,function() {});
							db.endBlock(block);
						} else {
							sys.puts("Committed");
							db.endBlock(block);
						}
					}
				}
			});
		});
	});

This example will do:

	BEGIN TRANSACTION;
	UPDATE table SET x=x+1 WHERE y=0;
	UPDATE table SET z=z+1 WHERE x=1;
	COMMIT;

While making sure that no other queries will be interspersed with it by using a block, as well as doing a ROLLBACK if any command goes wrong.

## Query callbacks
A callback can be a simple function, in which case it should be of the form:

	function finalCallback(err,rows1,result1,rows2,result2,...,rowsn,resultn) {
		/*
			rows1 are the rows from the first query, rows2 from the second
			Each row is either an object, or an array (in case of all numeric or unnamed columns)
			result1 will be something like "SELECT" for select queries, or "UPDATE n" for update queries.
			(see CommandComplete in http://developer.postgresql.org/pgdocs/postgres/protocol-message-formats.html)
		*/
	}

While noting that only simpleQuery supports multiple queries, and as such any callback from extendedQuery will always be of the form:

	function finalCallback(err,rows,result) {
		//...
	}

In case you need more control over the callbacks, it can be an object of the following form:

	{
		rowDescription: function(descr) {
			/*
			descr is an array of columns, where each column has the following form:
			{
				fieldName: String,
				tableOID: Integer,
				columnAttribute: Integer,
				datatypeOID: Integer,
				datatypesize: Integer,
				typemodifier: Integer,
				formatcode: Integer
			}
			
			See RowDescription command for more info
			*/ 
		},
		dataRow: function(row) {
			/*
			Signals a new data row
			row is now an object or an array
			Note that fields with a name of '?column?' will be set by their index, instead of their name.
			See DataRow command for more info
			*/ 
		},
		copyOut: function(format,columns) {
			/*
			Signals the beginning of a COPY ... TO STDOUT command.
			See CopyOutResponse for more info
			*/
		},
		copyData: function(buffer) {
			/*
			Signals data from a COPY ... TO STDOUT command.
			Note that PostgreSQL promises that the backend will send each row in a seperate CopyData
			As such, there is no need to buffer and split on newlines,
			instead just ignore the final newline, and treat each CopyData as a seperate row.
			See CopyOutData for more info
			*/
		},
		copyDone: function() {
			/*
			Signals the end of a COPY ... TO STDOUT command.
			See CopyOutResponse for more info
			*/
		},
		copyIn: function(str) {
			/*
			Signals a CopyIn operation.
			str is similar to a WritableStream
			str.write(buffer) to send CopyData,
			str.end(buffer) to send CopyDone,
			str.destroy() to send a generic CopyFail,
			or str.fail(message) to send a CopyFail with a message.
			See CopyInResponse for more info.
			*/
		},
		commandComplete: function(rows,result) {
			/*
			Signals the end of a query.
			Note that rows will be empty if a dataRow function exists.
			See CommandComplete for more info.
			*/
		},
		complete: function(err,rows,result,rows1,result1,...) {
			/*
			Signals the end of all queries contained in the request.
			Note that rows and result will not be passed if a commandComplete function exists.
			*/
		}
	}

Please note that the "complete" function, with or without an error, will signal the end of the query, and no other methods of your callback object will be called after this.
## Protocol overview
A list of protocol commands and their explanation can be found [here](http://www.postgresql.org/docs/8.2/static/protocol.html).