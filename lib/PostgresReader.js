var BufferReader=require("bufferlib/BufferReader").BufferReader;
var sys=require("sys");

function PostgresReader(buffer) {
	BufferReader.call(this,buffer);
}
sys.inherits(PostgresReader,BufferReader);
exports.PostgresReader=PostgresReader;

PostgresReader.prototype.popMultiStringZero=function(encoding) {
	var ret=[],cur;
	while ((cur=this.popStringZero(encoding)).length>0)
		ret.push(cur);
	return ret;
}