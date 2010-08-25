var BufferBuilder=require("bufferlib/BufferBuilder").BufferBuilder;
var sys=require("sys");

function PostgresEncoder(header) {
	BufferBuilder.call(this);
	this.header=header;
}
sys.inherits(PostgresEncoder,BufferBuilder);
exports.PostgresEncoder=PostgresEncoder;

PostgresEncoder.prototype.pushHash=function(obj,encoding) {
	var self=this;
	Object.keys(obj).forEach(function(k) {
		self.pushStringZero(k,encoding);
		self.pushStringZero(String(obj[k]),encoding);
	});
	self.pushStringZero("",encoding);
	return this;
}
PostgresEncoder.prototype.frame=function() {
	var writer=new BufferBuilder();
	if (this.header!==undefined) {
		writer.pushByte(this.header);
	}
	writer.pushIntBE(this.length+4,4);
	writer.pushBuilder(this);
	return writer.toBuffer();
}