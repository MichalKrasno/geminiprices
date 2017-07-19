var makeSource = require("stream-json");
var Assembler  = require("stream-json/utils/Assembler");
var fs = require('fs');
 
var source    = makeSource(),
    assembler = new Assembler();
 
// Example of use: 
 
source.output.on("data", function(chunk){
  assembler[chunk.name] && assembler[chunk.name](chunk.value);
});
source.output.on("end", function(){
  // here is our fully assembled object: 
  console.log(assembler.current[0].data.events[0]);
  console.dir(assembler.current.length);
});
 
fs.createReadStream("ethusd.json").pipe(source.input);
