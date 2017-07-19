const _ = require('lodash');
const fs = require('fs');
const csv = require('fast-csv');
const makeSource = require("stream-json");
const Assembler  = require("stream-json/utils/Assembler");
const sourcefile = './testETHUSD.json';

const tradeSize = 5; //when calculating the weighted avg price, how much ETH do you want to be able to trade?
let source    = makeSource(),
    assembler = new Assembler(),
    output = [];

source.output.on("data", function(chunk){
    assembler[chunk.name] && assembler[chunk.name](chunk.value);
});
source.output.on("end", function(){
    // here is our fully assembled object:
    let file = assembler.current, //Takes about 1min to read and assemble the entire ethusd.json object and takes up 400mb of ram
        csvStream = csv.createWriteStream({headers: true}),
        writeableStream = fs.createWriteStream("geminiETHUSD.csv");

    writeableStream.on("finish", function () {
        console.log('Done!');
    });

    //csvStream.pipe(writeableStream);

    //console.log(file[0].data.events[0].price);

    //helper function so I can calculate a sum using array.reduce()
    function add(a,b){
        return a + b;
    }

    let currentTime = file[0].time; //Set the time as when we recieved the orderbook data
    //console.log(currentTime);

    //Read in the initial state of the orderbook ... split this up into bid and ask objects and you can get rid of lines 58-69
    let obBid = [];
    let obAsk = [];
    file[0].data.events.forEach(function(event) {
        if(event.side == 'bid'){
            obBid.push({
                "price": parseFloat(event.price, 10),
                "remaining": parseFloat(event.remaining, 10)
            });   
        } else {
            obAsk.push({
                "price": parseFloat(event.price, 10),
                "remaining": parseFloat(event.remaining, 10)
            });   
        }

    });

    //Make sure the orderbook array is sorted in ascending order by price
    obBid.sort(function(a,b) { return a.price - b.price;}); 
    obAsk.sort(function(a,b) { return a.price - b.price;}); 

    //Should I also sort the whole file array by file[i].time to ensure that when we loop through below it's in ascending order?
    //Choose to use a for loop so I can easily skip over the 0 element which is the initial state of the orderbook, and for loops are faster and this is where the bulk of the proccessing will happen
    for(let i = 1; i < file.length; i++) {
        //Check to see if we're onto the next second, if so write to the csv
        if(parseInt(file[i].time) > parseInt(currentTime)) {
            obBid.sort(function(a,b) { return a.price - b.price;}); //Sort to make sure that any insertions done durring the last second are sorted
            obAsk.sort(function(a,b) { return a.price - b.price;}); //Sort to make sure that any insertions done durring the last second are sorted

            let highestBid = obBid[obBid.length-1];
            let highestBidIndex = obBid.length-1;
            let lowestAskIndex = 0;
            let lowestAsk = obAsk[lowestAskIndex];

            //Split order book into bid and ask sides
            // obBid = ob.filter(function(priceLevel) {
            //     return priceLevel.side == "bid"
            // });
            // 
            
            // obAsk = ob.filter(function(priceLevel) {
            //     return priceLevel.side == "ask"
            // });
            // 
            
            /*
            //Find the highest bid price and save it if remaining >= tradeSize
            let highestBid = _.maxBy(obBid, function(priceLevel) { 
                    return parseFloat(priceLevel.price);
            });
            let highestBidIndex = _.findIndex(obBid, function(level) 
            {
                return (level.side == highestBid.side) && (level.price == highestBid.price);
            });
            */
            // console.dir(highestBid);
            // console.log("at ob index" + highestBidIndex);
            // console.dir(lowestAsk);

            //calcualte the price for 5ETH bid and ask
            let bid = 0.0;
            let ask = 0.0;
            //work your way down the bid orderbook till you have a weighted average price for the whole tradeSize
            let remainingTradeSize = tradeSize;
            let priceVolume = [];
            while (remainingTradeSize > 0){
                if((obBid[highestBidIndex].remaining) > remainingTradeSize) {
                    //This is the last price level we need to calculate from
                    priceVolume.push((obBid[highestBidIndex].price) * remainingTradeSize)
                    remainingTradeSize -= remainingTradeSize;
                } else {
                    //Calculate and store the priceVolume for the volume available at this price level, decrease the remainingTrade size by the volume we got and move to the next highest bid
                    priceVolume.push((obBid[highestBidIndex].price) * (obBid[highestBidIndex].remaining));
                    remainingTradeSize -= (obBid[highestBidIndex].remaining);
                    highestBidIndex--;
                }
            }
            bid = priceVolume.reduce(add, 0) / tradeSize;
            //console.log("Weighted average bid for " + tradeSize + " ETH is " + bid);

            //work your way up the ask orderbook till you have a weighted average price for the whole tradeSize
            remainingTradeSize = tradeSize;  //reset so we can reuse this variable
            priceVolume = []; //reset so we can reuse this variable
            while (remainingTradeSize > 0){
                if((obAsk[lowestAskIndex].remaining) > remainingTradeSize) {
                    //This is the last price level we need to calculate from
                    priceVolume.push((obAsk[lowestAskIndex].price) * remainingTradeSize)
                    remainingTradeSize -= remainingTradeSize;
                } else {
                    //Calculate and store the priceVolume for the volume available at this price level, decrease the remainingTrade size by the volume we got and move to the next lowest ask
                    priceVolume.push((obAsk[lowestAskIndex].price) * (obAsk[lowestAskIndex].remaining));
                    remainingTradeSize -= (obAsk[lowestAskIndex].remaining);
                    lowestAskIndex++;
                }
            }
            ask = priceVolume.reduce(add, 0) / tradeSize;
            //console.log("Weighted average ask for " + tradeSize + " ETH is " + ask);        


            //Write to csv
            //Writing to file system is slow as fuck ... if you can save writing to the csv till one point at the very end it'll be a lot fast
            output.push({
                unixTime: parseInt(currentTime),
                bid: bid,
                ask: ask,
                tradeSize: tradeSize
            });
            //console.log("At time:" + parseInt(currentTime));
            if(parseInt(currentTime) % 60 == 0){
                console.log(time(currentTime));
            }
        }
        currentTime = file[i].time;
        //console.log("same second currentTime:" + currentTime + " nextTime:" + file[i].time);

        //if there is a trade there will be multiple events, we're only interested in the 'change' events as they change the orderbook
        file[i].data.events.forEach(function(event) {
            
            if(event.type === "change" && event.side === 'bid'){
                //check to see if the update is to an existing level and save the index of that level if it is
                let priceLevel = _.findIndex(obBid, function(level) {
                    return (level.price == event.price);
                });
                //if new, add that price level to the orderbook
                if(priceLevel == -1){
                    obBid.push({
                        "price": parseFloat(event.price),
                        "remaining": parseFloat(event.remaining)
                    });
                    //ob.sort(function(a,b) { return (a.price) - (b.price);}); //Optimization, don't need to do this each time b/c I do it once a second
                    //console.log("Orderbook size: " + ob.length);
                } else {
                    //update that pricelevel in the orderbook
                    obBid[priceLevel].remaining = event.remaining;
                    //Remove the priceLevel if remaining volume is 0
                    if(obBid[priceLevel].remaining == "0"){
                        obBid.splice(priceLevel, 1);
                    }
                    //console.log("Orderbook size: " + ob.length);        
                }          
            } else if (event.type === "change" && event.side === 'ask'){
                //check to see if the update is to an existing level and save the index of that level if it is
                let priceLevel = _.findIndex(obAsk, function(level) {
                    return (level.price == event.price);
                });
                //if new, add that price level to the orderbook
                if(priceLevel == -1){
                    obAsk.push({
                        "price": parseFloat(event.price),
                        "remaining": parseFloat(event.remaining)
                    });
                    //ob.sort(function(a,b) { return (a.price) - (b.price);}); //Optimization, don't need to do this each time b/c I do it once a second
                    //console.log("Orderbook size: " + ob.length);
                } else {
                    //update that pricelevel in the orderbook
                    obAsk[priceLevel].remaining = event.remaining;
                    //Remove the priceLevel if remaining volume is 0
                    if(obAsk[priceLevel].remaining == "0"){
                        obAsk.splice(priceLevel, 1);
                    }
                    //console.log("Orderbook size: " + ob.length);        
                }          
            }

        });



    }
    //Write to csv
    //Writing to file system is slow as fuck ... if you can save writing to the csv till one point at the very end it'll be a lot fast
    csv.write(output, {headers: true}).pipe(writeableStream);
    csvStream.end();
});
 
fs.createReadStream(sourcefile).pipe(source.input);

/**
 * Convert seconds to time string (hh:mm:ss).
 *
 * @param Number s
 *
 * @return String
 */
function time(s) {
    return new Date(s * 1e3).toISOString().slice(-13, -5);
}

