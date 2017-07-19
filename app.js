const _ = require('lodash');
const fs = require('fs');
const csv = require('fast-csv');
const file = require('./testETHUSD.json');
const tradeSize = 5; //when calculating the weighted avg price, how much ETH do you want to be able to trade?

let csvStream = csv.createWriteStream({headers: true}),
    writeableStream = fs.createWriteStream("geminiETHUSD.csv");

writeableStream.on("finish", function () {
    console.log('Done!');
});

csvStream.pipe(writeableStream);

//console.log(file[0].data.events[0].price);

//helper function so I can calculate a sum using array.reduce()
function add(a,b){
    return a + b;
}

let currentTime = file[0].time; //Set the time as when we recieved the orderbook data
//console.log(currentTime);

//Read in the initial state of the orderbook
let ob = [];
file[0].data.events.forEach(function(event) {
    ob.push({
        "side": event.side,
        "price": event.price,
        "remaining": event.remaining
    });
});

//Make sure the orderbook array is sorted in ascending order by price
ob.sort(function(a,b) { return parseFloat(a.price) - parseFloat(b.price);}); //Q: Is there any way to abstract this? I reuse the exact same code to sort the orderbook whenever I add a new price level

//Should I also sort the whole file array by file[i].time to ensure that when we loop through below it's in ascending order?
//Choose to use a for loop so I can easily skip over the 0 element which is the initial state of the orderbook, and for loops are faster and this is where the bulk of the proccessing will happen
for(let i = 1; i < file.length; i++) {
    //Check to see if we're onto the next second, if so write to the csv
    if(parseInt(file[i].time) > parseInt(currentTime)) {

        //Split order book into bid and ask sides
        obBid = ob.filter(function(priceLevel) {
            return priceLevel.side == "bid"
        });
        obBid.sort(function(a,b) { return parseFloat(a.price) - parseFloat(b.price);}); //Q: Do I really need to sort? I feel like I'm just making sure
        let highestBid = obBid[obBid.length-1];
        let highestBidIndex = obBid.length-1;
        obAsk = ob.filter(function(priceLevel) {
            return priceLevel.side == "ask"
        });
        obAsk.sort(function(a,b) { return parseFloat(a.price) - parseFloat(b.price);}); //Q: Do I really need to sort? I feel like I'm just making sure
        let lowestAskIndex = 0;
        let lowestAsk = obAsk[lowestAskIndex];
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
            if(parseFloat(obBid[highestBidIndex].remaining) > remainingTradeSize) {
                //This is the last price level we need to calculate from
                priceVolume.push(parseFloat(obBid[highestBidIndex].price) * remainingTradeSize)
                remainingTradeSize -= remainingTradeSize;
            } else {
                //Calculate and store the priceVolume for the volume available at this price level, decrease the remainingTrade size by the volume we got and move to the next highest bid
                priceVolume.push(parseFloat(obBid[highestBidIndex].price) * parseFloat(obBid[highestBidIndex].remaining));
                remainingTradeSize -= parseFloat(obBid[highestBidIndex].remaining);
                highestBidIndex--;
            }
        }
        bid = priceVolume.reduce(add, 0) / tradeSize;
        console.log("Weighted average bid for " + tradeSize + " ETH is " + bid);

        //work your way up the ask orderbook till you have a weighted average price for the whole tradeSize
        remainingTradeSize = tradeSize;  //reset so we can reuse this variable
        priceVolume = []; //reset so we can reuse this variable
        while (remainingTradeSize > 0){
            if(parseFloat(obAsk[lowestAskIndex].remaining) > remainingTradeSize) {
                //This is the last price level we need to calculate from
                priceVolume.push(parseFloat(obAsk[lowestAskIndex].price) * remainingTradeSize)
                remainingTradeSize -= remainingTradeSize;
            } else {
                //Calculate and store the priceVolume for the volume available at this price level, decrease the remainingTrade size by the volume we got and move to the next lowest ask
                priceVolume.push(parseFloat(obAsk[lowestAskIndex].price) * parseFloat(obAsk[lowestAskIndex].remaining));
                remainingTradeSize -= parseFloat(obAsk[lowestAskIndex].remaining);
                lowestAskIndex++;
            }
        }
        ask = priceVolume.reduce(add, 0) / tradeSize;
        console.log("Weighted average ask for " + tradeSize + " ETH is " + ask);        


        //Write to csv
        csvStream.write({
            unixTime: parseInt(currentTime),
            bid: bid,
            ask: ask,
            tradeSize: tradeSize
        });
        console.log("At time:" + parseInt(currentTime));
    }
    currentTime = file[i].time;
    //console.log("same second currentTime:" + currentTime + " nextTime:" + file[i].time);

    //if there is a trade there will be multiple events, we're only interested in the 'change' events as they change the orderbook
    file[i].data.events.forEach(function(event) {
        
        if(event.type === "change"){
            //check to see if the update is to an existing level and save the index of that level if it is
            let priceLevel = _.findIndex(ob, function(level) {
                return (level.side == event.side) && (level.price == event.price);
            });
            //if new, add that price level to the orderbook
            if(priceLevel == -1){
                ob.push({
                    "side": event.side,
                    "price": event.price,
                    "remaining": event.remaining
                });
                ob.sort(function(a,b) { return parseFloat(a.price) - parseFloat(b.price);}); //Q: Is there any way to abstract this?
                //console.log("Orderbook size: " + ob.length);
            } else {
                //update that pricelevel in the orderbook
                //Check to make sure we're updating a price level on the same side of the orderbook
                if(ob[priceLevel].side == event.side){
                    ob[priceLevel].remaining = event.remaining;
                } else {
                    //console.error("ERROR: Order book is messed up, you've got updates to the wrong side of the book at " + currentTime);
                }
                //Remove the priceLevel if remaining volume is 0
                if(ob[priceLevel].remaining == "0"){
                    ob.splice(priceLevel, 1);
                }
                //console.log("Orderbook size: " + ob.length);        
            }          
        }

    });



}
csvStream.end();