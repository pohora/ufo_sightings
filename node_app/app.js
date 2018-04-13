const fs =require('fs')
const parse=require('csv-parse')

// Express is a node web framework.It is not needed for parsing csv or publishing kafka messages
const express=require('express');
const app=express();


//kafka configuration
var kafka=require('kafka-node')
var Producer=kafka.Producer;
var KeyedMessage =kafka.KeyedMessage;
var Client = kafka.Client;
// Make sure to point the kafka client to correct Kafka broker.
var client =new Client('192.168.56.101:2181')

var topic = 'csv_test_input'
producerReady=false;

// Any records that are unable to be seent, we store in this array, for later processing.
var unsentData=[]

var producer=new Producer(client,{requireAcks:1});

//Location of the csv, that needs to be parsed.
const inputFile='/Users/pohora/tmp/node/ufo_sightings/ufo_sightings_10k.csv';


var sightingsArray;

var count=0


// When the client has connect, the producer is ready to send messages.
producer.on('ready',function(){

    console.log('>>> Producer is ready')
    producerReady=true;
    //var message='{"name":"homer"}'
    const parser=parse({delimiter:','},function(err,data){

        sightingsArray=data;
   
    
        console.log("Array length : "+sightingsArray.length);
         var start_time =new Date()

         let count=0
         // Look thru all the records
         // Build json object
         // Send json object
        for(var i=0;i<sightingsArray.length;i++){
    
            var item=createJSONData(sightingsArray[i]);
            
            produceMessage(item)
            count=count+1
            console.log(">>> COUNT "+count)
        }
    
        var end_time=new Date()
        console.log("Start time was "+start_time)
        console.log("End time was "+end_time)
        console.log("Time taken: "+((end_time.getTime()-start_time.getTime())/1000))
        console.log("Time now "+new Date())
       
    })
    // read the inputFile, feed the contents to the parser
    fs.createReadStream(inputFile).pipe(parser);
    

    

})



producer.on('error',function(err){
    console.error('Problem with producing data '+err);
})


// Transform individual csv record to json object

function createJSONData(line){

var _date1_asDate=new Date(line[0])
console.log(_date1_asDate.toUTCString())
var ts = Math.round((_date1_asDate).getTime() / 1000);

//console.log(">>> "+_date1_asDate.toUTCString())
    var data={"datetime_utc":_date1_asDate.toUTCString(),
                "timestamp":parseInt(ts),
               "city":line[1],
               "state":line[2],
               "country":line[3],
               "shape":line[4],
               "duration":line[5],
               "location":line[9]+","+line[10],
               "comments":line[7],
               "id":parseInt(line[11])    

            }

            console.log(JSON.stringify(data));
            return data;


}

function produceMessage(item){
    KeyedMessage = kafka.KeyedMessage,message = new KeyedMessage("ufo", JSON.stringify(item)),
    payloads = [
        { topic: topic, messages: message, partition: 0 },
    ];
    //console.log("Sending msg: "+message);

    if (producerReady) {
        producer.send(payloads, function (err, data) {
            console.log(data);
        });
        } else {
            // the exception handling can be improved, for example schedule this message to be tried again later on
            console.error("Sorry, Producer is not ready yet, failed to produce message to Kafka.");
            unsentData.add(payloads);
        }
}


const port =8585

app.get('/',function(req,res){
    res.send('UFO Sighting app');


})


app.listen(port,function(){
    console.log('App listening on port '+port);
})

