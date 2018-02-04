var aws = require('aws-sdk');
const sql = require("mysql");
const bucket="jabil-aws-assets";
const Key = "iotdata-mapper.json";
var userName = 'Jabil';
var passWord = 'jabilaws';


function readconfigdata(params, cb){
    console.log("reading s3 object");
     const s3 = new aws.S3({ region: 'us-east-1' });    
    
    s3.getObject(params, (err, configdata) => {
        if (err) {
            
            const message = `Error getting object ${Key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
            console.log(message);
            cb(err,null);
        } else {
            // Split and inject data into SQL Server
            console.log("Success reading config mapper data")
            let mapperdata = JSON.parse(configdata.Body.toString());
            cb(null,mapperdata);
        }
    });
    
}


function getObjectdata(o,s){
    s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
    s = s.replace(/^\./, '');           // strip a leading dot
    var a = s.split('.');
    for (var i = 0, n = a.length; i < n; ++i) {
        var k = a[i];
        if (k in o) {
            o = o[k];
        } else {
            return;
        }
    }
    return o;
}

function executeSQLInsert(mapperdata,iotData,cb){
    
    const connection = sql.createConnection({
      host     : '10.200.224.36',
      port     : '3306',
      user     : userName,
      password : passWord,
      database : 'jabildb'
    });
    
            
    
           var insQueryMySQLColumnstr="";
            var insQueryIoTJsonData="";
            for(var key in mapperdata)
            {
                 insQueryMySQLColumnstr = insQueryMySQLColumnstr + "`"+key+"`,";
                 insQueryIoTJsonData = insQueryIoTJsonData+"'"+getObjectdata(iotData,mapperdata[key].toString())+"',";
            }
            
           insQueryMySQLColumnstr = insQueryMySQLColumnstr.substring(0,insQueryMySQLColumnstr.length-1);
           insQueryIoTJsonData = insQueryIoTJsonData.substring(0,insQueryIoTJsonData.length-1);
           
          var insQueryPrefix =  "Insert Into FACT_Device_Telemetry (";
          var insQueryMid = ") Values (";
          var insQueryEnd = ")";
           
          var strFullQuery = insQueryPrefix+insQueryMySQLColumnstr+insQueryMid+insQueryIoTJsonData+insQueryEnd;
         console.log(strFullQuery);
         connection.query(strFullQuery, function (error, results, fields) {
            if (error) throw error;
            else
            console.log('Records successfully Inserted');
            // console.log('The solution is: ', results[0].solution);
            connection.end();
               
          });
         cb(null,"Success");
}


function processData(o,mapperdata,cb){

    let message = o.Records[0].Sns.Message;
    message = JSON.parse(message);
    
    const s3 = new aws.S3({ region: 'us-east-1' });    
    const params = {
        Bucket: message.Records[0].s3.bucket.name,
        Key: decodeURIComponent(message.Records[0].s3.object.key)
    };
    
    s3.getObject(params, (err, data) => {
      
        if(err){
            console.log("Error reading io-data.json file");
            cb(err,null);
        }
        else
        {
            console.log("Success reading iot-data.json file");
           var iotData = JSON.parse(data.Body.toString());
            
           for (var i=0;i<iotData.payload.rows.length;i++)
           {
                var row = iotData.payload.rows[i];
                console.log(row.doc.deviceId);
                executeSQLInsert(mapperdata,row,(err,data)=>{
                    if(err)
                    {
                        console.log("Error in executeSQLInsert");
                        cb(err,null);
                    }
                    else
                    {
                        console.log("executeSQLInsert processed successfully");
                        cb(null,data);
                    }
                });

           } 
          
           
            
        }
        
    });
             
}



exports.handler = (event, context, callback) => {
    const params = {
        Bucket: bucket,
        Key: Key
    };
    readconfigdata(params, function(err,mapperdata){
        if(err)
        {
            console.log(err)
        }
        else
        {
           
        processData(event,mapperdata,(err,data)=>{
            if(err)
            {
                console.log("Error inserting SQL data",err)
            }
            
        })
   
        }
        
    });
     

    callback(null, 'Hello from Lambda');
};