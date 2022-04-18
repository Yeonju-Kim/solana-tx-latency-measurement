const web3 = require("@solana/web3.js");
const Base58 = require('base-58');
const parquet = require('parquetjs-lite');
const moment = require('moment');
const AWS = require('aws-sdk');
const fs = require('fs');

require('dotenv').config();
const keypair = web3.Keypair.fromSecretKey(Base58.decode(process.env.SIGNER_PRIVATE_KEY));
const connection = new web3.Connection(web3.clusterApiUrl('testnet'), 'confirmed'); //To use mainnet, use 'mainnet-beta'

async function makeParquetFile(data) {
  var schema = new parquet.ParquetSchema({
      executedAt:{type:'TIMESTAMP_MILLIS'},
      txhash:{type:'UTF8'},
      startTime:{type:'TIMESTAMP_MILLIS'},
      endTime:{type:'TIMESTAMP_MILLIS'},
      chainId:{type:'INT64'},
      latency:{type:'INT64'},
      error:{type:'UTF8'}
  })

  var d = new Date()
  //20220101_032921
  var datestring = moment().format('YYYYMMDD_HHmmss')

  var filename = `${datestring}.parquet`

  // create new ParquetWriter that writes to 'filename'
  var writer = await parquet.ParquetWriter.openFile(schema, filename);

  await writer.appendRow(data)

  writer.close()

  return filename;
}

async function uploadToS3(data){
  const s3 = new AWS.S3();
  const filename = await makeParquetFile(data)
  const param = {
    'Bucket':process.env.S3_BUCKET,
    'Key':filename,
    'Body':fs.createReadStream(filename),
    'ContentType':'application/octet-stream'
  }
  await s3.upload(param).promise()
  fs.unlinkSync(filename) 
}

async function sendZeroSol(){
  var data = {
    executedAt: new Date().getTime(),
    txhash: '', // Solana has no txHash. Instead, it uses tx signature. 
    startTime: 0,
    endTime: 0,
    chainId: 0, //Solana has no chainId. 
    latency:0,
    error:'',
  } 

  try{
    //check balance 
    const balance = await connection.getBalance(keypair.publicKey)
    if(balance*(10**(-9)) < parseFloat(process.env.BALANCE_ALERT_CONDITION_IN_SOL))
    { 
      console.log(`Current balance of ${address} is less than ${process.env.BALANCE_ALERT_CONDITION_IN_SOL} ! balance=${balance*(10**(-9))}`)
    }

    // Write starttime 
    const start = new Date().getTime()
    data.startTime = start

    const transaction = new web3.Transaction().add(
      web3.SystemProgram.transfer({
        fromPubkey: keypair.publicKey,
        toPubkey: keypair.publicKey, 
        lamports: 0, 
      }),
    );

    const signature = await web3.sendAndConfirmTransaction(
      connection, 
      transaction, 
      [keypair]
    );
    const end = new Date().getTime()
    data.endTime = end
    data.latency = end-start
    data.txhash = signature;
    console.log(`${data.executedAt},${data.chainId},${data.txhash},${data.startTime},${data.endTime},${data.latency},${data.error}`)

  } catch{
    console.log("failed to execute.", err.toString())
    data.error = err.toString()
    console.log(`${data.executedAt},${data.chainId},${data.txhash},${data.startTime},${data.endTime},${data.latency},${data.error}`)
  }

  try{
    await uploadToS3(data)
  } catch(err){
    console.log('failed to s3.upload', err.toString())
  }
}

async function main (){
  const start = new Date().getTime()
  console.log(`starting tx latency measurement... start time = ${start}`)

  // run sendTx every SEND_TX_INTERVAL(sec).
  const interval = eval(process.env.SEND_TX_INTERVAL)
      setInterval(()=>{
      sendZeroSol();
  }, interval)
}

main();