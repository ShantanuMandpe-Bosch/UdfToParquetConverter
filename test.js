const fs = require('node:fs');
const readline = require('node:readline');
const { jspack } = require('./jspack');
const sizeof = require('object-sizeof');
const { readFile } = require('node:fs/promises');
const { Buffer, File } = require('node:buffer');

/*let results = [];
let header = [];

var filename = 'test.udf';

let check = 0;
let udf1 = 0;
let udf2 = 0;

async function processLineByLine() {
  const fileStream = fs.createReadStream(filename);

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if(line === "" ) {
        check = 1;
    }
    if(check===0){
        header.push(line);
    } else if(check===1){
        results.push(line);
    }
  }

  console.log('header: ' + header.length);
  console.log('results: ' + results.length);

  for (const head of header){
    if((/^(1.0|1.1)/).test(head)) {
        udf1 += 1;     
    }   
    else if((/^\d:/).test(head) || (/^\d\d:/).test(head)) {
        let score = head.match(/:/g);
        if (score.length == 5){
            udf2 += 1;
        }
    }
  }
  if(udf1 === 1 && udf2 === (header.length-1)){
    console.log("this is in udf format");
  } else {
    console.log("this is not in udf format");
  }

}
processLineByLine(); */

/*
the shit thats written is in:
    uint8_t uint64_t uint8_t uint8_t
*/



const buffer = new Buffer.alloc(10000);

Buffer.from(buffer)
  
console.log("Open existing file");
  fs.open('example.udf', 'r+', function (err, fd) {
      if (err) {
          return console.error(err);
      }
  
      console.log("Reading the file");
  
      fs.read(fd, buffer, 0, buffer.length,
          0, function (err, bytes) {

              if (err) {
                  console.log(err);
              }
              buffer8 = new ArrayBuffer(buffer.length)
              uint8View = new Uint8Array(buffer8)
              for (let i = 0; i < buffer.length; i++) {
                uint8View[i] = buffer.toString().charCodeAt(i)         
              }

              console.log(uint8View.slice(buffer.length-10,buffer.length))
                
              // Close the opened file.
              fs.close(fd, function (err) {
                  if (err) {
                      console.log(err);
                  }
  
                  console.log("File closed successfully");
              });
          });
  });
  




