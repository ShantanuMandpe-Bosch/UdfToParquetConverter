const fs = require('fs');
const readLine = require('readline')
var parquet = require('parquetjs');

const stream = fs.createReadStream('example.udf')
const rl = readLine.createInterface({
    input: stream,
    crlfDelay: Infinity
})

// https://arrow.apache.org/docs/js/index.html
let udf1 = 0
let test = 0

let timeStamp = []
let timeID = []
let sensorID = []
let sensorValue = []
let header = []


let schema = {
    sensorID: [],
    sensorName: [],
    eventSize: [],
    parseFormat: [],
    axisNames: [],
    scalingFactor: []
}

// var parquetSchema1 = new parquet.ParquetSchema({
//     ID: { type: 'UINT_8' },
//     Values: { type: 'INT_8', repeated: true }
// })

let schemaObject = {} //object for the schema

// let parquetSchema = new parquet.ParquetSchema(testObj);

rl.on('line', (line) => {
    const binaryData = Buffer.from(line)

    if ((/^(1.0|1.1)/).test(line)) {
        if (line == "1.0") {
            variable_schema = 0
            header.push(line)
            test = test + binaryData.byteLength
        } else if (line == "1.1") {
            variable_schema = 1
            header.push(line)
            test = test + binaryData.byteLength
        }
    }
    else if ((/^\d:/).test(line) || (/^\d\d:/).test(line)) {
        const split = line.split(":")
        // add elements to the schema
        schema.sensorID.push(split[0])
        schema.sensorName.push(split[1])
        schema.eventSize.push(split[2])
        schema.parseFormat.push(dataTypeEquivalent(split[3]))
        schema.axisNames.push(split[4])
        schema.scalingFactor.push(split[5])

        //add elements to the parquet-schema
        header.push(line)
        test = test + binaryData.byteLength
        udf1++
    } else {
        //console.log((test + header.length))
    }
})

rl.on('close', () => {
    readFullFile()
    console.log("File reading complete")
})

function readFullFile() {
    const filePath = 'example.udf';
    fs.readFile(filePath, (err, data) => {
        if (err) {
            console.error('Error reading file:', err);
            return;
        }
        
        const binaryData = Buffer.from(data)
        buffer = new ArrayBuffer(binaryData.byteLength)
        let uint8View = new Uint8Array(buffer)
        let line = []
        for (let i = 0; i < binaryData.byteLength; i++) {
            uint8View[i] = binaryData[i]
            line.push(uint8View[i])
        }

        let arr = []
        for (let i = 144; i < line.length; i++) {
            arr.push(line[i])
            //arrData.push(data[i])
        }

        //console.log("Arr " + arr)
        
        let newArr = []
        for (let i = 17; i < arr.length; i++) {
            newArr.push(arr[i])
        }

        let counter = 0

        while (newArr.length > 0) {
            if (counter == 0 || counter % (header.length + 1) == 0) {
                timeID.push(newArr.shift())
                for (let j = 0; j < 8; j++) {
                    timeStamp.push(newArr.shift())
                }
                counter++
            } else {
                sensorID.push(newArr.shift())
                let id = schema.sensorID.indexOf(String(sensorID.at(-1)))
                let valueSize = schema.eventSize[String(id)]
                let parse = schema.parseFormat[String(id)]
                let subArr = []
                for (let j = 0; j < valueSize; j++) {
                    subArr.push(newArr.shift())
                }
                ////////
                if(parse == 'INT_8'){
                    let uint8Array = new Uint8Array(subArr);
                    let int8Array = new Int8Array(uint8Array.buffer);
                    for(let i=0; i< int8Array.length;i++){
                        sensorValue.push(int8Array[i])
                    }
                    //console.log(int8Array)
                } else if (parse == 'INT_16'){
                    let uint8Array = new Uint8Array(subArr);
                    let int16Array = new Int16Array(uint8Array.buffer);
                    for (let i = 0; i < int16Array.length; i++) {
                        sensorValue.push(int16Array[i])
                    }
                    //console.log(int16Array)
                } else {
                    let uint8Array = new Uint8Array(subArr);
                    let float32Array = new Float32Array(uint8Array.buffer);
                    for (let i = 0; i < float32Array.length; i++) {
                        sensorValue.push(float32Array[i])
                    }

                }               
                ///////
                counter++
            }
        }
        convertToParquet(sensorID, sensorValue)
    });

}

function convertToParquet(arr1, arr2) {
    let sensorIDArr = []
    let sensorValueArr = []
    let length = (arr1.length)
    for (let i = 0; i < length; i++) {
        let x = arr1.shift()
        sensorIDArr.push(x)
        let id = schema.sensorID.indexOf(String(sensorIDArr.at(-1)))
        let axis = schema.axisNames[String(id)].split(",")
        let subArr = []
        for (let j = 0; j < parseInt(axis.length); j++) { //this is hardcoded 
            subArr.push(arr2.shift())
        }
        sensorValueArr.push(subArr)
    }
    //console.log(sensorIDArr.length)
    
    try {
        //console.log(sensorValueArr)
        toParquetFile(sensorIDArr, sensorValueArr)
    } catch (error) {
        console.error(error);
    }
}

async function toParquetFile(dataID, dataValue) {

    console.log(schemaObject)

    // let parquetSchema = new parquet.ParquetSchema({schemaObject})
    let parquetSchema = new parquet.ParquetSchema({
        Name: { type: 'UTF8' },
        A: { type: 'UTF8'},
        B: { type: 'UTF8'},
    })
    
    let writer = await parquet.ParquetWriter.openFile(parquetSchema, 'main.parquet')
    for (let i = 0; i < dataID.length; i++) {
        if (dataID[i] == 1) {
            await writer.appendRow({
                Name: String(schema.sensorName[0]),
                A: String(dataValue[i][0]),
                B: String(dataValue[i][1]),
            })
        } else if (dataID[i] == 2) {
            await writer.appendRow({
                Name: String(schema.sensorName[1]),
                A: String(dataValue[i][0]),
                B: String(dataValue[i][1]),
            })

        } else if (dataID[i] == 3) {           
            await writer.appendRow({
                Name: String(schema.sensorName[2]),
                A: String(dataValue[i][0]),
                B: String(dataValue[i][1]),
            })        
        }
    }
    console.log(schema)

    await writer.close()
}

function dataTypeEquivalent(data) {
    let newData = []
    data.split(",").forEach((item) => {
        switch (item) {
            case ("s8"):
                newData.push("INT_8")
                break
            case "u8":
                newData.push("UINT_8")
                break
            case "s16":
                newData.push("INT_16")
                break
            case "u16":
                newData.push("UINT_16")
                break
            case "s32":
                newData.push("INT_32")
                break
            case "u32":
                newData.push("UINT_32")
                break
            case "s64":
                newData.push("INT_64")
                break
            case "u64":
                newData.push("UINT_64")
                break
            case "f":
                newData.push("FLOAT")
                break
            case "d":
                newData.push("FLOAT")
                break
            case "s":
                newData.push("UTF8")
                break
            default:
                console.log("Error : This is not a accepted Parse Format")
        }
    })
    data = ""
    data = newData.join(",")
    return data
}

