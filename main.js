const fs = require('fs');
const readLine = require('readline')
var parquet = require('parquetjs');
const { Int } = require('apache-arrow');

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
    sensorID : [],
    sensorName : [],
    eventSize : [],
    parseFormat : [],
    axisNames : [],
    scalingFactor : []
}

var parquetSchema = new parquet.ParquetSchema({
    ID: { type: 'UINT_8'},
    /*Name: {typer: 'UTF-8'},
    Size: {type: 'UTF-8'},
    Format: {type: 'UTF-8'},
    Axis: {type: 'UTF-8'},
    Scaling: { type: 'FLOAT'},*/
    Values: { type: 'UINT_8', repeated: true }
})

rl.on('line', (line) => {
    const binaryData = Buffer.from(line)

    if((/^(1.0|1.1)/).test(line)){
        if(line == "1.0"){
            variable_schema = 0
            header.push(line)
            test = test + binaryData.byteLength
        } else if (line == "1.1") {
            variable_schema = 1
            header.push(line)
            test = test + binaryData.byteLength
        }
    }
    else if((/^\d:/).test(line) || (/^\d\d:/).test(line)) {
        console.log("this is in udf format")
        const split = line.split(":")
        schema.sensorID.push(split[0]) 
        schema.sensorName.push(split[1]) 
        schema.eventSize.push(split[2]) 
        schema.parseFormat.push(dataTypeEquivalent(split[3])) 
        schema.axisNames.push(split[4])
        schema.scalingFactor.push(split[5]) 
        header.push(line)
        test = test + binaryData.byteLength
        udf1 ++
    } else {
        console.log((test + header.length))
    }          
})

rl.on('close', () => {
    readFullFile()
    console.log("File reading complete")
})

function readFullFile(){
    // Replace 'your-binary-file.bin' with the path to your binary file
    const filePath = 'example.udf';

    // Use fs.readFile to read the binary file
    fs.readFile(filePath, (err, data) => {
        if (err) {
            console.error('Error reading file:', err);
            return;
        }
        console.log(data)

        const binaryData = Buffer.from(data)
        buffer = new ArrayBuffer(binaryData.byteLength)
        let uint8View = new Uint8Array(buffer)
        let line = []
        for (let i = 0; i < binaryData.byteLength; i++) {
            uint8View[i] = binaryData[i]
            line.push(uint8View[i])
        }
        //console.log(uint8View)

        let arr = []
        for (let i = 144; i < line.length; i++) {
            arr.push(line[i])
        }
        console.log(arr)
        console.log("ARR is :" + arr.indexOf(240))

        let newArr = []
        for (let i = 17; i < arr.length; i++) {
            newArr.push(arr[i])
        }
        console.log(newArr)
        console.log("ARR is :" + newArr.indexOf(240))

        let counter = 0

        while(newArr.length > 0){
            if (counter == 0 || counter%(header.length+1) == 0){
                timeID.push(newArr.shift())
                for (let j = 0; j < 8; j++) {
                    timeStamp.push(newArr.shift())
                }
                counter++
            } else {
                sensorID.push(newArr.shift())
                let id = schema.sensorID.indexOf(String(sensorID.at(-1)))
                let valueSize = schema.eventSize[String(id)]
                for (let j = 0; j < valueSize; j++) {
                    sensorValue.push(newArr.shift())
                }
                counter++
            }    
        }

        console.log(timeID.length)
        console.log(timeStamp.length)
        console.log(sensorID)
        console.log(sensorValue)
        convertToParquet(sensorID, sensorValue)
    });
   
}

function convertToParquet(arr1,arr2) {
    let sensorIDArr = []
    let sensorValueArr = []
    let length = (arr1.length)
    for (let i = 0; i < length; i++) {
        let x = arr1.shift()
        sensorIDArr.push(x)
        let id = schema.sensorID.indexOf(String(sensorIDArr.at(-1)))
        let valueSize = schema.eventSize[String(id)]
        let subArr = []
        for (let j = 0; j < parseInt(valueSize); j++) {
            subArr.push(arr2.shift())
        }
        sensorValueArr.push(subArr)
    }
    console.log(sensorIDArr.length)
    console.log(sensorValueArr)
    try {
        toParquetFile(sensorIDArr, sensorValueArr)
    } catch (error) {
        console.error(error);
    }
}

async function toParquetFile(dataID, dataValue) {
    // create new ParquetWriter that writes to 'fruits.parquet`
    var writer = await parquet.ParquetWriter.openFile(parquetSchema, 'main.parquet');
    for (let i = 0; i < dataID.length; i++) {
        if (dataID[i] == 1) {
            await writer.appendRow({
                ID: dataID[i],
                Values: [dataValue[i][0], dataValue[i][1]]
            })
        } else if (dataID[i] == 2) {
            await writer.appendRow({
                ID: dataID[i],
                Values: [dataValue[i][0], dataValue[i][1], dataValue[i][2], dataValue[i][3]]
            })
        } else if (dataID[i] == 3) {
            await writer.appendRow({
                ID: dataID[i],
                Values: [dataValue[i][0], dataValue[i][1], dataValue[i][2], dataValue[i][3], dataValue[i][4], dataValue[i][5], dataValue[i][6], dataValue[i][7]]
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
                newData.push("Int8")
                break
            case "u8":
                newData.push("Uint8")
                break
            case "s16":
                newData.push("Int16")
                break
            case "u16":
                newData.push("Uint16")
                break
            case "s32":
                newData.push("Int32")
                break
            case "u32":
                newData.push("Uint32")
                break
            case "s64":
                newData.push("Int64")
                break
            case "u64":
                newData.push("Uint64")
                break
            case "f":
                newData.push("Float32")
                break
            case "d":
                newData.push("Float64")
                break
            case "s":
                newData.push("String")
                break
            default:
                console.log("Error : This is not a accepted Parse Format")
        }
    })
    data = ""
    data = newData.join(",")
    return data
}
    
