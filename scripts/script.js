const fs = require('fs');
const readLine = require('readline')
var parquet = require('parquetjs');

const filename = '../example.udf'
const outputPath = 'main.parquet'

let schema = {
    sensorID: [],
    sensorName: [],
    eventSize: [],
    parseFormat: [],
    axisNames: [],
    scalingFactor: []
}
let schemaObject = {}


let udf1 = 0
let test = 0

let sensorTime = []
let timeID = []
let sensorID = []
let sensorValue = []
let header = []

const start_uint8 = 240 // 


const stream = fs.createReadStream(filename)
const rl = readLine.createInterface({
    input: stream,
    crlfDelay: Infinity
})


rl.on('line', (line) => {
    const binaryData = Buffer.from(line)

    if ((/^(1.0|1.1)/).test(line)) {
        if (line == "1.0") {
            //variable_schema = 0
            header.push(line)
            test = test + binaryData.byteLength
        } else if (line == "1.1") {
            //variable_schema = 1
            header.push(line)
            test = test + binaryData.byteLength
        }
    }
    else if ((/^\d:/).test(line) || (/^\d\d:/).test(line)) {
        header.push(line)
        test = test + binaryData.byteLength
        udf1++
    }
})

rl.on('close', () => {
    getSchemas()
    //console.log(schema)
    //console.log(schemaObject) 

    readFullFile()
    console.log("File reading complete")
})

function getSchemas(){
    header.forEach((line) => {
        const split = line.split(":")

        // add elements to the main schema
        schema.sensorID.push(split[0])
        schema.sensorName.push(String(split[1]).trim())
        schema.eventSize.push(split[2])
        schema.parseFormat.push(String(dataTypeEquivalent(split[3])).split(",").at(0))
        schema.axisNames.push(String(split[4]).trim().toUpperCase())
        schema.scalingFactor.push(split[5])
    })

    // add elements to the parquet object schema
    schema.sensorName.forEach((id) => {
        schemaObject[id] = {}
        schemaObject[id]["optional"] = true
        schemaObject[id]["fields"] = {}
        let axis = String(schema.axisNames.at(schema.sensorName.indexOf(id))).split(",")
        axis.forEach((axis) => {
            schemaObject[id]["fields"][axis.trim().toUpperCase()] = {}
            schemaObject[id]["fields"][axis.trim().toUpperCase()]["type"] = String(schema.parseFormat.at(schema.sensorName.indexOf(id)))
            schemaObject[id]["fields"][axis.trim().toUpperCase()]["optional"] = true
        })
    }) 
}

function readFullFile(){
    fs.readFile(filename, (err,data) => {
        if(err){
            console.error("Error Reading File", err)
            return
        }
        const binaryData = Buffer.from(data)
        buffer = new ArrayBuffer(binaryData.byteLength)
        let uint8View = new Uint8Array(buffer)

        let binaryDataByteLength = binaryData.byteLength
        for (let i = 0; i < binaryDataByteLength; i++) {
            uint8View[i] = binaryData[i]
        }

        // here we have some header part and rest is body
        let subUint8View = uint8View.subarray(test) 

        let identifierIndex = subUint8View.indexOf(start_uint8) //first entry of the value f0 - using as an identifier

        let temp = subUint8View.subarray(identifierIndex)

        let line = []
        let tempLength = temp.length
        for (let i = 0; i < tempLength; i++) {
            line.push(temp[i])
        }

        // let searchArray = schema.sensorID
        // searchArray.push(start_uint8)
        // console.log(searchArray)

        // let indexArray = []
        // searchArray.forEach((i) => {
        //     indexArray.push(line.indexOf(parseInt(240)))
        // })
        // console.log(indexArray)
        // console.log(line)

        // console.log()
        formatCheck = line.includes(start_uint8)

        let counter = 0
        //assuming its timestamp id and start id are the same ids 
        while (line.length > 0 && formatCheck==true) {
            if(line[counter] == start_uint8){
                timeID.push(line.splice(counter,1))
                sensorTime.push(line.splice(counter,8))
            } else {
                sensorID.push(line.shift())
                //console.log("SensorId : " + sensorID)

                let id = schema.sensorID.indexOf(String(sensorID.at(-1)))
                //console.log("id : " + id)
                let valueSize = schema.eventSize[parseInt(id)]
                //console.log("valueSize : " + parseInt(valueSize))
                let parse = schema.parseFormat[String(id)]
                //console.log("parse : " + parse)

                let subArr = []
                subArr = line.splice(counter, valueSize)

                dataFormatting(parse,subArr)
            }
        }

        while (line.length > 0 && formatCheck==false) {
                sensorID.push(line.shift())
                //console.log("SensorId : " + sensorID)

                let id = schema.sensorID.indexOf(String(sensorID.at(-1)))
                //console.log("id : " + id)
                let valueSize = schema.eventSize[parseInt(id)]
                //console.log("valueSize : " + parseInt(valueSize))
                let parse = schema.parseFormat[String(id)]
                //console.log("parse : " + parse)

                let subArr = []
                subArr = line.splice(counter, valueSize)

                dataFormatting(parse,subArr)
        }
        
        //console.log(sensorID)
        //console.log(sensorValue)
        convertToParquet(sensorID, sensorValue)
    })
}

function dataFormatting(parseFormat, arr){
    if(parseFormat == 'INT_8'){
        let uint8Array = new Uint8Array(arr);
        let int8Array = new Int8Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...int8Array]
    } else if (parseFormat == 'INT_16'){
        let uint8Array = new Uint8Array(arr);
        let int16Array = new Int16Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...int16Array]
    } else if (parseFormat == 'UINT_8'){
        let uint8Array = new Uint8Array(arr);
        sensorValue = [...sensorValue,...uint8Array]
    } else if (parseFormat == 'UINT_16'){
        let uint8Array = new Uint8Array(arr);
        let uint16Array = new Uint16Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...uint16Array]
    } else if (parseFormat == 'INT_32'){
        let uint8Array = new Uint8Array(arr);
        let int32Array = new Int32Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...int32Array]
    } else if (parseFormat == 'UINT_32'){
        let uint8Array = new Uint8Array(arr);
        let uint32Array = new Uint32Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...uint32Array]
    } else if (parseFormat == 'INT_64'){
        let uint8Array = new Uint8Array(arr);
        let int64Array = new BigInt64Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...int64Array]
    } else if (parseFormat == 'UINT_64'){
        let uint8Array = new Uint8Array(arr);
        let uint64Array = new BigUint64Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...uint64Array]
    } else if (parseFormat == 'FLOAT'){
        let uint8Array = new Uint8Array(arr);
        let float32Array = new Float32Array(uint8Array.buffer);
        sensorValue = [...sensorValue,...float32Array]
    } else {
        console.error("This is an invalid sensor Data Format")
    }
}

async function convertToParquet(arr1, arr2){
    let sensorIDArr = []
    let sensorValueArr = []

    for (let i = 0; i < arr1.length; i++) {
        let x = arr1.shift()
        //console.log("Value : " + x)
        sensorIDArr.push(x)
        let id = schema.sensorID.indexOf(String(sensorIDArr.at(-1)))
        //console.log(id)
        let axis = schema.axisNames[String(id)].split(",")  
        let subArr = []
        for (let j = 0; j < parseInt(axis.length); j++) { 
            subArr.push(arr2.shift())
        }
        sensorValueArr.push(subArr)

    }
    //console.log(sensorIDArr)
    //console.log(sensorValueArr)

    let testArr = []
    while(sensorValueArr.length >0){
        let testArrSub = []
        schema.sensorID.forEach( (i) => {
            //console.log(i)
            let id = sensorIDArr.indexOf(parseInt(i))
            let length = String(schema.axisNames.at(schema.sensorID.indexOf(id))).split(",").length

            if(id == -1){
                let myArray = new Array(length).fill(0);
                console.log(length)
                testArrSub.push(myArray) 
                //testArrSub.push([0,0]) //need to change - this is hard coded
            } else {
                sensorIDArr.splice(id,1)
                testArrSub.push(sensorValueArr.splice(id,1)[0])  
            }
        })
        testArr.push(testArrSub)
    }
    //console.log(testArr)

    let parquetSchema = new parquet.ParquetSchema(schemaObject)

    let writer = await parquet.ParquetWriter.openFile(parquetSchema, outputPath)
    
    let test1 = {}
    let testArrLength = testArr.length
    for (let i = 0; i < testArrLength; i = i + 1) {
        schema.sensorName.forEach((id) => {
            let axisList = String(schema.axisNames.at(schema.sensorName.indexOf(id))).split(",")
            let new1 = {}
            test1[id] = [] 
            axisList.forEach((element) => {
                let a = schema.sensorName.indexOf(id)
                let b = axisList.indexOf(element)
                new1[element] = testArr[i][parseInt(a)][parseInt(b)]
            })
            test1[id] = [new1] 
        }) 
        await writer.appendRow(test1)
    } 

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
                break
        }
    })
    data = ""
    data = newData.join(",")
    return data
}