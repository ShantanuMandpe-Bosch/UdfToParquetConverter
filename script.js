const fs = require('fs');
const readLine = require('readline')
var parquet = require('parquetjs');

const filename = 'example.udf'

const stream = fs.createReadStream(filename)
const rl = readLine.createInterface({
    input: stream,
    crlfDelay: Infinity
})

let schema = {
    sensorID: [],
    sensorName: [],
    eventSize: [],
    parseFormat: [],
    axisNames: [],
    scalingFactor: []
}

let udf1 = 0
let test = 0

let sensorTime = []
let timeID = []
let sensorID = []
let sensorValue = []
let header = []

const start_uint8 = 240

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
        header.push(line)
        test = test + binaryData.byteLength
        udf1++
    } else {
        //console.log((test + header.length))
    }
})

rl.on('close', () => {
    getSchemas()
    console.log(schema)
    readFullFile()
    console.log("File reading complete")
})

function getSchemas(){
    header.forEach((line) => {
        const split = line.split(":")

        // add elements to the schema
        schema.sensorID.push(split[0])
        schema.sensorName.push(split[1])
        schema.eventSize.push(split[2])
        schema.parseFormat.push(dataTypeEquivalent(split[3]))
        schema.axisNames.push(split[4])
        schema.scalingFactor.push(split[5])
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

        for (let i = 0; i < binaryData.byteLength; i++) {
            uint8View[i] = binaryData[i]
        }

        // here we have some header part and rest is body
        let subUint8View = uint8View.subarray(test) 

        let identifierIndex = subUint8View.indexOf(start_uint8) //first entry of the value f0 - using as an identifier

        let temp = subUint8View.subarray(identifierIndex)


        let line = []
        for (let i = 0; i < temp.length; i++) {
            line.push(temp[i])
        }

        console.log(typeof schema.eventSize[0])

        let counter = 0
        //assuming its timestamp id and start id are the same ids 
        while (line.length > 0) {
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
                //console.log("Parse : " + parse + " valueSize : " + subArr.length)

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
            }
        }
        //console.log(sensorID)
        //console.log(sensorValue)
        convertToParquet(sensorID, sensorValue)
    })
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
    // console.log(sensorIDArr)
    // console.log(sensorValueArr)

    let testArr = []
    while(sensorValueArr.length >0){
        let testArrSub = []
        schema.sensorID.forEach( (i) => {
            //console.log(i)
            let id = sensorIDArr.indexOf(parseInt(i))

            if(id == -1){
                testArrSub.push([0,0]) //need to change - this is hard coded
            } else {
                sensorIDArr.splice(id,1)
                testArrSub.push(sensorValueArr.splice(id,1)[0])  
            }
        })
        testArr.push(testArrSub)
    }
    //console.log(testArr)

    let parquetSchema = new parquet.ParquetSchema({
        "Sensor1" : {
            optional : true,
            fields: {
                A : {type : 'INT_8', optional: true },
                B : {type : 'INT_8', optional: true }
            }
        },
        "Sensor2" : {
            optional : true,
            fields: {
                A : {type : 'INT_16', optional: true },
                B : {type : 'INT_16', optional: true }
            }
        },
        "Sensor3" : {
            optional : true,
            fields: {
                A : {type : 'FLOAT', optional: true },
                B : {type : 'FLOAT', optional: true }
            }
        }
    })


    let test1 = {}
    test1["1"] = {}
    console.log(test1) 
    test1["1"]["optional"] = true
    console.log(test1) 
    test1["1"]["fields"] = {}
    console.log(test1) 
    test1["1"]["fields"]["A"] = {}
    console.log(test1) 
    test1["1"]["fields"]["A"]["type"] = "INT_8"
    test1["1"]["fields"]["A"]["optional"] = true
    console.log(test1) 


    let writer = await parquet.ParquetWriter.openFile(parquetSchema, 'main.parquet')

     for (let i = 0; i < testArr.length; i = i + 1) {
        await writer.appendRow({
            "Sensor1" : [
                {
                A : (testArr[i][0][0]),
                B : (testArr[i][0][1])}
            ],
            "Sensor2" : [
                {
                A : (testArr[i][1][0]),
                B : (testArr[i][1][1])}
            ],
            "Sensor3" : [
                {
                A : (testArr[i][2][0]),
                B : (testArr[i][2][1])}
            ] 
        })
    } 

    await writer.close()
}

function getParquetSchema(){

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