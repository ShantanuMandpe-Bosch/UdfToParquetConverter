const fs = require('fs');
const readLine = require('readline')

const stream = fs.createReadStream('example.udf')

const rl = readLine.createInterface({
    input: stream,
    crlfDelay: Infinity
})

// https://arrow.apache.org/docs/js/index.html
let udf1 = 0

let timeStamp = []
let timeID = []
let sensorID = []
let sensorValue = []
let data = []
let header = []
let body = []

let schema = {
    sensorID : [],
    sensorName : [],
    eventSize : [],
    parseFormat : [],
    axisNames : [],
    scalingFactor : []
}

let variable_schema

rl.on('line', (line) => {
    const binaryData = Buffer.from(line)

    if((/^(1.0|1.1)/).test(line)){
        if(line == "1.0"){
            variable_schema = 0
            header.push(line)
        } else if (line == "1.1") {
            variable_schema = 1
            header.push(line)
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
        udf1 ++
    } else {
        buffer = new ArrayBuffer(binaryData.byteLength)
        uint8View = new Uint8Array(buffer)
        for (let i = 0; i < binaryData.byteLength; i++) {
            uint8View[i] = binaryData.toString().charCodeAt(i)     
            data.push(uint8View[i])    
        }
        let i=0
        let n=0
        let endPoint = 0
        console.log(binaryData.byteLength)
        //while (i< binaryData.byteLength) {
        for (let i = 0; i < binaryData.byteLength; i++) {
            sensorID.push(data[i])
            let id = schema.sensorID.indexOf(data[i].toString())
            endPoint = schema.eventSize[id.toString()]
            console.log("reached1")
            //addValues(data,i,endPoint)
            for (let k = i+1;k<=endPoint;k++){
                sensorValue.push(data[k])
                console.log("reached")
            }
            i = endPoint + 1
            console.log("reached2")
        }
        //console.log(sensorID)
        //console.log(sensorValue)
    }  
        
})

rl.on('close', () => {
    console.log(sensorID)
    console.log(sensorValue)
    console.log("File reading complete")
})
    
/*

}*/        

function dataTypeEquivalent(data){
    let newData = []
    data.split(",").forEach((item) =>{
        switch(item){
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