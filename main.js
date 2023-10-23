let schema = {
    sensorID: [],
    sensorName: [],
    eventSize: [],
    parseFormat: [],
    axisNames: [],
    scalingFactor: []
}

let header = []
let body = []
let test = 0

let sensorTime = []
let timeID = []
let sensorID = []
let sensorValue = []

const start_uint8 = 240

const csvmaker = function (data,schema,fileName) { 

	csvRows = []; 
	const headers = Object.keys(schema); 
	csvRows.push(headers); 

    data.forEach((row) =>{
        const rowData = headers.map((col) =>{
            const value = row[col];
            return String(value);
        })
        csvRows.push(rowData);
    })

    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' }); 
    const url = window.URL.createObjectURL(blob)  
    const a = document.createElement('a') 
    a.setAttribute('href', url) 
    a.setAttribute('download', fileName); 
    a.click() 
    URL.revokeObjectURL(url)
} 

previewFile()

function previewFile() {
    const content = document.querySelector(".content");
    const [file] = document.querySelector("input[type=file]").files;
    const reader = new FileReader();
  
    reader.addEventListener(
      "load",
      () => {
        // this will then display a text file

        // load the file as lines
        let fileContentArray = reader.result.split(/\r\n|\n/);

        //separate the header files from the body
        for(var line = 0; line < fileContentArray.length-1; line++){

            if ((/^(1.0|1.1)/).test(fileContentArray[line])) {
                if (fileContentArray[line] == "1.0") {
                    //variable_schema = 0
                    header.push(fileContentArray[line])
                    test = test + binaryData.byteLength
                } else if (fileContentArray[line] == "1.1") {
                    //variable_schema = 1
                    header.push(fileContentArray[line])
                    test = test + fileContentArray[line].length
                }
            }
            else if ((/^\d:/).test(fileContentArray[line]) || (/^\d\d:/).test(fileContentArray[line])) {
                header.push(fileContentArray[line])
                test = test + fileContentArray[line].length
            } else {
                body.push(fileContentArray[line])
            }
            
        }

        console.log(header)

        // get the schema from the header files 
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

        console.log(schema)

        content.innerText = reader.result;

        readAsBuffer()
      },
      false,
    );
  
    if (file) {
      reader.readAsText(file);
    }
}

function readAsBuffer(){
    const content = document.querySelector(".content");
    const [file] = document.querySelector("input[type=file]").files;
    const reader = new FileReader();
  
    reader.addEventListener(
      "load",
      () => {
        // this will then display a text file
        const buffer = reader.result;
        const view = new Uint8Array(buffer);

        content.innerText = view;

        // here we have some header part and rest is body
        let subUint8View = view.subarray(test) 

        let identifierIndex = subUint8View.indexOf(start_uint8) //first entry of the value f0 - using as an identifier

        let temp = subUint8View.subarray(identifierIndex)

        let line = []
        let tempLength = temp.length
        for (let i = 0; i < tempLength; i++) {
            line.push(temp[i])
        }

        content.innerText = line;

        parseFile(line)


      },
      false,
    );
  
    if (file) {
      reader.readAsArrayBuffer(file)
    }
}

function parseFile(line){
    let formatCheck = line.includes(start_uint8)

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

        console.log(sensorID)
        console.log(sensorValue)
        convertToParquet(sensorID, sensorValue)
}

async function convertToParquet(arr1, arr2){

    const content = document.querySelector(".content");
    content.innerHTML = arr2


    let csvSchema = {}
    schema.sensorName.forEach((id) => {
        let axisList = String(schema.axisNames.at(schema.sensorName.indexOf(id))).split(",")
        axisList.forEach((element) => {
            csvSchema[id + "-" + element] = String(schema.parseFormat.at(schema.sensorName.indexOf(id)))
        })
    }) 

    let csvData = []
    let ArrLength = arr2.length
    console.log(ArrLength)
    let schemaLength = Object.entries(csvSchema).length
    console.log(schemaLength)
    for (let i = 0; i < ArrLength; i = i + schemaLength) {
        let k = i
        let temp = {} // arr2
        for (const key in csvSchema){
            temp[key] = arr2[k]
            k++
        }
        csvData.push(temp)
    }
    console.log(csvSchema)
    
    console.log(csvData)
    csvmaker(csvData,csvSchema,"download.csv")

    // let sensorIDArr = []
    // let sensorValueArr = []

    // for (let i = 0; i < arr1.length; i++) {
    //     let x = arr1.shift()
    //     //console.log("Value : " + x)
    //     sensorIDArr.push(x)
    //     let id = schema.sensorID.indexOf(String(sensorIDArr.at(-1)))
    //     //console.log(id)
    //     let axis = schema.axisNames[String(id)].split(",")  
    //     let subArr = []
    //     for (let j = 0; j < parseInt(axis.length); j++) { 
    //         subArr.push(arr2.shift())
    //     }
    //     sensorValueArr.push(subArr)

    // }
    // //console.log(sensorIDArr)
    // console.log(sensorValueArr)

    // let testArr = []
    // while(sensorValueArr.length >0){
    //     let testArrSub = []
    //     schema.sensorID.forEach( (i) => {
    //         //console.log(i)
    //         let id = sensorIDArr.indexOf(parseInt(i))
    //         let length = String(schema.axisNames.at(schema.sensorID.indexOf(id))).split(",").length

    //         if(id == -1){
    //             let myArray = new Array(length).fill(0);
    //             console.log(length)
    //             testArrSub.push(myArray) 
    //             //testArrSub.push([0,0]) //need to change - this is hard coded
    //         } else {
    //             sensorIDArr.splice(id,1)
    //             testArrSub.push(sensorValueArr.splice(id,1)[0])  
    //         }
    //     })
    //     testArr.push(testArrSub)
    // }
    // console.log(testArr)
    
    // let schemaObject = {}
    // schema.sensorName.forEach((id) => {
    //     schemaObject[id] = {}
    //     schemaObject[id]["optional"] = true
    //     schemaObject[id]["fields"] = {}
    //     let axis = String(schema.axisNames.at(schema.sensorName.indexOf(id))).split(",")
    //     axis.forEach((axis) => {
    //         schemaObject[id]["fields"][axis.trim().toUpperCase()] = {}
    //         schemaObject[id]["fields"][axis.trim().toUpperCase()]["type"] = String(schema.parseFormat.at(schema.sensorName.indexOf(id)))
    //         schemaObject[id]["fields"][axis.trim().toUpperCase()]["optional"] = true
    //     })
    // }) 


    // let parquetSchema = new parquetjs.ParquetSchema(schemaObject)
    // console.log(parquetSchema)

    // let writer = await parquetjs.ParquetWriter.openFile(parquetSchema, outputPath)
    
    // let test1 = {}
    // let testArrLength = testArr.length
    // for (let i = 0; i < testArrLength; i = i + 1) {
    //     schema.sensorName.forEach((id) => {
    //         let axisList = String(schema.axisNames.at(schema.sensorName.indexOf(id))).split(",")
    //         let new1 = {}
    //         test1[id] = [] 
    //         axisList.forEach((element) => {
    //             let a = schema.sensorName.indexOf(id)
    //             let b = axisList.indexOf(element)
    //             new1[element] = testArr[i][parseInt(a)][parseInt(b)]
    //         })
    //         test1[id] = [new1] 
    //     }) 
    //     await writer.appendRow(test1)
    // } 

    // await writer.close()

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





