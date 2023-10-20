const express = require('express');
const fileUpload = require('express-fileupload');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const readLine = require('readline')

const app = express();
const port = process.env.PORT || 3000;

app.use(fileUpload());

app.use(express.static(__dirname));

// function getSchemas(schema, schemaObject, header) {
//     console.log(header)
//     header.forEach((line) => {
//         const split = line.split(":")

//         // add elements to the main schema
//         schema.sensorID.push(split[0])
//         schema.sensorName.push(String(split[1]).trim())
//         schema.eventSize.push(split[2])
//         //schema.parseFormat.push(String(dataTypeEquivalent(split[3])).split(",").at(0))
//         schema.parseFormat.push(((split[3])).split(",").at(0))
//         schema.axisNames.push(String(split[4]).trim().toUpperCase())
//         schema.scalingFactor.push(split[5])
//     })

//     // add elements to the parquet object schema
//     schema.sensorName.forEach((id) => {
//         schemaObject[id] = {}
//         schemaObject[id]["optional"] = true
//         schemaObject[id]["fields"] = {}
//         let axis = String(schema.axisNames.at(schema.sensorName.indexOf(id))).split(",")
//         axis.forEach((axis) => {
//             schemaObject[id]["fields"][axis.trim().toUpperCase()] = {}
//             schemaObject[id]["fields"][axis.trim().toUpperCase()]["type"] = String(schema.parseFormat.at(schema.sensorName.indexOf(id)))
//             schemaObject[id]["fields"][axis.trim().toUpperCase()]["optional"] = true
//         })
//     })

//     console.log(schema)
//     return schema
// }

app.post('/modify', (req, res) => {
    if (!req.files || Object.keys(req.files).length === 0) {
        return res.status(400).send('No files were uploaded.');
    }

    const uploadedFile = req.files.file;
    const newFilename = uuidv4() + path.extname(uploadedFile.name);
    const uploadPath = path.join(__dirname, 'uploads', newFilename);
    const modifiedPath = path.join(__dirname, 'modified', newFilename);

    uploadedFile.mv(uploadPath, (err) => {
        if (err) {
            return res.status(500).send(err);
        }

        //declare variables
        let test = 0
        let newData1 = ""
        let newData  = ""

        let schema = {
            sensorID: [],
            sensorName: [],
            eventSize: [],
            parseFormat: [],
            axisNames: [],
            scalingFactor: []
        }
        let schemaObject = {}
        let header = []

        const stream = fs.createReadStream(uploadPath)
        const rl = readLine.createInterface({
            input: stream,
            crlfDelay: Infinity
        })

        console.log("Reading file lines")


        rl.on('line', (line) => {
            const binaryData = Buffer.from(line)
            console.log(line)

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
                //udf1++
            }
        })

        rl.on('close', () => {
            //schema = getSchemas(schema,schemaObject,header)
            console.log(schema)
            console.log("File reading complete")
        })   

        // Here, you can append data to the file using fs
        const data = "new Uint8Array(Buffer.from(schema.axisNames));"

        fs.writeFile(uploadPath, data, (err) => {
            if (err) {
                return res.status(500).send(err);
            }

            // res.download(modifiedPath, uploadedFile.name, (err) => {
            //     if (err) {
            //         return res.status(500).send(err);
            //     }
            // });

            fs.rename(uploadPath, modifiedPath, (err) => {
                if (err) {
                    return res.status(500).send(err);
                }
                res.download(modifiedPath, uploadedFile.name, (err) => {
                    if (err) {
                        return res.status(500).send(err);
                    }
                });
            });
        });
    });
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});