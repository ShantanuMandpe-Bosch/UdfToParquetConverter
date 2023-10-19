const express = require('express');
const fileUpload = require('express-fileupload');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const path = require('path');


let udf1 = 0
let test = 0
let header = []

const app = express();
const port = process.env.PORT || 3000;

app.use(fileUpload());

app.use(express.static(__dirname));

app.post('/modify', (req, res) => {
    if (!req.files || Object.keys(req.files).length === 0) {
        return res.status(400).send('No files were uploaded.');
    }

    const uploadedFile = req.files.file;
    const newFilename = uuidv4() + path.extname(uploadedFile.name);
    const uploadPath = path.join(__dirname, 'uploads', uploadedFile.name);
    const modifiedPath = path.join(__dirname, 'modified', newFilename);

    uploadedFile.mv(uploadPath, (err) => {
        if (err) {
            return res.status(500).send(err);
        }

        read(uploadPath)

        res.download(modifiedPath, newFilename, (err) => {
            if (err) {
                return res.status(500).send(err);
            }
        });

    });
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});

function read(filename){
    console.log("Creating read stream")

    const stream = fs.createReadStream(filename)
    const rl = readLine.createInterface({
        input: stream,
        crlfDelay: Infinity
    })

    console.log("Reading file lines")

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
        console.log("File reading complete")
    })    
}