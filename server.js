const express = require('express');
const fileUpload = require('express-fileupload');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const udf = require('./scripts/script')

const app = express();
const port = process.env.PORT || 3000;

app.use(fileUpload());

app.use(express.static(__dirname));

app.post('/modify', (req, res) => {
    if (!req.files || Object.keys(req.files).length === 0) {
        return res.status(400).send('No files were uploaded.');
    }

    const uploadedFile = req.files.file;
    const newFilename = uuidv4() + path.extname(udf.outputPath);
    const uploadPath = path.join(__dirname, 'uploads', uploadedFile.name);
    const modifiedPath = path.join(__dirname, 'modified', newFilename);

    uploadedFile.mv(uploadPath, (err) => {
        if (err) {
            return res.status(500).send(err);
        }

        // Here, you can append data to the file using fs
        const newData = 'Additional data to append to the file.\n';

        udf.read(uploadPath)

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