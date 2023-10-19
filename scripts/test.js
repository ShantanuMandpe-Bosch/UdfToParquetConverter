const fs = require('fs');
var parquet = require('parquetjs');

async function merge(f1,f2,output){
    try{

        const fr1 = await parquet.ParquetReader.openFile(f1)

        const fr2 = await parquet.ParquetReader.openFile(f2)

        console.log(fr1.schema.schema)
        console.log(fr2.schema.schema)

        test = {Sensor1 : fr1.schema.schema['Sensor1'], Sensor2 : fr2.schema.schema['Sensor2']}

        console.log(test)

        const mergedSchema = new parquet.ParquetSchema(test)

        const writer = await parquet.ParquetWriter.openFile(mergedSchema,output)

        let cursor = fr1.getCursor();
        let record = null;
        while (record = await cursor.next()) {
          writer.appendRow(record)
        }

        cursor = fr2.getCursor();
        record = null;
        while (record = await cursor.next()) {
          writer.appendRow(record)
        }

        await writer.close()
    } catch (error){
        console.log('Error: ',error)
    }
}

const f1 = 'main1.parquet'
const f2 = 'main2.parquet'
const output = 'merged.parquet'

// merge(f1,f2,output)


console.log(myArray)