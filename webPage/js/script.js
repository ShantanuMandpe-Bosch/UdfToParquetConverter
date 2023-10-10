const parquet = require("parquetjs")

/*async function f1() {
    let reader = await parquet.ParquetReader.openFile('main.parquet');
    let cursor = reader.getCursor();

    // read all records from the file and print them
    let record = null;
    while (record = await cursor.next()) {
        console.log(record);
    }
} f1();*/

const data = [
    {
        x: [1, 2, 3, 4, 5],
        y: [10, 11, 12, 11, 10],
        type: 'scatter',
        mode: 'lines+points',
        marker: { color: 'blue' },
    },
];

const layout = {
    title: 'Sample Plotly Graph',
    xaxis: { title: 'X-axis' },
    yaxis: { title: 'Y-axis' },
};

// Create the Plotly graph in the center of the main section
Plotly.newPlot('plotly-graph', data, layout);