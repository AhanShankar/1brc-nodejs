import fs from 'fs';
const fileName = process.argv[2];
const readStream = fs.createReadStream(fileName, 'utf-8');
const minMap = new Map();
const maxMap = new Map();
const sumMap = new Map();
const countMap = new Map();
let buffer = '';
readStream.on('data', (data) => {
    buffer += data;  // Append the new data chunk to the buffer

    // Process lines in the buffer
    let lines = buffer.split('\n');
    
    // Check if the last line is incomplete and move it to the buffer
    buffer = lines.pop();  // The last part (which may be incomplete) stays in the buffer
    
    // Process each complete line
    for (const line of lines) {
        if (line === '') continue; // Skip empty lines
        
        const [stationName, temperatureStr] = line.split(';');
        const temperature = Math.floor(parseFloat(temperatureStr) * 10);

        // Update the maps with the parsed temperature values
        if (minMap.has(stationName)) {
            minMap.set(stationName, Math.min(minMap.get(stationName), temperature));
        } else {
            minMap.set(stationName, temperature);
        }

        if (maxMap.has(stationName)) {
            maxMap.set(stationName, Math.max(maxMap.get(stationName), temperature));
        } else {
            maxMap.set(stationName, temperature);
        }

        if (sumMap.has(stationName)) {
            sumMap.set(stationName, sumMap.get(stationName) + temperature);
        } else {
            sumMap.set(stationName, temperature);
        }

        if (countMap.has(stationName)) {
            countMap.set(stationName, countMap.get(stationName) + 1);
        } else {
            countMap.set(stationName, 1);
        }
    }
});
readStream.on('end', printCompiledResults)
function printCompiledResults() {
    const sortedStations = Array.from(minMap.keys()).sort();

    let result =
        '{' +
        sortedStations
            .map((station) => {
                return `${station}=${round(minMap.get(station) / 10)}/${round(
                    sumMap.get(station) / 10 / countMap.get(station)
                )}/${round(maxMap.get(station) / 10)}`;
            })
            .join(', ') +
        '}';

    console.log(result);
}
function round(num) {
    const fixed = Math.round(10 * num) / 10;

    return fixed.toFixed(1);
}