import fs from 'fs';
const fileName = process.argv[2];
const readStream = fs.createReadStream(fileName);
const minMap = new Map();
const maxMap = new Map();
const sumMap = new Map();
const countMap = new Map();
let buffer = Buffer.alloc(0);

readStream.on('data', function processChunk(chunk) {
    // Concatenate the new chunk with any remaining buffer
    buffer = Buffer.concat([buffer, chunk]);
    
    let lineStart = 0;
    let i = 0;
    
    // Iterate through each byte
    while (i < buffer.length) {
        // Check for newline (ASCII 10)
        if (buffer[i] === 10) {
            if (i > lineStart) {
                // Process the line
                processLineBytes(buffer, lineStart, i);
            }
            lineStart = i + 1;
        }
        i++;
    }
    
    // Keep the remaining incomplete line in the buffer
    if (lineStart < buffer.length) {
        buffer = buffer.subarray(lineStart);
    } else {
        buffer = Buffer.alloc(0);
    }
});

readStream.on('end', function processEnd() {
    // Process any remaining data in the buffer
    if (buffer.length > 0) {
        processLineBytes(buffer, 0, buffer.length);
    }
    
    printCompiledResults();
});

function processLineBytes(buffer, start, end) {
    let separatorPos = -1;
    
    // Find the semicolon separator by iterating through bytes
    for (let i = start; i < end; i++) {
        if (buffer[i] === 59) { // 59 is ASCII for semicolon
            separatorPos = i;
            break;
        }
    }
    
    if (separatorPos === -1) return; // Skip malformed lines
    
    // Extract station name as UTF-8 string
    const stationName = buffer.subarray(start, separatorPos).toString('utf-8');
    
    // Parse temperature value
    const temperatureStr = buffer.subarray(separatorPos + 1, end).toString('utf-8');
    const temperature = Math.floor(parseFloat(temperatureStr) * 10);
    
    // Update maps
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