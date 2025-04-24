import fs from 'fs';
const fileName = process.argv[2];
const readStream = fs.createReadStream(fileName);
const stationMap = new Map(); // Single map to hold all data per station
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
    
    // Create a hash key from the station buffer
    const stationBuffer = buffer.subarray(start, separatorPos);
    const stationKey = bufferToKey(stationBuffer);
    
    // Parse temperature value from bytes
    const temperatureStr = buffer.subarray(separatorPos + 1, end).toString('utf-8');
    const temperature = Math.floor(parseFloat(temperatureStr) * 10);
    
    // Update station data
    if (stationMap.has(stationKey)) {
        const stationData = stationMap.get(stationKey);
        stationData.min = Math.min(stationData.min, temperature);
        stationData.max = Math.max(stationData.max, temperature);
        stationData.sum += temperature;
        stationData.count += 1;
    } else {
        // Store the original buffer for later conversion to string
        stationMap.set(stationKey, {
            buffer: Buffer.from(stationBuffer), // Create a copy of the buffer
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1
        });
    }
}

// Create a unique key for the buffer content
function bufferToKey(buffer) {
    // Using the buffer as a key in a Map can be done by creating a string 
    // representation of the bytes, but NOT converting to UTF-8
    let key = '';
    for (let i = 0; i < buffer.length; i++) {
        key += String.fromCharCode(buffer[i]);
    }
    return key;
}

function printCompiledResults() {
    // Sort station keys
    const sortedEntries = Array.from(stationMap.entries()).sort((a, b) => {
        const aName = a[1].buffer.toString('utf-8');
        const bName = b[1].buffer.toString('utf-8');
        return aName.localeCompare(bName);
    });

    let result =
        '{' +
        sortedEntries
            .map(([_, data]) => {
                const stationName = data.buffer.toString('utf-8');
                return `${stationName}=${round(data.min / 10)}/${round(
                    data.sum / 10 / data.count
                )}/${round(data.max / 10)}`;
            })
            .join(', ') +
        '}';

    console.log(result);
}

function round(num) {
    const fixed = Math.round(10 * num) / 10;
    return fixed.toFixed(1);
}