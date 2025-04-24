const fs = require('fs');
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
    
    // Create a station key from the buffer
    const stationBuffer = buffer.subarray(start, separatorPos);
    const stationName = stationBuffer.toString('utf-8');
    
    // Parse temperature value directly from bytes without string conversion
    const temperature = parseTemperatureBytes(buffer, separatorPos + 1, end);
    
    // Update station data
    if (stationMap.has(stationName)) {
        const stationData = stationMap.get(stationName);
        stationData.min = Math.min(stationData.min, temperature);
        stationData.max = Math.max(stationData.max, temperature);
        stationData.sum += temperature;
        stationData.count += 1;
    } else {
        // Store the data for this station
        stationMap.set(stationName, {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1
        });
    }
}

// Fast parsing of temperature directly from bytes
function parseTemperatureBytes(buffer, start, end) {
    let value = 0;
    let negative = false;
    let decimalSeen = false;
    let decimalPos = 0;
    
    for (let i = start; i < end; i++) {
        const byte = buffer[i];
        
        if (byte === 45) { // '-' character
            negative = true;
        } else if (byte === 46) { // '.' character
            decimalSeen = true;
            decimalPos = 1;
        } else if (byte >= 48 && byte <= 57) { // '0' to '9' characters
            value = value * 10 + (byte - 48);
            if (decimalSeen) {
                decimalPos++;
            }
        }
    }
    
    // Multiply by 10 to avoid floating-point calculations
    // and adjust decimal places
    if (decimalPos === 0) {
        value *= 10; // No decimal point, add a trailing zero
    } else if (decimalPos > 2) {
        value = Math.floor(value / Math.pow(10, decimalPos - 2));
    }
    
    return negative ? -value : value;
}

function printCompiledResults() {
    // Get sorted station names - using basic JavaScript sort which should match expected behavior
    const sortedStations = Array.from(stationMap.keys()).sort();
    
    // Build result string exactly as in the working implementation
    let result = '{';
    
    for (let i = 0; i < sortedStations.length; i++) {
        if (i > 0) {
            result += ', ';
        }
        
        const stationName = sortedStations[i];
        const data = stationMap.get(stationName);
        
        result += `${stationName}=${round(data.min / 10)}/${round(
            data.sum / 10 / data.count
        )}/${round(data.max / 10)}`;
    }
    
    result += '}';
    
    console.log(result);
}

function round(num) {
    const fixed = Math.round(10 * num) / 10;
    return fixed.toFixed(1);
}