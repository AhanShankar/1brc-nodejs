const fs = require('fs');
const fileName = process.argv[2];
const readStream = fs.createReadStream(fileName, { highWaterMark: 1e9 });
const stationMap = new Map(); // Map to hold data per station
let buffer = Buffer.alloc(0);

// Constants for parsing
const NEWLINE = 10; // ASCII for newline
const SEMICOLON = 59; // ASCII for semicolon

// Temperature can be:
// 1.1    -> 3 chars -> newline at position i+3
// 11.1   -> 4 chars -> newline at position i+4
// -9.9   -> 4 chars -> newline at position i+4
// -99.9  -> 5 chars -> newline at position i+5

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
        // We've seen this station, update its data
        const stationData = stationMap.get(stationName);
        stationData.min = Math.min(stationData.min, temperature);
        stationData.max = Math.max(stationData.max, temperature);
        stationData.sum += temperature;
        stationData.count += 1;
    } else {
        // New station, add to the map
        stationMap.set(stationName, {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1
        });
    }
}

// Optimized temperature parsing with the constraint of exactly one decimal place
function parseTemperatureBytes(buffer, start, end) {
    // Since we know there's always exactly one decimal place, we can
    // simplify by just reading all digits and multiplying appropriately
    
    // Check for minus sign
    let negative = buffer[start] === 45; // '-' is ASCII 45
    
    // Starting position for digits (skip minus sign if present)
    let i = negative ? start + 1 : start;
    
    // First digit before decimal (can be multiple digits if value >= 10)
    let intPart = 0;
    
    // Find the decimal point
    while (i < end && buffer[i] !== 46) { // '.' is ASCII 46
        // Accumulate integer part digits
        intPart = intPart * 10 + (buffer[i] - 48); // '0' is ASCII 48
        i++;
    }
    
    // Skip the decimal point
    i++;
    
    // Read the single digit after decimal (we know there's exactly one)
    const decimalPart = buffer[i] - 48;
    
    // Compute final result: multiply by 10 to get fixed-point integer
    const result = intPart * 10 + decimalPart;
    
    return negative ? -result : result;
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
    
    result += '}\n';
    
    process.stdout.write(result);
}

function round(num) {
    const fixed = Math.round(10 * num) / 10;
    return fixed.toFixed(1);
}