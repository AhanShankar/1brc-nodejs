const fs = require('fs');
const fileName = process.argv[2];
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

// Constants for parsing
const NEWLINE = 10; // ASCII for newline

// Main thread / worker logic split
if (isMainThread) {
    // Main thread code
    const file = fs.openSync(fileName, 'r');
    const fileSize = fs.fstatSync(file).size;
    const numCPUs = os.cpus().length;
    
    // Determine chunk boundaries
    const chunkSize = Math.floor(fileSize / numCPUs);
    const boundaries = [0]; // Start with 0
    
    // Find chunk boundaries at newlines
    for (let i = 1; i < numCPUs; i++) {
        const targetPos = i * chunkSize;
        if (targetPos >= fileSize) break;
        
        // Read a small buffer at the target position to find the next newline
        const buffer = Buffer.alloc(1000);
        fs.readSync(file, buffer, 0, 1000, targetPos);
        
        const nlPos = buffer.indexOf(NEWLINE);
        if (nlPos === -1) {
            boundaries.push(fileSize);
            break;
        } else {
            boundaries.push(targetPos + nlPos + 1);
        }
    }
    
    // Add file end if not already included
    if (boundaries[boundaries.length - 1] !== fileSize) {
        boundaries.push(fileSize);
    }
    
    // Create global results map
    const globalMap = new Map();
    let completedWorkers = 0;
    const numWorkers = boundaries.length - 1;
    
    // Create workers
    for (let i = 0; i < numWorkers; i++) {
        const start = boundaries[i];
        const end = boundaries[i + 1] - 1;
        
        // Skip creating a worker if the range is invalid
        if (start > end) {
            completedWorkers++;
            continue;
        }
        
        const worker = new Worker(__filename, {
            workerData: { 
                fileName,
                start,
                end
            }
        });
        
        // Handle results from worker
        worker.on('message', (workerMap) => {
            // Merge worker results into global map
            for (const [station, data] of workerMap) {
                const existing = globalMap.get(station);
                if (existing) {
                    existing.min = Math.min(existing.min, data.min);
                    existing.max = Math.max(existing.max, data.max);
                    existing.sum += data.sum;
                    existing.count += data.count;
                } else {
                    globalMap.set(station, { ...data });
                }
            }
        });
        
        // Handle worker completion
        worker.on('exit', () => {
            completedWorkers++;
            if (completedWorkers === numWorkers) {
                // All workers done, print results
                printCompiledResults(globalMap);
                fs.closeSync(file);
            }
        });
    }
    
    // Handle the case where we have no valid workers (small file)
    if (completedWorkers === numWorkers) {
        const singleReadStream = fs.createReadStream(fileName);
        const singleStationMap = new Map();
        let singleBuffer = Buffer.alloc(0);
        
        singleReadStream.on('data', function(chunk) {
            if (singleBuffer.length > 0) {
                singleBuffer = Buffer.concat([singleBuffer, chunk]);
                processBufferSingle(singleBuffer, singleStationMap);
            } else {
                processBufferSingle(chunk, singleStationMap);
            }
        });
        
        singleReadStream.on('end', function() {
            // Process any remaining data
            if (singleBuffer.length > 0) {
                let separatorPos = -1;
                for (let i = 0; i < singleBuffer.length; i++) {
                    if (singleBuffer[i] === 59) { // ASCII for semicolon
                        separatorPos = i;
                        break;
                    }
                }
                processSingleLine(singleBuffer, 0, singleBuffer.length, separatorPos, singleStationMap);
            }
            
            printCompiledResults(singleStationMap);
            fs.closeSync(file);
        });
    }
} else {
    // Worker thread code - using original processing logic with minimal changes
    const { fileName, start, end } = workerData;
    const stationMap = new Map();
    let buffer = Buffer.alloc(0);
    
    // Verify valid range
    if (start <= end) {
        const readStream = fs.createReadStream(fileName, {
            start,
            end,
            highWaterMark: 64 * 1024 // Smaller chunks for better memory usage
        });
        
        readStream.on('data', function processChunk(chunk) {
            // Only concatenate if we have leftover data, otherwise use the chunk directly
            if (buffer.length > 0) {
                buffer = Buffer.concat([buffer, chunk]);
                processBuffer(buffer);
                // Reset the buffer after processing if we ended at a complete line
            } else {
                processBuffer(chunk);
                // No need to keep chunk in buffer if we processed everything
            }
        });
        
        readStream.on('end', function processEnd() {
            // Process any remaining data in the buffer
            if (buffer.length > 0) {
                // Find semicolon in any remaining buffer
                let separatorPos = -1;
                for (let i = 0; i < buffer.length; i++) {
                    if (buffer[i] === 59) { // 59 is ASCII for semicolon
                        separatorPos = i;
                        break;
                    }
                }
                processLineBytes(buffer, 0, buffer.length, separatorPos);
            }
            
            // Send results back to main thread
            parentPort.postMessage(Array.from(stationMap.entries()));
        });
    } else {
        // Invalid range, just send empty results
        parentPort.postMessage([]);
    }
    
    // Process a buffer ensuring we stop at complete lines
    function processBuffer(buf) {
        let lineStart = 0;
        let i = 0;
        let separatorPos = -1;
        
        // Iterate through each byte
        while (i < buf.length) {
            // Check for semicolon (ASCII 59)
            if (buf[i] === 59) { // 59 is ASCII for semicolon
                separatorPos = i;
            }
            // Check for newline (ASCII 10)
            else if (buf[i] === NEWLINE) {
                if (i > lineStart) {
                    // Process the line
                    processLineBytes(buf, lineStart, i, separatorPos);
                }
                lineStart = i + 1;
                separatorPos = -1; // Reset separator position for next line
            }
            i++;
        }
        
        // Keep the remaining incomplete line in the buffer
        if (lineStart < buf.length) {
            buffer = buf.subarray(lineStart);
        } else {
            buffer = Buffer.alloc(0);
        }
    }
    
    function processLineBytes(buffer, start, end, separatorPos) {
        if (separatorPos === -1 || separatorPos < start || separatorPos >= end) return; // Skip malformed lines
        
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
}

// Single-thread processing for small files
function processBufferSingle(buf, stationMap) {
    let lineStart = 0;
    let i = 0;
    let separatorPos = -1;
    let buffer = Buffer.alloc(0);
    
    // Iterate through each byte
    while (i < buf.length) {
        // Check for semicolon (ASCII 59)
        if (buf[i] === 59) { // 59 is ASCII for semicolon
            separatorPos = i;
        }
        // Check for newline (ASCII 10)
        else if (buf[i] === NEWLINE) {
            if (i > lineStart) {
                // Process the line
                processSingleLine(buf, lineStart, i, separatorPos, stationMap);
            }
            lineStart = i + 1;
            separatorPos = -1; // Reset separator position for next line
        }
        i++;
    }
    
    // Keep the remaining incomplete line
    if (lineStart < buf.length) {
        buffer = buf.subarray(lineStart);
    } else {
        buffer = Buffer.alloc(0);
    }
    
    return buffer;
}

function processSingleLine(buffer, start, end, separatorPos, stationMap) {
    if (separatorPos === -1 || separatorPos < start || separatorPos >= end) return; // Skip malformed lines
    
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

function printCompiledResults(resultsMap = null) {
    // Use provided map or global stationMap
    const mapToUse = resultsMap || stationMap;
    
    // Get sorted station names
    const sortedStations = Array.from(mapToUse.keys()).sort();
    
    // Build result string
    let result = '{';
    
    for (let i = 0; i < sortedStations.length; i++) {
        if (i > 0) {
            result += ', ';
        }
        
        const stationName = sortedStations[i];
        const data = mapToUse.get(stationName);
        
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