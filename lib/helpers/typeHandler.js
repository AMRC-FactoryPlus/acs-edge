/*
 *  Factory+ / AMRC Connectivity Stack (ACS) Edge component
 *  Copyright 2023 AMRC
 */
import { JSONPath } from "jsonpath-plus";
import * as xpath from "xpath";
import { DOMParser } from "@xmldom/xmldom";
import * as jsonpointer from 'jsonpointer';
import { MessageSecurityMode, SecurityPolicy } from "node-opcua";
import { log } from "./log.js";
export var serialisationType;
(function (serialisationType) {
    serialisationType["ignored"] = "Defined by Protocol";
    serialisationType["delimited"] = "Delimited String";
    serialisationType["JSON"] = "JSON";
    serialisationType["XML"] = "XML";
    serialisationType["fixedBuffer"] = "Buffer";
    serialisationType["serialisedBuffer"] = "Buffer";
})(serialisationType || (serialisationType = {}));
export var sparkplugDataType;
(function (sparkplugDataType) {
    sparkplugDataType["boolean"] = "boolean";
    sparkplugDataType["int8"] = "int8";
    sparkplugDataType["int16"] = "int16";
    sparkplugDataType["int32"] = "int32";
    sparkplugDataType["int64"] = "int64";
    sparkplugDataType["uInt8"] = "uInt8";
    sparkplugDataType["uInt16"] = "uInt16";
    sparkplugDataType["uInt32"] = "uInt32";
    sparkplugDataType["uInt64"] = "uInt64";
    sparkplugDataType["float"] = "float";
    sparkplugDataType["double"] = "double";
    sparkplugDataType["dateTime"] = "dateTime";
    sparkplugDataType["string"] = "string";
    sparkplugDataType["text"] = "text";
    sparkplugDataType["uuid"] = "uuid";
    sparkplugDataType["dataSet"] = "dataSet";
    sparkplugDataType["bytes"] = "bytes";
    sparkplugDataType["file"] = "file";
    sparkplugDataType["template"] = "template";
    sparkplugDataType["propertySet"] = "propertySet";
    sparkplugDataType["propertySetList"] = "propertySetList";
})(sparkplugDataType || (sparkplugDataType = {}));
export var restAuthMethod;
(function (restAuthMethod) {
    restAuthMethod["None"] = "None";
    restAuthMethod["Basic"] = "Basic";
})(restAuthMethod || (restAuthMethod = {}));
export var OPCUADataType;
(function (OPCUADataType) {
    OPCUADataType["boolean"] = "Boolean";
    OPCUADataType["int8"] = "SByte";
    OPCUADataType["int16"] = "Int16";
    OPCUADataType["int32"] = "Int32";
    OPCUADataType["int64"] = "Int64";
    OPCUADataType["uInt8"] = "Byte";
    OPCUADataType["uInt16"] = "UInt16";
    OPCUADataType["uInt32"] = "UInt32";
    OPCUADataType["uInt64"] = "UInt64";
    OPCUADataType["float"] = "Float";
    OPCUADataType["double"] = "Double";
    OPCUADataType["dateTime"] = "DateTime";
    OPCUADataType["string"] = "String";
    OPCUADataType["text"] = "String";
    OPCUADataType["uuid"] = "Guid";
    OPCUADataType["dataSet"] = "dataSet";
    OPCUADataType["bytes"] = "bytes";
    OPCUADataType["file"] = "file";
    OPCUADataType["template"] = "template";
    OPCUADataType["propertySet"] = "propertySet";
    OPCUADataType["propertySetList"] = "propertySetList";
})(OPCUADataType || (OPCUADataType = {}));
export var byteOrder;
(function (byteOrder) {
    byteOrder[byteOrder["littleEndian"] = 1234] = "littleEndian";
    byteOrder[byteOrder["bigEndian"] = 4321] = "bigEndian";
    byteOrder[byteOrder["PDPEndian"] = 3412] = "PDPEndian";
})(byteOrder || (byteOrder = {}));
export class Metrics {
    #array;
    #addrIndex;
    #addrPathIndex;
    #nameIndex;
    #aliasIndex;
    constructor(metricArr) {
        this.#array = metricArr;
        this.#nameIndex = {};
        this.#aliasIndex = {};
        this.#addrPathIndex = {};
        this.#addrIndex = {};
        this.#buildIndices();
    }
    #buildIndices() {
        for (let i = 0; i < this.#array.length; i++) {
            const metric = this.#array[i];
            this.#nameIndex[metric.name || ""] = i;
            // this.#aliasIndex[metric.alias as number] = i;
            if (metric.properties != null && metric.properties.address != null && metric.properties.path != null && metric.properties.method.value.search(/^GET/g) > -1) {
                const addr = metric.properties.address.value;
                if (!this.#addrIndex[addr]) {
                    this.#addrIndex[addr] = [];
                }
                this.#addrIndex[addr].push(i);
                this.#addrIndex[addr].push(i);
                const path = metric.properties.path.value;
                if (!this.#addrPathIndex[addr]) {
                    this.#addrPathIndex[addr] = {};
                }
                this.#addrPathIndex[addr][path] = i;
            }
        }
    }
    setAlias(index, alias) {
        this.#array[index].alias = alias;
        this.#aliasIndex[alias] = index;
    }
    get length() {
        return this.#array.length;
    }
    get array() {
        return this.#array;
    }
    add(metrics) {
        this.#array = this.#array.concat(metrics);
        this.#buildIndices();
    }
    get addresses() {
        return Object.keys(this.#addrPathIndex);
    }
    getByName(name) {
        return this.#array[this.#nameIndex[name]];
    }
    setValueByName(name, value, timestamp) {
        this.#array[this.#nameIndex[name]].value = value;
        this.#array[this.#nameIndex[name]].timestamp = timestamp || Date.now();
        this.#array[this.#nameIndex[name]].isNull = (value === null);
        return this.#array[this.#nameIndex[name]];
    }
    setValueByIndex(index, value, timestamp) {
        this.#array[index].value = value;
        this.#array[index].timestamp = timestamp || Date.now();
        this.#array[index].isNull = (value === null);
        return this.#array[index];
    }
    // removeByName(name: string) {
    //     const spliced = this.#array.splice(this.#nameIndex[name], 1);
    //     this.#buildIndices();
    //     return spliced;
    // }
    getByAlias(alias) {
        return this.#array[this.#aliasIndex[alias]];
    }
    setValueByAlias(alias, value, timestamp) {
        this.#array[this.#aliasIndex[alias]].value = value;
        this.#array[this.#aliasIndex[alias]].timestamp = timestamp || Date.now();
        this.#array[this.#aliasIndex[alias]].isNull = (value === null);
        return this.#array[this.#aliasIndex[alias]];
    }
    // removeByAlias(alias: number) {
    //     return this.#array.splice(this.#aliasIndex[alias], 1);
    // }
    getByAddress(addr) {
        return this.#addrIndex[addr].map((x) => this.#array[x]);
    }
    getPathsForAddr(addr) {
        return Object.keys(this.#addrPathIndex[addr] || {});
    }
    getByAddrPath(addr, path) {
        return this.#array[this.#addrPathIndex[addr][path]];
    }
    setValueByAddrPath(addr, path, value, timestamp) {
        this.#array[this.#addrPathIndex[addr][path]].value = value;
        this.#array[this.#addrPathIndex[addr][path]].timestamp = timestamp || Date.now();
        this.#array[this.#addrPathIndex[addr][path]].isNull = (value === null);
        return this.#array[this.#addrPathIndex[addr][path]];
    }
}
export function parseValueFromPayload(msg, metric, payloadFormat, delimiter) {
    let payload;
    const path = (typeof metric.properties !== "undefined" && typeof metric.properties.path !== "undefined" ? metric.properties.path.value : "");
    switch (payloadFormat) {
        case serialisationType.delimited:
            // Handle no delimiter
            payload = (delimiter != '') ? msg.toString()
                .split(delimiter) : msg.toString();
            // Handle no path parsing
            let newVal = (path != '') ? payload[path] : payload;
            return parseTypeFromString(metric.type, newVal);
        case serialisationType.JSON:
            try { // Handles error if invalid JSON is sent in
                if (typeof msg == "string") {
                    payload = JSON.parse(msg);
                }
                else if (Buffer.isBuffer(msg)) {
                    payload = JSON.parse(msg.toString());
                }
                else {
                    payload = msg;
                }
            }
            catch (e) {
                log('Failed to parse JSON data');
            }
            if (path) {
                let newVal = JSONPath({ path: path, json: payload });
                if (payload === 0) {
                    return 0;
                }
                else {
                    if (newVal[0]?.type === "Buffer") {
                        newVal[0] = Buffer.from(newVal[0].data);
                    }
                    return parseTypeFromString(metric.type, newVal[0]);
                }
            }
            else {
                if (metric.type == sparkplugDataType.dataSet) {
                    let newVal = { ...metric.value };
                    if (!Array.isArray(payload)) {
                        payload = [payload];
                    }
                    let dataSet = [];
                    payload.forEach((row, i) => {
                        let arr = [];
                        metric.value.columns.forEach((col) => {
                            arr.push(row[col]);
                        });
                        dataSet[i] = arr;
                    });
                    newVal.rows = dataSet;
                    return newVal;
                }
            }
            break;
        case serialisationType.XML:
            if (path) {
                const doc = new DOMParser().parseFromString(msg.toString());
                const xPathResults = xpath.select(path, doc);
                let res = xPathResults.toString();
                return parseTypeFromString(metric.type, res);
            }
            break;
        case serialisationType.fixedBuffer:
            return parseValFromBuffer(metric.type, (typeof metric.properties !== "undefined" && typeof metric.properties.endianness !== "undefined" ? metric.properties.endianness.value : 0), parseInt(path), msg);
        case serialisationType.serialisedBuffer:
            break;
        default:
            break;
    }
}
/**
 * Handles pass through of timestamp from device connection
 * Currently only supports JSON
 * @param msg
 * @param metric
 * @param payloadFormat
 * @param delimiter
 */
export function parseTimeStampFromPayload(msg, metric, payloadFormat, delimiter) {
    let payload;
    switch (payloadFormat) {
        case serialisationType.JSON:
            try { // Handles error if invalid JSON is sent in
                if (typeof msg == "string") {
                    payload = JSON.parse(msg);
                }
                else if (Buffer.isBuffer(msg)) {
                    payload = JSON.parse(msg.toString());
                }
                else {
                    payload = msg;
                }
            }
            catch (e) {
                log('Failed to parse JSON data');
            }
            const timestamp = JSONPath({
                path: '$.timestamp', json: payload
            });
            return timestamp[0];
        default:
            return undefined;
    }
}
function parseTypeFromString(type, val) {
    // Pass straight through if not a string
    if (typeof val !== 'string') {
        return val;
    }
    let testVal;
    if (type.match(/[Ff]loat/) || type.match(/[Dd]ouble/)) {
        testVal = parseFloat(val);
        return isNaN(testVal) ? null : testVal;
    }
    else if (type.match(/[iI]nt/g)) {
        testVal = parseInt(val);
        return isNaN(testVal) ? null : testVal;
    }
    else if (type.match(/[Dd]ateTime/g)) {
        return Date.parse(val);
    }
    else if (type.match(/[Bb]oolean/g)) {
        return stringToBoolean(val);
    }
    else {
        return val;
    }
}
function stringToBoolean(string) {
    switch (string.toLowerCase()) {
        case "false":
        case "no":
        case "0":
        case "": return false;
        default: return true;
    }
}
/**
 * Function to prepare payload to send to a device connection
 * @param metrics
 * @param payloadFormat
 * @param delimiter
 */
export function writeValuesToPayload(metrics, payloadFormat, delimiter) {
    let payload;
    switch (payloadFormat) {
        case serialisationType.JSON:
            payload = {};
            metrics.forEach((metric) => {
                if (typeof metric.properties !== "undefined" && typeof metric.properties.path !== "undefined") {
                    // Set the value of the metric based on the tagpath value (e.g. $.v)
                    const valPointer = JSONPath.toPointer(JSONPath.toPathArray(metric.properties.path.value));
                    jsonpointer.set(payload, valPointer, metric.value);
                }
            });
            return JSON.stringify(payload);
        case serialisationType.fixedBuffer:
            payload = Buffer.alloc(0);
            metrics.forEach((metric) => {
                if (payload.length) {
                    payload = Buffer.concat([payload, writeValToBuffer(metric)]);
                }
                else {
                    payload = writeValToBuffer(metric);
                }
            });
            if (payload && payload.length) {
                return payload;
            }
            return "";
        case serialisationType.serialisedBuffer:
            console.log(`Writing to serialised buffer payloads is not yet supported.`);
            return "";
        case serialisationType.XML:
            console.log(`Writing to XML payloads is not yet supported.`);
            return "";
        case serialisationType.delimited:
            payload = [];
            metrics.forEach((metric) => {
                payload.push(metric.value);
            });
            return payload.join(delimiter);
        default:
            return "";
    }
}
// Returns the number of bytes required to hold the variable type
// These are the fixed length variable types in the sparkplug spec.
// Variable length types (String, Buffer etc) are not considered... yet.
export function typeLens(type) {
    switch (type) {
        case 'boolean':
        case 'uInt':
        case 'int':
            return 1;
        case 'uInt16':
        case 'int16':
            return 2;
        case 'uInt32':
        case 'int32':
        case 'float':
            return 4;
        case 'double':
            return 8;
        case 'dateTime':
            return 12;
        default:
            return -1;
    }
}
/**
 *
 * @param type Sparkplug Datatype of metric
 * @param endianness Endianness of metric
 * @param byteAddr Register address of metric
 * @param buf Buffer to read from
 * @param bit Bit position to read from within byte (optional)
 * @returns Value parsed from buffer
 */
export function parseValFromBuffer(type, endianness, byteAddr, buf, bit) {
    // Reads in a variable of the specified metric.type type from buffer buf starting at position byteAddr
    // Datetime is not implemented properly as it uses Rikki's workaround for Siemens PLCs
    let fullType = type.toString();
    if (["boolean", "int8", "uInt8"].indexOf(fullType) == -1) {
        fullType += getEndianString(endianness || null);
    }
    switch (fullType) {
        case 'boolean':
            if (bit) {
                return (!!(buf[byteAddr] & (1 << bit)));
            }
            else {
                return -1;
            }
        case 'int8':
            return buf.readInt8(byteAddr);
        case 'int16BE':
            return buf.readInt16BE(byteAddr);
        case 'int16LE':
            return buf.readInt16LE(byteAddr);
        case 'int32BE':
            return buf.readInt32BE(byteAddr);
        case 'int32LE':
            return buf.readInt32LE(byteAddr);
        case 'int64BE':
            return buf.readBigInt64BE(byteAddr);
        case 'int64LE':
            return buf.readBigInt64LE(byteAddr);
        case 'int64PDP':
            return buf.subarray(byteAddr, byteAddr + 8)
                .swap16()
                .readBigInt64BE();
        case 'int32PDP':
            return buf.subarray(byteAddr, byteAddr + 4)
                .swap16()
                .readInt32BE();
        case 'uInt8':
            return buf.readUInt8(byteAddr);
        case 'uInt16BE':
            return buf.readUInt16BE(byteAddr);
        case 'uInt16LE':
            return buf.readUInt16LE(byteAddr);
        case 'uInt32BE':
            return buf.readUInt32BE(byteAddr);
        case 'uInt32LE':
            return buf.readUInt32LE(byteAddr);
        case 'uInt32PDP':
            return buf.subarray(byteAddr, byteAddr + 4)
                .swap16()
                .readUInt32BE();
        case 'dateTimeBE':
        case 'uInt64BE':
            return Number(buf.readBigUInt64BE(byteAddr));
        case 'dateTimeLE':
        case 'uInt64LE':
            return Number(buf.readBigUInt64LE(byteAddr));
        case 'dateTimePDP':
        case 'uint64PDP':
            return buf.subarray(byteAddr, byteAddr + 8)
                .swap16()
                .readBigUInt64BE();
        case 'floatBE':
            return buf.readFloatBE(byteAddr);
        case 'floatLE':
            return buf.readFloatLE(byteAddr);
        case 'floatPDP':
            return (buf.subarray(byteAddr, byteAddr + 4)
                .swap16()).readFloatBE();
        case 'doubleBE':
            return buf.readDoubleBE(byteAddr);
        case 'doubleLE':
            return buf.readDoubleLE(byteAddr);
        case 'doublePDP':
            return buf.subarray(byteAddr, byteAddr + 8)
                .swap16()
                .readDoubleBE();
        case 'uuid':
            return buf.toString();
        case 'String':
            return buf.toString();
        default:
        // return null;
    }
}
function getEndianString(endianness) {
    switch (endianness) {
        case (byteOrder.bigEndian):
            return "BE";
        case (byteOrder.littleEndian):
            return "LE";
        case (byteOrder.PDPEndian):
            return "PDP";
        default:
            return "";
    }
}
export function writeValToBuffer(metric) {
    // Writes out a variable of the specified metric.type type to output buffer buf
    // Datetime, string, and buffer are not implemented...yet
    let buf = Buffer.allocUnsafe(8);
    let len = 0;
    const endianness = (typeof metric.properties !== "undefined" && typeof metric.properties.endianness !== "undefined" ? metric.properties.endianness.value : byteOrder.bigEndian);
    let fullType = metric.type + getEndianString(endianness);
    try {
        switch (fullType) {
            case 'boolean':
                // This must be handled outside of this function
                // as the whole byte must first be read, then the
                // bit of interest set, then the new byte written
                break;
            case 'int8':
                len = buf.writeInt8(metric.value);
                break;
            case 'int16BE':
                len = buf.writeInt16BE(metric.value);
                break;
            case 'int16LE':
                len = buf.writeInt16LE(metric.value);
                break;
            case 'int32PDP':
            case 'int32BE':
                len = buf.writeInt32BE(metric.value);
                break;
            case 'int32LE':
                len = buf.writeInt32LE(metric.value);
                break;
            case 'int64BE':
            case 'int64PDP':
                len = buf.writeBigInt64BE(metric.value);
                break;
            case 'uInt8':
                len = buf.writeUInt8(metric.value);
                break;
            case 'uInt16BE':
                len = buf.writeUInt16BE(metric.value);
                break;
            case 'uInt16LE':
                len = buf.writeUInt16LE(metric.value);
                break;
            case 'uInt32BE':
            case 'uInt32PDP':
                len = buf.writeUInt32BE(metric.value);
                break;
            case 'uInt32LE':
                len = buf.writeUInt32LE(metric.value);
                break;
            case 'uInt64BE':
            case 'uInt64PDP':
                len = buf.writeBigUInt64BE(metric.value);
                break;
            case 'floatBE':
            case 'floatPDP':
                len = buf.writeFloatBE(metric.value);
                break;
            case 'floatLE':
                len = buf.writeFloatLE(metric.value);
                break;
            case 'doubleBE':
            case 'doublePDP':
                len = buf.writeDoubleBE(metric.value);
                break;
            case 'doubleLE':
                len = buf.writeDoubleLE(metric.value);
                break;
            case 'dateTime':
                break;
            default:
                break;
        }
        if (fullType.endsWith('PDP')) {
            buf.swap16();
        }
        return buf.slice(0, len);
    }
    catch (err) {
        console.log(err);
        return buf.slice(0);
    }
}
/**
 * Converting OPC Security Policy to correct class type
 * @param type Security policy type as string (e.g. "Basic256Sha256", "None", etc.)
 * @returns OPC Security Policy type
 */
export function getOpcSecurityPolicy(type) {
    switch (type) {
        case ("None"):
            return SecurityPolicy.None;
        case ("Basic128"):
            return SecurityPolicy.Basic128;
        case ("Basic192"):
            return SecurityPolicy.Basic192;
        case ("Basic192Rsa15"):
            return SecurityPolicy.Basic192Rsa15;
        case ("Basic256Rsa15"):
            return SecurityPolicy.Basic256Rsa15;
        case ("Basic256Sha256"):
            return SecurityPolicy.Basic256Sha256;
        case ("Aes128_Sha256"):
            return SecurityPolicy.Aes128_Sha256_RsaOaep;
        case ("Aes128_Sha256_RsaOaep"):
            return SecurityPolicy.Aes128_Sha256_RsaOaep;
        case ("PubSub_Aes128_CTR"):
            return SecurityPolicy.PubSub_Aes128_CTR;
        case ("PubSub_Aes256_CTR"):
            return SecurityPolicy.PubSub_Aes256_CTR;
        case ("Basic128Rsa15"):
            return SecurityPolicy.Basic128Rsa15;
        case ("Basic256"):
            return SecurityPolicy.Basic256;
        default:
            return SecurityPolicy.Invalid;
    }
}
/**
 * Converting OPC Security Mode to correct class type
 * @param type Security mode type as string (e.g. "Sign", "None", "SignAndEncrypt".)
 * @returns OPC Security Mode type
 */
export function getOpcSecurityMode(type) {
    switch (type) {
        case ("None"):
            return MessageSecurityMode.None;
        case ("Sign"):
            return MessageSecurityMode.Sign;
        case ("SignAndEncrypt"):
            return MessageSecurityMode.SignAndEncrypt;
        default:
            return MessageSecurityMode.Invalid;
    }
}
