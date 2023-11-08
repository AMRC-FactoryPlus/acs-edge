/*
 *  Factory+ / AMRC Connectivity Stack (ACS) Edge component
 *  Copyright 2023 AMRC
 */
import { Device, DeviceConnection } from "../device.js";
import { log } from "../helpers/log.js";
import { S7Endpoint, S7ItemGroup } from '@st-one-io/nodes7';
export class S7Connection extends DeviceConnection {
    #s7Conn;
    #itemGroup;
    #vars;
    constructor(type, connDetails) {
        super(type);
        // Instantiate S7 endpoint
        this.#s7Conn = new S7Endpoint({
            host: connDetails.hostname,
            port: connDetails.port,
            rack: connDetails.rack,
            slot: connDetails.slot,
            autoReconnect: connDetails.timeout
        });
        // Prepare variables to hold optimized metric list
        this.#itemGroup = null;
        this.#vars = {};
        // Pass on disconnect event to parent
        this.#s7Conn.on('disconnect', () => {
            this.emit('close');
        });
        // Notify when connection is ready
        this.#s7Conn.on('connect', () => {
            log(`S7 connected to ${this.#s7Conn._connOptsTcp.host}:${this.#s7Conn._connOptsTcp.port}`);
            this.emit("open");
        });
        // Pass on errors to parent
        this.#s7Conn.on('error', (e) => {
            this.emit('error');
            log("S7 Error: " + e);
        });
    }
    /**
     * Builds the S7 item group from the defined metric list
     * @param {object} vars object containing metric names and PLC addresses
     */
    setItemGroup(vars) {
        if (this.#itemGroup) {
            this.#itemGroup.destroy();
        }
        this.#itemGroup = new S7ItemGroup(this.#s7Conn);
        this.#itemGroup.setTranslationCB((metric) => this.#vars[metric]); //translates a metric name to its address
        this.#vars = vars;
        this.#itemGroup.addItems(Object.keys(this.#vars));
    }
    /**
     * Open the connection to the PLC
     */
    async open() {
        if (!this.#s7Conn.isConnected) {
            this.#s7Conn.connect();
        }
    }
    /**
     * Read metrics from PLC
     * @param {array} metrics Array of metric objects to read to
     * @returns {array} Old metric values (for RbE checking)
     */
    readMetrics(metrics, payloadFormat) {
        const changedMetrics = [];
        // Tell S7 to update metric values
        let newVals = this.#itemGroup.readAllItems(); // name: value
        this.emit('data', newVals, false);
    }
    /**
     * Writes metric values to the PLC
     * @param {array} metrics Array of metric objects to write to the PLC
     */
    writeMetrics(metrics) {
        // This doesn't seem to work for Ixxx value writes
        // Untested with other writes at present
        // Go through each metric to write and separate the name and desired value
        const addrs = [];
        const values = [];
        metrics.array.forEach((metric) => {
            if (typeof metric.properties !== "undefined" && metric.properties.address.value) {
                addrs.push(metric.properties.address.value);
                values.push(metric.value);
            }
        });
        // If there are actuall metrics to be written...
        if (addrs.length) {
            // Notify user
            log(`Writing ${values} to ${addrs}`);
            // Write metric values
            this.#itemGroup.writeItems(addrs, values);
        }
    }
    /**
     * Close connection and tidy up
     */
    close() {
        // Clear the variable list
        this.#vars = {};
        // Destroy the metric item group, if it exists
        if (this.#itemGroup) {
            this.#itemGroup.destroy();
        }
        // Close the PLC connection
        this.#s7Conn.disconnect();
    }
}
// !! IMPORTANT !!
// Ensure metric addresses are as specified here: 
// https://github.com/st-one-io/node-red-contrib-s7#variable-addressing
export class S7Device extends (Device) {
    s7Vars;
    #devConn;
    constructor(spClient, devConn, options) {
        super(spClient, devConn, options);
        this.#devConn = devConn;
        this._metrics.add(options.metrics);
        // Prepare list of variables for S7 library to use
        this.s7Vars = {};
        // Push metric to S7 variables list
        options.metrics.forEach((metric) => {
            if (typeof metric.properties !== "undefined" && metric.properties.address.value) {
                this.s7Vars[metric.properties.address.value] =
                    metric.properties.address.value;
            }
        });
        // Set S7 variables as item group (this allows optimization of PLC transactions)
        this.#devConn.setItemGroup(this.s7Vars);
    }
}
