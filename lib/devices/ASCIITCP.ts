/*
 *  Factory+ / AMRC Connectivity Stack (ACS) Edge component
 *  Copyright 2023 AMRC
 */

import {DeviceConnection, Device, deviceOptions} from "../device.js";
import {log} from "../helpers/log.js";
import {SparkplugNode} from "../sparkplugNode.js";
import {Metrics, serialisationType} from "../helpers/typeHandler.js";
import net from "net";
import {AxiosResponse} from "axios";

export default interface ASCIITCPConnDetails {
    ip: string,
    port: number,
    encoding: string,
    delimiter: string,
    keepAlive: number,
}

/**
 * Define class for your device connection type
 */
export class ASCIITCPConnection extends DeviceConnection {

    #ip: string
    #port: number
    #encoding: string
    delimiter: string
    #keepAlive: number
    #socket: net.Socket | null
    private _queue: string;
    private _promiseResolutionQueue: any[];

    constructor(type: string, connDetails: ASCIITCPConnDetails) {
        super(type);
        this.#ip = connDetails.ip;
        this.#port = connDetails.port;
        this.#encoding = connDetails.encoding;
        this.delimiter = connDetails.delimiter;
        this.#keepAlive = connDetails.keepAlive;
        this.#socket = null;
        this._queue = "";
        // queue of unresolved/outstanding promises functions
        // in the form {resolve, reject}
        this._promiseResolutionQueue = [];
    }


    async open() {
        try {
            const vm = this;
            await new Promise((resolve, reject) => {
                vm.#socket = net.createConnection({port: vm.#port, host: vm.#ip});
                vm.#socket.setEncoding('utf8');
                vm.#socket.setKeepAlive(true, 2000);

                vm.#socket.on('end', async () => {
                    console.log('ASCII TCP connection ended');
                    await vm.close();
                });

                vm.#socket.on('data', (data: any) => this._receiveData(data));

                vm.#socket.on('error', async (err) => {
                    console.log('ASCII TCP connection error', err);
                    await vm.close();
                });

                vm.#socket.on('connect', () => {
                    console.log('ASCII TCP connection established');
                    resolve(true);
                });

            }).then(() => {
                // When ready you MUST emit an "open" event
                log(`ASCII TCP connected to ${this.#ip}:${this.#port}`);
                this.emit("open");
            });
        } catch (e) {
            console.log(e);
        }

    }

    /**
     *
     * @param metrics Metrics object
     * @param writeCallback Function to call when write attempt complete
     * @param payloadFormat Optional string denoting the payload format, must be one of serialisationType
     * @param delimiter Optional string specifying the delimiter character if needed
     */
    writeMetrics(metrics: Metrics, writeCallback: Function, payloadFormat?: serialisationType, delimiter?: string) {
        let err = null;
        // Do whatever connection specific stuff you need to in order to write to the device

        // Call the writeCallback when complete, setting the error if necessary
        writeCallback(err);
    }

    /**
     *
     * @param metrics Metrics object
     * @param payloadFormat Optional string denoting the payload format, must be one of serialisationType
     * @param delimiter Optional string specifying the delimiter character if needed
     */
    async readMetrics(metrics: Metrics, payloadFormat?: string, delimiter?: string) {
        if (payloadFormat !== "ASCII HEX") {
            log("The ASCII TCP driver only supports ASCII HEX payload format. Sorry, we probably should have told you this about this before now.")
        } else {
            await Promise.all(metrics.addresses.map(async (addr) => {
                if (addr !== 'undefined') {

                    this.sendMessage(addr).then((res) => {
                        // Receive response
                        if (res) {
                            let payload = res;
                            let obj: any = {};
                            obj[addr] = payload;
                            console.log(obj);
                            this.emit('data', obj);
                        }

                    })
                }
            }))
        }
    }

    /**
     * Close connection and tidy up
     */
    async close() {
        try {
            this.#socket?.end();
            this.emit('close');
        } catch (e) {
            console.log(e);
        }
    }

    sendMessage(message: string) {
        let promiseResolve, promiseReject;
        let vm = this;
        const prom = new Promise((resolve, reject) => {
            if (vm.#socket) {
                promiseResolve = resolve;
                promiseReject = reject;
                log("TX => " + message)
                vm.#socket.write(Buffer.from(message + JSON.parse('"' + vm.delimiter + '"')));
            }
        });

        this._promiseResolutionQueue.push({promiseResolve, promiseReject});

        return prom;
    }

    _receiveData = (data: any) => {

        this._queue += data;
        let nextChunk = this._queue.indexOf(JSON.parse('"' + this.delimiter + '"'));
        while (nextChunk > 0) {
            const response = this._queue.slice(0, nextChunk);
            this._queue = this._queue.slice(nextChunk + 1);
            nextChunk = this._queue.indexOf(JSON.parse('"' + this.delimiter + '"'));
            console.log("RX <= " + response)
            if (this._promiseResolutionQueue.length > 0) {
                const promRes = this._promiseResolutionQueue.shift();
                promRes.promiseResolve(response);
            }
        }
    }
}

/**
 * Define device for your device type
 */
export class ASCIITCPDevice extends Device {
    // Declare any class attributes and types here
    #devConn: ASCIITCPConnection

    constructor(spClient: SparkplugNode, devConn: ASCIITCPConnection, options: deviceOptions) {
        super(spClient, devConn, options);
        this.#devConn = devConn;

        // Add metrics from options argument
        this._metrics.add(options.metrics);

        this._isConnected = true;
        log(`${this._name} ready`);
    }
};
