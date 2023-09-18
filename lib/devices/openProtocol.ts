/*
 *  Factory+ / AMRC Connectivity Stack (ACS) Edge component
 *  Copyright 2023 AMRC
 */

import {Device, deviceOptions, DeviceConnection} from "../device.js";
import {log} from "../helpers/log.js";
import {SparkplugNode} from "../sparkplugNode.js";
import {Metrics, serialisationType} from "../helpers/typeHandler.js";
// @ts-ignore
import openProtocol from 'node-open-protocol';

/**
 * Define structure of options for device connection
 */
export default interface openProtocolConnDetails {
    host: string,
    port: number,
}

/**
 * Define class for your device connection type
 */
export class OpenProtocolConnection extends DeviceConnection {

    #options: {
        host: string,
        port: number,
    }
    #client: any

    constructor(type: string, connDetails: openProtocolConnDetails) {
        super(type);

        this.#options = {
            host: connDetails.host,
            port: connDetails.port,
        };

        this.#client = openProtocol.createClient(this.#options.port, this.#options.host, {
            'defaultRevisions': {
                '1': 1
            }
        }, () => {
        })

    }

    /**
     *
     * @param metrics Metrics object
     * @param payloadFormat Optional string denoting the payload format, must be one of serialisationType
     * @param delimiter Optional string specifying the delimiter character if needed
     */
    readMetrics(metrics: Metrics, payloadFormat?: string, delimiter?: string) {

        // This function effectively kicks the keepAlive timer to ensure that a DDEATH is not issued on an
        // inactive broker. It is run at the `Polling Interval (ms)` value of the connection.

        this.emit('data', {});
    }

    async subscribe(event: string) {

        // Ignore if event is undefined
        if (event === 'undefined') return;

        this.#client.on(event, (midData: any) => {
            let obj: any = {};
            obj[event] = midData;
            this.emit('data', obj);
        })

        console.log(`🔌 Subscribing to OpenProtocol device.`, event);
        this.#client.subscribe(event, (err: any, data: any) => {
            if (err) {
                log(`⚠️ Could not subscribe to OpenProtocol device: ${err}`);
                return;
            }
            log(`🔌 Subscribed to OpenProtocol device.`);
        });

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
     * Close connection and tidy up
     */
    async close() {
        // Do whatever cleanup code you need to here
    }
}


/**
 * Define device for your device type
 */
export class OpenProtocolDevice extends Device {
    // Declare any class attributes and types here
    #devConn: OpenProtocolConnection

    constructor(spClient: SparkplugNode, devConn: OpenProtocolConnection, options: deviceOptions) {


        super(spClient, devConn, {
            ...options, ...{
                payloadFormat: serialisationType.JSON,
            }
        });

        // Assign device connection to class attribute
        this.#devConn = devConn;

        this._metrics.add(options.metrics);
        this._metrics.addresses.forEach((event) => {
            if (event) this.#devConn.subscribe(event);
        })
    }
};
