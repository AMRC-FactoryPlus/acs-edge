/*
 *  Factory+ / AMRC Connectivity Stack (ACS) Edge component
 *  Copyright 2023 AMRC
 */

import timers from "timers/promises";
import { ServiceClient } from "@amrc-factoryplus/utilities";
import type { Identity } from "@amrc-factoryplus/utilities";

/* XXX These need to be incorporated into the main codebase. The config
 * rehashing just needs to go: the code which uses the config needs
 * adapting to accept the correct format. */
import {validateConfig} from '../utils/CentralConfig.js';
import {reHashConf} from "../utils/FormatConfig.js";

// Import device connections
import {
    SparkplugNode
} from "./sparkplugNode.js";

// Import devices
import {
    RestConnection,
    RestDevice
} from "./devices/REST.js";
import {
    S7Connection,
    S7Device
} from "./devices/S7.js";
import {
    OPCUAConnection,
    OPCUADevice
} from "./devices/OPCUA.js";
import {
    MQTTConnection,
    MQTTDevice
} from "./devices/MQTT.js";
import {
    UDPConnection,
    UDPDevice
} from "./devices/UDP.js";
import {
    WebsocketConnection,
    WebsocketDevice
} from "./devices/websocket.js";
import {
    MTConnectConnection,
    MTConnectDevice
} from "./devices/MTConnect.js";
import {
    log
} from "./helpers/log.js";
import {
    sparkplugConfig
} from "./helpers/typeHandler.js";
import {
    Device,
    deviceOptions
} from "./device.js";
import {EventEmitter} from "events";

/**
 * Translator class basically turns config file into instantiated classes
 * for device protocol translation and Sparkplug communication
 */

export interface translatorConf {
    sparkplug: sparkplugConfig,
    deviceConnections: any[]
}

interface deviceInfo {
    type: any,
    connection: any;
    connectionDetails: any
}

const EdgeAgentConfig = "aac6f843-cfee-4683-b121-6943bfdf9173"; 

export class Translator extends EventEmitter {
    /**
     * Class constructor - Unpacks config file and defines helpful class attributes
     * @param {Object} conf Config file from web UI
     */
    sparkplugNode!: SparkplugNode
    fplus: ServiceClient
    pollInt: number

    connections: {
        [index: string]: any
    }
    devices: {
        [index: string]: any
    }

    constructor(fplus: ServiceClient, pollInt: number) {
        super();

        this.fplus = fplus;
        this.pollInt = pollInt;
        this.connections = {};
        this.devices = {};
    }

    /**
     * Start function instantiates all connections defined in the config file
     */
    async start() {
        try {
            // Fetch our config
            const ids = await this.fetchIdentities();
            const conf = await this.fetchConfig(ids.uuid!);

            // Create sparkplug node
            this.sparkplugNode = new SparkplugNode(
                this.fplus, ids.sparkplug!, conf.sparkplug);
            log(`Created Sparkplug node "${ids.sparkplug!}".`);

            // Create a new device connection for each type listed in config file
            log('Building up connections and devices...');
            conf.deviceConnections?.forEach(c => this.setupConnection(c));

            // Setup Sparkplug node handlers
            this.setupSparkplug();
        } catch (e: any) {
            log(`Error starting translator: ${e.message}`);
            console.log((e as Error).stack);

            // If the config is giving us errors we'll stop here
            log(`Error starting translator.`);
            await this.stop();
        }
    }

    /**
     * Stop function stops all devices, connections, and clients in preparation for destruction
     */
    async stop(kill: Boolean = false) {
        log('Waiting for devices to stop...');
        await Promise.all(
            Object.values(this.devices)?.map((dev: Device) => {
                log(`Stopping device ${dev._name}`);
                dev.stop();
            })
        );
        log('Waiting for connections to close...');
        await Promise.all(
            Object.values(this.connections)?.map((connection) => {
                log(`Closing connection ${connection._type}`);
                connection.close();
            })
        );
        log('Waiting for sparkplug node to stop...');
        await this.sparkplugNode?.stop();

        log('Stop complete.');

        this.emit('stopped', kill);
    }

    setupSparkplug () {
        const sp = this.sparkplugNode;

        /**
         * What to do when a Sparkplug Birth certificate is requested
         * @param deviceId The Device ID which must produce a birth certificate
         */
        sp.on('dbirth', (deviceId) => {
            log('Handling DBIRTH request for ' + deviceId);
            Object.values(this.devices).find((e: Device) => e._name === deviceId)?._publishDBirth();
        });

        /**
         * What to do when a Sparkplug Birth certificate is requested
         * @param deviceId The Device ID which must produce a birth certificate
         */
        sp.on('dbirth-all', () => {
            log('Publishing DBIRTH request for all devices');
            Object.values(this.devices)?.map((dev: Device) => {
                dev._publishDBirth();
            })
        });

        /**
         * What to do when a Sparkplug Device Command is received
         * @param deviceId The Device ID which must produce a birth certificate
         * @param payload The Sparkplug payload containing DCMD metrics
         */
        sp.on('dcmd', (deviceId, payload) => {
            log('Handling DCMD request for ' + deviceId);
            Object.values(this.devices).find((e: Device) => e._name === deviceId)?._handleDCmd(payload);
        });

        // Listen to the stop event
        sp.on('stop', () => {
            log('Handling stop request for all devices');
            this.stop();
        })
    }

    setupConnection (connection: any): void {
        const cType = connection.connType;
        const deviceInfo = this.chooseDeviceInfo(cType);

        if (deviceInfo == undefined) {
            log(`Failed to find DeviceInfo for connection type '${cType}'`);
            return;
        }

        // Instantiate device connection
        const newConn = this.connections[cType] = new deviceInfo.connection(
            connection.connType,
            connection[deviceInfo.connectionDetails]
        );

        connection.devices?.forEach((devConf: deviceOptions) => {
            this.devices[devConf.deviceId] = new deviceInfo.type(
                this.sparkplugNode, newConn, devConf);
        });

        // What to do when the connection is open
        newConn.on('open', () => {
            connection.devices?.forEach((devConf: deviceOptions) => {
                this.devices[devConf.deviceId]?._deviceConnected();
            })
        });

        // What to do when the device connection has new data from a device
        newConn.on('data', (obj: { [index: string]: any }, parseVals = true) => {
            connection.devices?.forEach((devConf: deviceOptions) => {
                this.devices[devConf.deviceId]?._handleData(obj, parseVals);
            })
        })

        // What to do when device connection dies
        newConn.on('close', () => {
            connection.devices?.forEach((devConf: deviceOptions) => {
                this.devices[devConf.deviceId]?._deviceDisconnected();
            })
        });

        // Open the connection
        newConn.open();
    }

    /* There is a better way to do this. At minimum this should be in a
     * factory class, not the main Translator class. */
    chooseDeviceInfo (connType: string): deviceInfo | undefined {
        // Initialise the connection parameters
        switch (connType) {
            case "REST":
                return {
                    type: RestDevice,
                    connection: RestConnection,
                    connectionDetails: 'RESTConnDetails'
                }
            case "MTConnect":
                return {
                    type: MTConnectDevice,
                    connection: MTConnectConnection,
                    connectionDetails: 'MTConnectConnDetails'
                }
            case "S7":
                return {
                    type: S7Device,
                    connection: S7Connection,
                    connectionDetails: 's7ConnDetails'
                }
            case "OPC UA":
                return {
                    type: OPCUADevice,
                    connection: OPCUAConnection,
                    connectionDetails: 'OPCUAConnDetails'
                }
            case "MQTT":
                return {
                    type: MQTTDevice,
                    connection: MQTTConnection,
                    connectionDetails: 'MQTTConnDetails'
                }
            case "Websocket":
                return {
                    type: WebsocketDevice,
                    connection: WebsocketConnection,
                    connectionDetails: 'WebsocketConnDetails'
                }
            case "UDP":
                return {
                    type: UDPDevice,
                    connection: UDPConnection,
                    connectionDetails: 'UDPConnDetails'

                }
            default:
                return;
        }
    }

    /* Fetch our identities (UUID, Sparkplug) from the Auth service. */
    async fetchIdentities (): Promise<Identity> {
        const auth = this.fplus.Auth;

        const ids = await this.retry("identities", async () => {
            const ids = await auth.find_principal();
            if (!ids || !ids.uuid || !ids.sparkplug) return;
            return ids;
        });

        log(`Found my identities: UUID ${ids.uuid}, Sparkplug ${ids.sparkplug}`);
        return ids;
    }

    /* Fetch our config from the ConfigDB. */
    async fetchConfig (uuid: string): Promise<translatorConf> {
        const cdb = this.fplus.ConfigDB;

        const config = await this.retry("config", async () => {
            const config = await cdb.get_config(EdgeAgentConfig, uuid);
            if (!config || !validateConfig(config)) return;
            return config;
        });

        return reHashConf(config);
    }

    async retry<RV> (what: string, fetch: () => Promise<RV | undefined>): 
        Promise<RV>
    {
        const interval = this.pollInt;

        while (true) {
            log(`Attempting to fetch ${what}...`);
            const rv = await fetch();
            if (rv != undefined) {
                log(`Fetched ${what}.`);
                return rv;
            }

            log(`Failed to fetch ${what}. Trying again in ${interval} seconds...`);
            await timers.setTimeout(interval * 1000);
        }
    }
}
