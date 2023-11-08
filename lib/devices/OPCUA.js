/*
 *  Factory+ / AMRC Connectivity Stack (ACS) Edge component
 *  Copyright 2023 AMRC
 */
import { Device, DeviceConnection } from "../device.js";
import { log } from "../helpers/log.js";
import { getOpcSecurityMode, getOpcSecurityPolicy, OPCUADataType } from "../helpers/typeHandler.js";
import { AttributeIds, ClientSubscription, OPCUAClient, resolveNodeId, TimestampsToReturn, UserTokenType } from "node-opcua";
export class OPCUAConnection extends DeviceConnection {
    #subscription;
    #subscriptionOptions;
    #client;
    #options;
    #credentials;
    #endpointUrl;
    #session;
    constructor(type, connDetails) {
        super(type);
        this.#subscription = new ClientSubscription();
        this.#subscriptionOptions = {
            maxNotificationsPerPublish: 1000,
            publishingEnabled: true,
            requestedLifetimeCount: 100,
            requestedMaxKeepAliveCount: 10,
            requestedPublishingInterval: 1000,
        };
        // Set credentials if specified
        if (connDetails.useCredentials) {
            this.#credentials = {
                userName: connDetails.username,
                password: connDetails.password,
                type: UserTokenType.UserName
            };
        }
        else {
            this.#credentials = {
                type: UserTokenType.Anonymous
            };
        }
        this.#options = {
            applicationName: `acs-edge`,
            keepSessionAlive: true,
            connectionStrategy: {
                initialDelay: 1000,
                maxDelay: 20000
            },
            securityMode: getOpcSecurityMode(connDetails.securityMode),
            securityPolicy: getOpcSecurityPolicy(connDetails.securityPolicy),
            endpointMustExist: false
        };
        this.#client = OPCUAClient.create(this.#options);
        this.#endpointUrl = connDetails.endpoint;
        this.#client.on("start_reconnection", function () {
            log("Trying to reconnect to OPC UA server...");
        });
        this.#client.on("connecting", function () {
            log("Trying to connect to OPC UA server...");
        });
        this.#client.on("backoff", function (nb, delay) {
            console.log(`Connection to the OPC UA server failed for the ${nb} time. Retrying in ${delay} ms`);
        });
        this.#session = null;
    }
    async open() {
        try {
            await this.#client.connect(this.#endpointUrl);
            this.#session = await this.#client.createSession(this.#credentials);
            this.emit('open');
            log(`OPC UA connected to ${this.#endpointUrl}`);
        }
        catch (e) {
            console.log(e);
        }
    }
    readMetrics(metrics, payloadFormat) {
        const oldVals = [];
        let items = [];
        for (let i = 0; i < metrics.length; i++) {
            const metric = metrics.array[i];
            // Only add the metrics to the array to monitor if they have an address, otherwise the node-opcua library will throw an error
            if (typeof metric.properties !== "undefined" && metric.properties.address?.value) {
                oldVals.push(metric.value);
                items.push({
                    nodeId: metric.properties.address.value,
                    attributeId: AttributeIds.Value
                });
            }
        }
        if (this.#session) {
            this.#session.read(items, 0, (err, dataValues) => {
                if (err) {
                    console.log(err);
                }
                else if (typeof dataValues !== "undefined") {
                    for (let i = 0; i < dataValues.length; i++) {
                        metrics.array[i].value = dataValues[i].value.value;
                        metrics.array[i].timestamp = Date.now();
                    }
                }
            });
        }
        return oldVals;
    }
    async writeMetrics(metrics, writeCallback, payloadFormat, delimiter) {
        let items = await Promise.all(metrics.array.map(metric => {
            let obj = {};
            if (typeof metric.properties !== "undefined") {
                obj = {
                    nodeId: metric.properties.address.value,
                    attributeId: AttributeIds.Value,
                    value: {
                        value: {
                            dataType: OPCUADataType[metric.type],
                            value: metric.value
                        }
                    }
                };
            }
            return obj;
        }));
        let isErr = false;
        const statusCodes = this.#session ? await this.#session.write(items) : [{
                value: -1,
                description: "No OPC UA client session"
            }];
        for (let i = 0; i < statusCodes.length; i++) {
            if (statusCodes[i].value !== 0) {
                isErr = true;
                writeCallback(new Error(statusCodes[i].description));
                break;
            }
        }
        if (!isErr)
            writeCallback();
    }
    startSubscription(metrics, payloadFormat, delimiter, interval, deviceId, subscriptionStartCallback) {
        this.#subscriptionOptions.requestedPublishingInterval = interval;
        if (this.#session) {
            this.#session.createSubscription2(this.#subscriptionOptions, (err, subscription) => {
                if (typeof subscription !== "undefined") {
                    this.#subscription = subscription;
                    this.#subscription
                        .on("started", () => {
                        log("OPC UA metric subscription started - subscriptionId=" +
                            this.#subscription.subscriptionId);
                    })
                        .on("keepalive", function () {
                        // log("OPC UA subscription keepalive");
                    })
                        .on("terminated", function () {
                        log("OPC UA subscription terminated");
                    });
                    const itemsToMonitor = [];
                    metrics.addresses.forEach(addr => {
                        if (addr !== 'undefined') {
                            itemsToMonitor.push({
                                nodeId: resolveNodeId(addr),
                                attributeId: AttributeIds.Value
                            });
                        }
                    });
                    this.#subscription.monitorItems(itemsToMonitor, {
                        samplingInterval: interval,
                        discardOldest: false,
                        queueSize: 1000
                    }, TimestampsToReturn.Both);
                    this.#subscription.on("received_notifications", (msg) => {
                        // log(msg.toString())
                        if (msg.notificationData) {
                            msg.notificationData.forEach(dataChangeNotification => {
                                let changedMetrics = {};
                                dataChangeNotification.monitoredItems.forEach((monitoredItemNotification) => {
                                    const monitoredItem = this.#subscription.monitoredItems[monitoredItemNotification.clientHandle];
                                    const nodeId = monitoredItem.itemToMonitor.nodeId.toString();
                                    changedMetrics[nodeId] = monitoredItemNotification.value.value.value;
                                });
                                this.emit('data', changedMetrics, false);
                            });
                        }
                    });
                }
            });
            log(`Subscription created for ${metrics.length} tags on ${this._type}`);
        }
    }
    async close() {
        try {
            if (this.#session) {
                await this.#session.close();
            }
            await this.#client.disconnect();
            this.emit('close');
        }
        catch (e) {
            console.log(e);
        }
    }
}
export class OPCUADevice extends (Device) {
    devConn;
    constructor(spClient, devConn, options) {
        super(spClient, devConn, options);
        this.devConn = devConn;
        this._metrics.add(options.metrics);
        try {
            this.devConn.on('open', () => {
                this._isConnected = true;
                log(`${this._name} ready`);
            });
            // this.devConn.on('asyncData', (changedMetrics: changedMetricType) => {
            //   let updatedMetrics:sparkplugMetric[] = [];
            //   for (const addr in changedMetrics) {
            //     this._metrics.setValueByAddrPath(addr, '', changedMetrics[addr]);
            //     updatedMetrics.push(this._metrics.getByAddrPath(addr, ''));
            //   }
            //   this.onConnData(updatedMetrics);
            // })
        }
        catch (e) {
            console.log(e);
        }
    }
}
