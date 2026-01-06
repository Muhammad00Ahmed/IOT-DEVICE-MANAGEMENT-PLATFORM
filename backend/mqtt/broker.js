const mqtt = require('mqtt');
const { Influx } = require('influx');
const mongoose = require('mongoose');
const EventEmitter = require('events');

class MQTTBroker extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.clients = new Map();
    this.subscriptions = new Map();
    
    // Initialize InfluxDB for time-series data
    this.influx = new Influx.InfluxDB({
      host: config.influxdb.host,
      database: config.influxdb.database,
      schema: [
        {
          measurement: 'device_telemetry',
          fields: {
            temperature: Influx.FieldType.FLOAT,
            humidity: Influx.FieldType.FLOAT,
            pressure: Influx.FieldType.FLOAT,
            battery: Influx.FieldType.FLOAT
          },
          tags: ['device_id', 'device_type', 'location']
        }
      ]
    });
  }

  async start() {
    // Create MQTT broker
    const mosca = require('mosca');
    
    const moscaSettings = {
      port: this.config.mqtt.port,
      backend: {
        type: 'redis',
        redis: require('redis'),
        host: this.config.redis.host,
        port: this.config.redis.port
      },
      persistence: {
        factory: mosca.persistence.Redis,
        host: this.config.redis.host,
        port: this.config.redis.port
      }
    };

    this.broker = new mosca.Server(moscaSettings);

    this.broker.on('ready', () => {
      console.log('✅ MQTT Broker is running on port', this.config.mqtt.port);
      this.setupEventHandlers();
    });

    // Create InfluxDB database if not exists
    try {
      const databases = await this.influx.getDatabaseNames();
      if (!databases.includes(this.config.influxdb.database)) {
        await this.influx.createDatabase(this.config.influxdb.database);
        console.log('✅ InfluxDB database created');
      }
    } catch (error) {
      console.error('❌ InfluxDB error:', error);
    }
  }

  setupEventHandlers() {
    // Client connected
    this.broker.on('clientConnected', (client) => {
      console.log('📱 Device connected:', client.id);
      this.clients.set(client.id, {
        id: client.id,
        connectedAt: new Date(),
        lastSeen: new Date()
      });
      
      this.emit('device-connected', client.id);
    });

    // Client disconnected
    this.broker.on('clientDisconnected', (client) => {
      console.log('📴 Device disconnected:', client.id);
      this.clients.delete(client.id);
      this.emit('device-disconnected', client.id);
    });

    // Message published
    this.broker.on('published', async (packet, client) => {
      if (!client) return;

      const topic = packet.topic;
      const message = packet.payload.toString();

      // Update last seen
      const clientInfo = this.clients.get(client.id);
      if (clientInfo) {
        clientInfo.lastSeen = new Date();
      }

      // Handle telemetry data
      if (topic.includes('/telemetry')) {
        await this.handleTelemetry(client.id, topic, message);
      }

      // Handle device status
      if (topic.includes('/status')) {
        await this.handleStatus(client.id, topic, message);
      }

      // Handle device events
      if (topic.includes('/events')) {
        await this.handleEvent(client.id, topic, message);
      }
    });

    // Client subscribed
    this.broker.on('subscribed', (topic, client) => {
      console.log('📬 Device subscribed:', client.id, 'to', topic);
      
      if (!this.subscriptions.has(client.id)) {
        this.subscriptions.set(client.id, new Set());
      }
      this.subscriptions.get(client.id).add(topic);
    });

    // Client unsubscribed
    this.broker.on('unsubscribed', (topic, client) => {
      console.log('📭 Device unsubscribed:', client.id, 'from', topic);
      
      const subs = this.subscriptions.get(client.id);
      if (subs) {
        subs.delete(topic);
      }
    });
  }

  async handleTelemetry(deviceId, topic, message) {
    try {
      const data = JSON.parse(message);
      
      // Store in InfluxDB
      await this.influx.writePoints([
        {
          measurement: 'device_telemetry',
          tags: {
            device_id: deviceId,
            device_type: data.type || 'unknown',
            location: data.location || 'unknown'
          },
          fields: {
            temperature: data.temperature || 0,
            humidity: data.humidity || 0,
            pressure: data.pressure || 0,
            battery: data.battery || 100
          },
          timestamp: new Date(data.timestamp || Date.now())
        }
      ]);

      // Emit event for real-time processing
      this.emit('telemetry', {
        deviceId,
        data,
        timestamp: new Date()
      });

      // Check for anomalies
      await this.checkAnomalies(deviceId, data);

    } catch (error) {
      console.error('Error handling telemetry:', error);
    }
  }

  async handleStatus(deviceId, topic, message) {
    try {
      const status = JSON.parse(message);
      
      // Update device status in MongoDB
      await Device.findOneAndUpdate(
        { deviceId },
        {
          $set: {
            status: status.state,
            lastSeen: new Date(),
            battery: status.battery,
            signalStrength: status.signal
          }
        }
      );

      this.emit('status-update', {
        deviceId,
        status,
        timestamp: new Date()
      });

    } catch (error) {
      console.error('Error handling status:', error);
    }
  }

  async handleEvent(deviceId, topic, message) {
    try {
      const event = JSON.parse(message);
      
      // Store event in MongoDB
      await DeviceEvent.create({
        deviceId,
        eventType: event.type,
        data: event.data,
        severity: event.severity || 'info',
        timestamp: new Date(event.timestamp || Date.now())
      });

      this.emit('device-event', {
        deviceId,
        event,
        timestamp: new Date()
      });

      // Trigger alerts if needed
      if (event.severity === 'critical' || event.severity === 'error') {
        await this.triggerAlert(deviceId, event);
      }

    } catch (error) {
      console.error('Error handling event:', error);
    }
  }

  async checkAnomalies(deviceId, data) {
    // Simple anomaly detection
    const thresholds = {
      temperature: { min: -10, max: 50 },
      humidity: { min: 0, max: 100 },
      battery: { min: 10, max: 100 }
    };

    for (const [key, value] of Object.entries(data)) {
      if (thresholds[key]) {
        if (value < thresholds[key].min || value > thresholds[key].max) {
          await this.triggerAlert(deviceId, {
            type: 'anomaly',
            message: `${key} out of range: ${value}`,
            severity: 'warning'
          });
        }
      }
    }
  }

  async triggerAlert(deviceId, event) {
    // Create alert
    await Alert.create({
      deviceId,
      type: event.type,
      message: event.message,
      severity: event.severity,
      timestamp: new Date()
    });

    // Emit alert event
    this.emit('alert', {
      deviceId,
      event,
      timestamp: new Date()
    });

    console.log('🚨 Alert triggered for device:', deviceId, event.message);
  }

  // Send command to device
  async sendCommand(deviceId, command) {
    const topic = `devices/${deviceId}/commands`;
    const message = JSON.stringify(command);
    
    this.broker.publish({
      topic,
      payload: message,
      qos: 1,
      retain: false
    });

    console.log('📤 Command sent to device:', deviceId, command);
  }

  // Get connected devices
  getConnectedDevices() {
    return Array.from(this.clients.values());
  }

  // Get device subscriptions
  getDeviceSubscriptions(deviceId) {
    return Array.from(this.subscriptions.get(deviceId) || []);
  }
}

module.exports = MQTTBroker;