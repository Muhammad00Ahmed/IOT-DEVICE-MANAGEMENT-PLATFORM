# IoT Device Management Platform

<div align="center">

![Node.js](https://img.shields.io/badge/Node.js-339933?style=for-the-badge&logo=nodedotjs&logoColor=white)
![MQTT](https://img.shields.io/badge/MQTT-660066?style=for-the-badge&logo=mqtt&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=influxdb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

**Comprehensive IoT platform for device management, real-time data collection, analytics, and automation**

[Documentation](#) · [Quick Start](#) · [API Reference](#) · [Contributing](#)

</div>

---

## 🎯 Overview

An enterprise-grade IoT device management platform designed for scalability, security, and real-time data processing. Supports millions of connected devices with MQTT and CoAP protocols, edge computing capabilities, predictive maintenance, remote device control, and comprehensive analytics dashboards.

### Key Features

- 📡 **Device Management**: Register, monitor, and control IoT devices
- 🔄 **Real-Time Data**: MQTT/CoAP protocol support for live telemetry
- 📊 **Analytics Dashboard**: Visualize device data and metrics
- 🤖 **Automation Rules**: Create triggers and actions
- 🔔 **Alerts & Notifications**: Real-time alerts for anomalies
- 🛡️ **Security**: End-to-end encryption and authentication
- 🌐 **Edge Computing**: Process data at the edge
- 📈 **Predictive Maintenance**: ML-based failure prediction
- 🔧 **Remote Control**: OTA updates and configuration
- 📱 **Mobile Apps**: iOS and Android support

---

## ✨ Features

### Device Management

**Device Registration**
- Bulk device provisioning
- Device authentication (X.509, JWT)
- Device groups and tags
- Device metadata management
- Firmware version tracking
- Device lifecycle management

**Device Monitoring**
- Real-time status tracking
- Connection state monitoring
- Battery level monitoring
- Signal strength tracking
- Last seen timestamp
- Device health scores

**Device Control**
- Remote command execution
- Configuration updates
- OTA firmware updates
- Device reboot/reset
- Diagnostic commands
- Batch operations

### Data Collection

**Protocol Support**
- MQTT (Message Queue Telemetry Transport)
- CoAP (Constrained Application Protocol)
- HTTP/HTTPS REST APIs
- WebSocket connections
- AMQP (Advanced Message Queuing Protocol)

**Data Processing**
- Real-time data ingestion
- Data validation and filtering
- Data transformation
- Aggregation and downsampling
- Time-series storage
- Data retention policies

**Edge Computing**
- Edge device support
- Local data processing
- Offline operation
- Data synchronization
- Edge analytics
- Reduced latency

### Analytics & Visualization

**Dashboards**
- Real-time metrics
- Historical data visualization
- Custom widgets
- Multi-device views
- Geolocation maps
- Trend analysis

**Reports**
- Device usage reports
- Performance metrics
- Anomaly detection
- Predictive analytics
- Export to PDF/Excel
- Scheduled reports

**Machine Learning**
- Anomaly detection
- Predictive maintenance
- Pattern recognition
- Failure prediction
- Energy optimization
- Custom ML models

### Automation

**Rules Engine**
- Trigger-based automation
- Conditional logic
- Time-based schedules
- Device state triggers
- Threshold alerts
- Complex workflows

**Actions**
- Send notifications (Email, SMS, Push)
- Execute device commands
- Update device configuration
- Trigger webhooks
- Log events
- Chain multiple actions

**Integrations**
- AWS IoT Core
- Azure IoT Hub
- Google Cloud IoT
- Twilio (SMS)
- SendGrid (Email)
- Slack, Discord
- Custom webhooks

---

## 🛠️ Tech Stack

### Backend

- **Node.js 20** - Runtime environment
- **Express.js** - Web framework
- **MQTT.js** - MQTT client/broker
- **Mosca** - MQTT broker
- **CoAP** - CoAP protocol support
- **MongoDB** - Device metadata
- **InfluxDB** - Time-series data
- **Redis** - Caching and pub/sub
- **Bull** - Job queue

### Frontend

- **React 18** - UI library
- **TypeScript** - Type safety
- **Redux Toolkit** - State management
- **Recharts** - Data visualization
- **Leaflet** - Maps
- **Material-UI** - Components
- **Socket.io Client** - Real-time updates

### Infrastructure

- **Docker** - Containerization
- **Kubernetes** - Orchestration
- **Nginx** - Reverse proxy
- **Prometheus** - Metrics
- **Grafana** - Monitoring
- **ELK Stack** - Logging

### Machine Learning

- **TensorFlow.js** - ML models
- **Python** - Data science
- **scikit-learn** - ML algorithms
- **Pandas** - Data processing

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      IoT Devices                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Sensors  │  │ Actuators│  │  Gateways│  │   Edge   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                    MQTT / CoAP / HTTP
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Protocol Gateway                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  MQTT Broker │ CoAP Server │ HTTP API │ WebSocket   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Message Processing                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Validation │ Transformation │ Routing │ Filtering   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   MongoDB    │    │   InfluxDB   │    │    Redis     │
│  (Metadata)  │    │(Time-series) │    │   (Cache)    │
└──────────────┘    └──────────────┘    └──────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Application Layer                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Rules Engine │ Analytics │ ML Models │ Automation  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Client Applications                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Web App    │  │  Mobile App  │  │     API      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Getting Started

### Prerequisites

- Node.js >= 20.0.0
- MongoDB >= 6.0.0
- InfluxDB >= 2.0.0
- Redis >= 7.0.0
- Docker (optional)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/Muhammad00Ahmed/IOT-DEVICE-MANAGEMENT-PLATFORM.git
cd IOT-DEVICE-MANAGEMENT-PLATFORM
```

2. **Install dependencies**

Backend:
```bash
cd backend
npm install
```

Frontend:
```bash
cd frontend
npm install
```

3. **Environment Configuration**

Backend `.env`:
```env
NODE_ENV=development
PORT=5000

# Database
MONGODB_URI=mongodb://localhost:27017/iot-platform
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your_influxdb_token
INFLUXDB_ORG=your_org
INFLUXDB_BUCKET=iot_data
REDIS_URL=redis://localhost:6379

# MQTT
MQTT_BROKER_PORT=1883
MQTT_WS_PORT=8883

# JWT
JWT_SECRET=your_jwt_secret

# AWS IoT (optional)
AWS_IOT_ENDPOINT=your-endpoint.iot.region.amazonaws.com
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

4. **Start services**

Using Docker:
```bash
docker-compose up -d
```

Or manually:
```bash
# Terminal 1 - Backend
cd backend && npm run dev

# Terminal 2 - Frontend
cd frontend && npm start

# Terminal 3 - MQTT Broker
npm run mqtt-broker
```

5. **Access the application**
- Frontend: http://localhost:3000
- Backend API: http://localhost:5000
- MQTT Broker: mqtt://localhost:1883

---

## 📚 Device Integration

### MQTT Example

```javascript
const mqtt = require('mqtt');

// Connect to MQTT broker
const client = mqtt.connect('mqtt://localhost:1883', {
  clientId: 'device_001',
  username: 'device_token',
  password: 'your_device_token'
});

client.on('connect', () => {
  console.log('Connected to MQTT broker');
  
  // Publish telemetry data
  setInterval(() => {
    const data = {
      temperature: 25.5,
      humidity: 60,
      timestamp: Date.now()
    };
    
    client.publish('devices/device_001/telemetry', JSON.stringify(data));
  }, 5000);
});

// Subscribe to commands
client.subscribe('devices/device_001/commands');

client.on('message', (topic, message) => {
  const command = JSON.parse(message.toString());
  console.log('Received command:', command);
  
  // Execute command
  executeCommand(command);
});
```

### REST API Example

```javascript
// Register device
const response = await fetch('http://localhost:5000/api/devices', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer YOUR_API_TOKEN'
  },
  body: JSON.stringify({
    deviceId: 'device_001',
    name: 'Temperature Sensor',
    type: 'sensor',
    location: {
      latitude: 40.7128,
      longitude: -74.0060
    }
  })
});

// Send telemetry data
await fetch('http://localhost:5000/api/devices/device_001/telemetry', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer YOUR_DEVICE_TOKEN'
  },
  body: JSON.stringify({
    temperature: 25.5,
    humidity: 60
  })
});
```

---

## 📊 Performance

- Supports 1M+ connected devices
- Handles 100K+ messages/second
- < 100ms message latency
- 99.9% uptime SLA
- Horizontal scalability
- Edge computing support

---

## 🔒 Security

- TLS/SSL encryption
- X.509 certificate authentication
- JWT token-based auth
- Role-based access control
- Device whitelisting
- Encrypted data storage
- Audit logging

---

## 🤝 Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md)

---

## 📝 License

MIT License - see [LICENSE](LICENSE)

---

## 👨‍💻 Author

**Muhammad Ahmed**
- GitHub: [@Muhammad00Ahmed](https://github.com/Muhammad00Ahmed)
- Email: mahmedrangila@gmail.com

---

<div align="center">

**⭐ Star this repository if you find it helpful!**

Made with ❤️ by Muhammad Ahmed

</div>