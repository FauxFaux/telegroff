import { Client, Client as Pg } from 'pg';
import mqttMod from 'mqtt';

export async function main() {
  const mqtt = await mqttMod.connectAsync(process.env.MQTT_HOST!);
  const client = new Pg({
    host: process.env.PG_HOST,
    user: 'tsdb_write',
    database: 'tsdb',
    password: process.env.PG_PASSWORD,
  });
  await client.connect();
  await mqtt.subscribeAsync(['zigbee2mqtt/+', 'tele/+/+']);
  const handlers = { tasmota, zigbee2Mqtt } as const;
  const messages: [string, Buffer][] = [];
  mqtt.on('message', (topic: string, message: Buffer) => {
    messages.push([topic, message]);
  });

  for (;;) {
    if (messages.length === 0) {
      await sleep(1000);
      continue;
    }

    const taken = [...messages];
    messages.length = 0;

    const ops: Op[] = [];

    for (const [topic, message] of taken) {
      for (const [handlerName, handler] of Object.entries(handlers)) {
        handler(topic, message, (op: Op) => ops.push(op));
      }
    }

    for (const op of ops) {
      await op(client);
    }
  }
}

type Op = (client: Client) => Promise<unknown>;
type Handler = (topic: string, message: Buffer, op: (op: Op) => void) => void;

const zigbee2Mqtt: Handler = (topic, message, op) => {
  if (!topic.startsWith('zigbee2mqtt/')) {
    return;
  }

  const text = message.toString('utf-8');
  const [, deviceName] = topic.split('/');
  if (deviceName.startsWith('therm-')) {
    const data = JSON.parse(text);
    op(async (client) =>
      client.query(
        'insert into ambient_temp (time, sensor, temperature, humidity) values (now(), $1, $2, $3)',
        [deviceName.slice('therm-'.length), data.temperature, data.humidity],
      ),
    );
    return;
  }

  console.warn(`Unknown zigbee2mqtt device: ${deviceName}`);
};

const tasmota: Handler = (topic, message, op) => {
  if (!topic.startsWith('tele/tasmota_')) {
    return;
  }

  const text = message.toString('utf-8');
  // e.g. "Offline" (unquoted)
  if (!text.startsWith('{')) return;
  const data = JSON.parse(text);
  const [, deviceName, kind] = topic.split('/');
  const deviceId = deviceName.slice('tasmota_'.length);
  switch (kind) {
    case 'SENSOR':
      {
        const {
          Time,
          ENERGY: { Total, Power, ApparentPower, ReactivePower, Voltage },
        } = data;
        op(async (client) =>
          client.query(
            /*
             create table power (time timestamptz not null, device_id text not null,
               total numeric not null, power numeric not null, apparent_power numeric not null,
               reactive_power numeric not null, voltage numeric not null)
               WITH (tsdb.hypertable, tsdb.partition_column='time', tsdb.segmentby='device_id', tsdb.orderby='time DESC');
            */
            'insert into power (time, device_id, total,' +
              ' power, apparent_power, reactive_power, voltage)' +
              ' values ($1, $2, $3, $4, $5, $6, $7)',
            [
              Time,
              deviceId,
              Total,
              Power,
              ApparentPower,
              ReactivePower,
              Voltage,
            ],
          ),
        );
      }
      break;
    case 'STATE':
      {
        const {
          Time,
          Wifi: { RSSI, Signal, BSSId },
        } = data;
        op(async (client) =>
          /*
           create table wifi (time timestamptz not null, device_id text not null,
             rssi integer not null, signal integer not null, bssid text not null)
             WITH (tsdb.hypertable, tsdb.partition_column='time', tsdb.segmentby='device_id', tsdb.orderby='time DESC');
          */
          client.query(
            'insert into wifi (time, device_id, rssi,' +
              ' signal, bssid) values ($1, $2, $3, $4, $5)',
            [Time, deviceId, RSSI, Signal, BSSId],
          ),
        );
      }
      break;
    default:
      console.warn(`Unknown Tasmota message kind: ${kind}`);
  }
};

const sleep = async (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));
