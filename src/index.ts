import { Client as Pg } from 'pg';
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
  await mqtt.subscribeAsync('zigbee2mqtt/+');
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

    const therms: [string, number, number][] = [];
    for (const [topic, message] of taken) {
      const text = message.toString('utf-8');
      const [, deviceName] = topic.split('/');
      if (deviceName.startsWith('therm-')) {
        const data = JSON.parse(text);
        therms.push([
          deviceName.slice('therm-'.length),
          data.temperature,
          data.humidity,
        ]);
      } else {
        console.warn(`Unknown device: ${deviceName}`);
        continue;
      }
    }
    for (const therm of therms) {
      await client.query(
        'insert into ambient_temp (time, sensor, temperature, humidity) values (now(), $1, $2, $3)',
        therm,
      );
    }
  }
}

const sleep = async (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));
