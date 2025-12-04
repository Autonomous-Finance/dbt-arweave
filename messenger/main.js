/**
 * AO Messenger - Queries analytics data and sends it to an AO process
 * 
 * This script:
 * 1. Queries aggregated metrics from the ClickHouse database
 * 2. Sends the data to a specified AO process for on-chain storage
 * 
 * Environment variables required:
 * - CLICKHOUSE_HOST: Database host
 * - CLICKHOUSE_USER: Database username
 * - CLICKHOUSE_PASSWORD: Database password
 * - WALLET_JSON: Arweave wallet JSON for signing
 * - AGGREGATOR_PROCESS_ID: Target AO process ID
 */

const { createDataItemSigner, message } = require("@permaweb/aoconnect");
const { ClickHouse } = require('clickhouse');

async function queryClickHouse() {
  const clickhouse = new ClickHouse({
    url: process.env.CLICKHOUSE_HOST,
    port: 8123,
    debug: false,
    basicAuth: {
      username: process.env.CLICKHOUSE_USER,
      password: process.env.CLICKHOUSE_PASSWORD,
    },
  });

  const query = `SELECT * 
    FROM prod.ao_metrics
    LEFT JOIN prod.ao_rolling_metrics USING (created_date);`;
  const result = await clickhouse.query(query).toPromise();
  return result;
}

async function sendMessage(processId, msgData) {
  try {
    const wallet = JSON.parse(process.env.WALLET_JSON);

    const response = await message({
      process: processId,
      tags: [
        { name: 'Action', value: 'Update-Stats' },
      ],
      signer: createDataItemSigner(wallet),
      data: msgData,
    });
    console.log('Message sent successfully:', response);
  } catch (error) {
    console.error('Failed to send message:', error);
    throw error;
  }
}

async function main() {
  try {
    const data = await queryClickHouse();
    const msgData = JSON.stringify(data);
    await sendMessage(process.env.AGGREGATOR_PROCESS_ID, msgData);
  } catch (error) {
    console.error('An error occurred:', error);
    process.exit(1);
  }
}

main();
