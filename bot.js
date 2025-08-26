import 'dotenv/config';
import { ethers } from 'ethers';
import fs from 'fs';
import path from 'path';
import { EventEmitter } from 'events';
import fetch from 'node-fetch'; // For Telegram notifications (if needed)

//
// === CONFIG SECTION ===
//

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const SAFE_WALLET = process.env.SAFE_WALLET;
const WS_RPC_URLS = process.env.WS_RPC_URLS?.split(',').map(s => s.trim());
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

if (!PRIVATE_KEY || !SAFE_WALLET || !WS_RPC_URLS || WS_RPC_URLS.length === 0) {
  console.error('Missing env variables: PRIVATE_KEY, SAFE_WALLET, WS_RPC_URLS');
  process.exit(1);
}

// Load token list from tokens.json file (the file must be in the same folder as bot.js)
const TOKENS_PATH = path.resolve('./tokens.json');
if (!fs.existsSync(TOKENS_PATH)) {
  console.error('Missing tokens.json file');
  process.exit(1);
}
const tokens = JSON.parse(fs.readFileSync(TOKENS_PATH, 'utf8'));

//
// === UTILS ===
//

// Logging (console + file)
const LOG_FILE = path.resolve('./bot.log');
function log(...args) {
  const msg = `[${new Date().toISOString()}] ${args.join(' ')}`;
  console.log(msg);
  fs.appendFileSync(LOG_FILE, msg + '\n');
}

// Delay helper
function delay(ms) {
  return new Promise(res => setTimeout(res, ms));
}

// Telegram notify
async function telegramNotify(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message })
    });
  } catch (e) {
    log('Telegram notify failed:', e.message);
  }
}

//
// === MULTI-RPC PROVIDER WITH LATENCY & AUTO-SWITCH ===
//

class MultiRpcProvider extends EventEmitter {
  constructor(urls) {
    super();
    this.urls = urls;
    this.providers = urls.map(url => new ethers.providers.WebSocketProvider(url));
    this.latencies = new Array(urls.length).fill(Infinity);
    this.currentIndex = 0;
    this.healthCheckIntervalMs = 30_000;
    this.healthCheckTimer = null;
    this.init();
  }

  async init() {
    await this.measureLatencies();
    this.selectBestProvider();
    this.setupListeners();
    this.startHealthCheck();
  }

  async measureLatency(index) {
    const p = this.providers[index];
    const start = Date.now();
    try {
      await p.getBlockNumber();
      this.latencies[index] = Date.now() - start;
    } catch {
      this.latencies[index] = Infinity;
    }
  }

  async measureLatencies() {
    await Promise.all(this.providers.map((_, i) => this.measureLatency(i)));
  }

  selectBestProvider() {
    const minLatency = Math.min(...this.latencies);
    this.currentIndex = this.latencies.indexOf(minLatency);
    log(`Selected RPC #${this.currentIndex} (${this.urls[this.currentIndex]}) latency ${this.latencies[this.currentIndex]}ms`);
    this.emit('providerChanged', this.currentProvider);
  }

  get currentProvider() {
    return this.providers[this.currentIndex];
  }

  setupListeners() {
    this.providers.forEach((p, i) => {
      p._websocket.on('close', async (code) => {
        log(`RPC #${i} websocket closed with code ${code}. Re-selecting provider...`);
        await this.measureLatencies();
        this.selectBestProvider();
      });
      p._websocket.on('error', async (err) => {
        log(`RPC #${i} websocket error: ${err.message}. Re-selecting provider...`);
        await this.measureLatencies();
        this.selectBestProvider();
      });
    });
  }

  startHealthCheck() {
    this.healthCheckTimer = setInterval(async () => {
      await this.measureLatencies();
      this.selectBestProvider();
    }, this.healthCheckIntervalMs);
  }

  stopHealthCheck() {
    clearInterval(this.healthCheckTimer);
  }
}

//
// === TOKEN CONTRACT SETUP ===
//

const ERC20_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)",
  "function decimals() view returns (uint8)",
  "event Transfer(address indexed from, address indexed to, uint256 value)"
];

let wallet;
let multiProvider;
const tokenContracts = new Map();
const tokenProcessing = new Map();

async function setupContracts() {
  tokenContracts.clear();
  tokenProcessing.clear();

  for (const t of tokens) {
    const contract = new ethers.Contract(t.address, ERC20_ABI, wallet);
    tokenContracts.set(t.address.toLowerCase(), { contract, decimals: t.decimals, symbol: t.symbol });
    tokenProcessing.set(t.address.toLowerCase(), false);

    contract.removeAllListeners('Transfer');
    contract.on('Transfer', (from, to, value) => {
      if (to.toLowerCase() === wallet.address.toLowerCase()) {
        log(`📥 Transfer event detected: ${t.symbol} from ${from}, amount: ${ethers.utils.formatUnits(value, t.decimals)}`);
        enqueueTransfer(t.address, value);
      }
    });
  }
}

//
// === PRIORITY QUEUE + NONCE MANAGEMENT ===
//

class TxQueue {
  constructor() {
    this.queue = [];
    this.processing = false;
    this.nonce = null;
  }

  async initNonce() {
    this.nonce = await wallet.getTransactionCount('pending');
  }

  enqueue(job) {
    this.queue.push(job);
    if (!this.processing) {
      this.process();
    }
  }

  async process() {
    this.processing = true;
    while (this.queue.length > 0) {
      const job = this.queue.shift();
      await transferWithRetry(job.tokenAddress, job.amount);
    }
    this.processing = false;
  }
}

const txQueue = new TxQueue();

function enqueueTransfer(tokenAddress, amount) {
  txQueue.enqueue({ tokenAddress: tokenAddress.toLowerCase(), amount });
}

//
// === TRANSFER WITH RETRY, ADAPTIVE GAS, PRE-SIGNED TX ===
//

async function transferWithRetry(tokenAddress, amount) {
  if (tokenProcessing.get(tokenAddress)) return;
  tokenProcessing.set(tokenAddress, true);

  const tokenInfo = tokenContracts.get(tokenAddress);
  if (!tokenInfo) {
    log(`Token contract not found for ${tokenAddress}`);
    tokenProcessing.set(tokenAddress, false);
    return;
  }
  const { contract, decimals, symbol } = tokenInfo;

  if (!txQueue.nonce) {
    await txQueue.initNonce();
  }

  let gasPrice;
  let nonce = txQueue.nonce;
  const maxRetries = 5;
  let attempt = 0;

  while (attempt < maxRetries) {
    attempt++;
    try {
      gasPrice = await multiProvider.currentProvider.getGasPrice();
      gasPrice = gasPrice.mul(120).div(100); // +20% buffer

      const gasEstimate = await contract.estimateGas.transfer(SAFE_WALLET, amount, { gasPrice, nonce });

      const unsignedTx = await contract.populateTransaction.transfer(SAFE_WALLET, amount);
      unsignedTx.gasLimit = gasEstimate.mul(120).div(100);
      unsignedTx.gasPrice = gasPrice;
      unsignedTx.nonce = nonce;

      const signedTx = await wallet.signTransaction(unsignedTx);

      const txResponse = await multiProvider.currentProvider.sendTransaction(signedTx);
      log(`✅ Sent ${symbol} tx: ${txResponse.hash} attempt ${attempt} nonce ${nonce} gasPrice ${ethers.utils.formatUnits(gasPrice, 'gwei')} gwei`);

      await txResponse.wait(1);

      log(`✅ Confirmed ${symbol} transfer tx: ${txResponse.hash}`);

      nonce++;
      txQueue.nonce = nonce;
      tokenProcessing.set(tokenAddress, false);

      await telegramNotify(`✅ Transfer success: ${symbol} ${ethers.utils.formatUnits(amount, decimals)} TX: ${txResponse.hash}`);

      return true;
    } catch (err) {
      log(`⚠️ Transfer attempt ${attempt} failed for ${symbol}: ${err.message}`);

      // Handle specific errors for nonce/gas
      if (err.message.includes('nonce too low')) {
        nonce = await wallet.getTransactionCount('pending');
        txQueue.nonce = nonce;
      } else if (err.message.includes('replacement transaction underpriced')) {
        gasPrice = gasPrice.mul(110).div(100);
      } else if (err.message.includes('insufficient funds')) {
        log(`❌ Insufficient funds to send gas fee!`);
        await telegramNotify(`❌ Insufficient funds for gas!`);
        tokenProcessing.set(tokenAddress, false);
        return false;
      }

      gasPrice = gasPrice.mul(110).div(100); // เพิ่ม gas price 10%
      nonce++;
      txQueue.nonce = nonce;

      await delay(2 ** attempt * 1000); // Exponential backoff
    }
  }
  log(`❌ Failed to transfer ${symbol} after ${maxRetries} attempts`);
  tokenProcessing.set(tokenAddress, false);
  await telegramNotify(`❌ Failed to transfer ${symbol} after ${maxRetries} attempts`);
  return false;
}

//
// === POLLING BALANCES + HEALTH CHECK ===
//

async function pollingBalances() {
  for (const t of tokens) {
    try {
      const tokenInfo = tokenContracts.get(t.address.toLowerCase());
      if (!tokenInfo) continue;

      const balance = await tokenInfo.contract.balanceOf(wallet.address);
      if (balance.gt(0)) {
        log(`⌚ Poll detected ${tokenInfo.symbol} balance: ${ethers.utils.formatUnits(balance, tokenInfo.decimals)}`);
        enqueueTransfer(t.address, balance);
      }
    } catch (err) {
      log(`Polling error for ${t.symbol}: ${err.message}`);
    }
  }
}

async function startPolling() {
  while (true) {
    await pollingBalances();
    await delay(1000);
  }
}

//
// === START BOT ===
//

(async () => {
  multiProvider = new MultiRpcProvider(WS_RPC_URLS);

  multiProvider.on('providerChanged', (newProvider) => {
    log(`Provider switched to: ${newProvider.connection.url}`);
    wallet = new ethers.Wallet(PRIVATE_KEY, newProvider);
    setupContracts().catch(e => log('Setup contracts error:', e.message));
    txQueue.nonce = null; // reset nonce cache
  });

  // Init wallet and contracts first
  wallet = new ethers.Wallet(PRIVATE_KEY, multiProvider.currentProvider);
  await setupContracts();

  // Init nonce cache
  await txQueue.initNonce();

  log(`🚀 Bot started on wallet: ${wallet.address}`);

  // Start polling loop
  startPolling();

  // Optional: Setup graceful shutdown on signals
  process.on('SIGINT', () => {
    log('Received SIGINT, exiting...');
    multiProvider.stopHealthCheck();
    process.exit();
  });
  process.on('SIGTERM', () => {
    log('Received SIGTERM, exiting...');
    multiProvider.stopHealthCheck();
    process.exit();
  });
})();