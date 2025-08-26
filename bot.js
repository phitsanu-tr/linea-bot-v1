import 'dotenv/config';
import { ethers } from 'ethers';
import fs from 'fs';
import path from 'path';
import { EventEmitter } from 'events';
import fetch from 'node-fetch';
import express from 'express';

//
// === CONFIG SECTION ===
//

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const SAFE_WALLET = process.env.SAFE_WALLET;
const WS_RPC_URLS = process.env.WS_RPC_URLS?.split(',').map(s => s.trim());
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const HEALTH_PORT = process.env.HEALTH_PORT || 3000;
const DEBUG = process.env.DEBUG === 'true'; // Production debug control

if (!PRIVATE_KEY || !SAFE_WALLET || !WS_RPC_URLS || WS_RPC_URLS.length === 0) {
  console.error('Missing env variables: PRIVATE_KEY, SAFE_WALLET, WS_RPC_URLS');
  process.exit(1);
}

// Load token list from tokens.json file
const TOKENS_PATH = path.resolve('./tokens.json');
if (!fs.existsSync(TOKENS_PATH)) {
  console.error('Missing tokens.json file');
  process.exit(1);
}
const tokens = JSON.parse(fs.readFileSync(TOKENS_PATH, 'utf8'));

//
// === HEALTH MONITORING ===
//

const metrics = {
  startTime: Date.now(),
  totalTransactions: 0,
  successfulTx: 0,
  failedTx: 0,
  lastTxTime: null,
  lastHeartbeat: Date.now(),
  rpcFailures: 0,
  wsReconnections: 0,
  consecutiveFailures: 0,
  processingLockResets: 0
};

function updateMetrics(type, success = true) {
  switch (type) {
    case 'transaction':
      metrics.totalTransactions++;
      if (success) {
        metrics.successfulTx++;
        metrics.consecutiveFailures = 0;
      } else {
        metrics.failedTx++;
        metrics.consecutiveFailures++;
      }
      metrics.lastTxTime = Date.now();
      break;
    case 'rpc_failure':
      metrics.rpcFailures++;
      break;
    case 'reconnection':
      metrics.wsReconnections++;
      break;
    case 'lock_reset':
      metrics.processingLockResets++;
      break;
  }
}

//
// === UTILS ===
//

const LOG_FILE = path.resolve('./bot.log');
function log(...args) {
  const msg = `[${new Date().toISOString()}] ${args.join(' ')}`;
  console.log(msg);
  fs.appendFileSync(LOG_FILE, msg + '\n');
  metrics.lastHeartbeat = Date.now();
}

function debugLog(...args) {
  if (DEBUG) log(...args);
}

function delay(ms) {
  return new Promise(res => setTimeout(res, ms));
}

async function telegramNotify(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message })
    });
  } catch (e) {
    debugLog('Telegram notify failed:', e.message);
  }
}

//
// === MULTI-RPC PROVIDER WITH ERROR RECOVERY ===
//

class MultiRpcProvider extends EventEmitter {
  constructor(urls) {
    super();
    this.urls = urls;
    this.providers = [];
    this.latencies = new Array(urls.length).fill(Infinity);
    this.currentIndex = 0;
    this.healthCheckIntervalMs = 30_000;
    this.healthCheckTimer = null;
    this.reconnectAttempts = new Array(urls.length).fill(0);
    this.maxReconnectAttempts = 5;
    this.init();
  }

  async init() {
    await this.createProviders();
    await this.measureLatencies();
    this.selectBestProvider();
    this.setupListeners();
    this.startHealthCheck();
  }

  async createProviders() {
    this.providers = this.urls.map(url => new ethers.providers.WebSocketProvider(url));
  }

  async reconnectProvider(index) {
    if (this.reconnectAttempts[index] >= this.maxReconnectAttempts) {
      debugLog(`Max reconnect attempts reached for RPC #${index}`);
      return;
    }

    try {
      debugLog(`Reconnecting RPC #${index}...`);
      this.providers[index] = new ethers.providers.WebSocketProvider(this.urls[index]);
      this.setupProviderListeners(index);
      this.reconnectAttempts[index] = 0;
      updateMetrics('reconnection');
      debugLog(`Successfully reconnected RPC #${index}`);
      
      this.emit('reconnected', index);
    } catch (err) {
      this.reconnectAttempts[index]++;
      debugLog(`Failed to reconnect RPC #${index}, attempt ${this.reconnectAttempts[index]}: ${err.message}`);
      setTimeout(() => this.reconnectProvider(index), 5000 * this.reconnectAttempts[index]);
    }
  }

  async measureLatency(index) {
    const p = this.providers[index];
    const start = Date.now();
    try {
      await p.getBlockNumber();
      this.latencies[index] = Date.now() - start;
      this.reconnectAttempts[index] = 0;
    } catch (err) {
      this.latencies[index] = Infinity;
      updateMetrics('rpc_failure');
      this.reconnectProvider(index);
    }
  }

  async measureLatencies() {
    await Promise.all(this.providers.map((_, i) => this.measureLatency(i)));
  }

  selectBestProvider() {
    const minLatency = Math.min(...this.latencies);
    if (minLatency === Infinity) {
      log('⚠️ All RPCs unavailable!');
      return;
    }
    this.currentIndex = this.latencies.indexOf(minLatency);
    debugLog(`Selected RPC #${this.currentIndex} (${this.urls[this.currentIndex]}) latency ${this.latencies[this.currentIndex]}ms`);
    this.emit('providerChanged', this.currentProvider);
  }

  get currentProvider() {
    return this.providers[this.currentIndex];
  }

  setupProviderListeners(index) {
    const p = this.providers[index];
    p._websocket.on('close', async (code) => {
      debugLog(`RPC #${index} websocket closed with code ${code}`);
      await this.measureLatencies();
      this.selectBestProvider();
      this.reconnectProvider(index);
    });
    p._websocket.on('error', async (err) => {
      debugLog(`RPC #${index} websocket error: ${err.message}`);
      updateMetrics('rpc_failure');
      await this.measureLatencies();
      this.selectBestProvider();
    });
  }

  setupListeners() {
    this.providers.forEach((_, i) => this.setupProviderListeners(i));
  }

  async callWithFallback(fn) {
    for (let i = 0; i < this.providers.length; i++) {
      try {
        return await fn(this.providers[i]);
      } catch (err) {
        debugLog(`RPC #${i} call failed: ${err.message}`);
        updateMetrics('rpc_failure');
        if (i === this.providers.length - 1) throw err;
      }
    }
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

async function cleanupContracts() {
  for (const [address, info] of tokenContracts) {
    info.contract.removeAllListeners('Transfer');
  }
  tokenContracts.clear();
  tokenProcessing.clear();
  debugLog('🧹 Cleaned up contracts and reset processing locks');
}

async function setupContracts() {
  await cleanupContracts();

  for (const t of tokens) {
    const contract = new ethers.Contract(t.address, ERC20_ABI, wallet);
    tokenContracts.set(t.address.toLowerCase(), { contract, decimals: t.decimals, symbol: t.symbol });
    tokenProcessing.set(t.address.toLowerCase(), false);

    contract.on('Transfer', (from, to, value) => {
      if (to.toLowerCase() === wallet.address.toLowerCase()) {
        log(`📥 ${t.symbol} ${ethers.utils.formatUnits(value, t.decimals)} from ${from}`);
        enqueueTransfer(t.address);
      }
    });
  }
  debugLog(`✅ Setup ${tokens.length} token contracts with event listeners`);
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
    debugLog(`🔢 Initialized nonce: ${this.nonce}`);
  }

  enqueue(job) {
    this.queue.push(job);
    debugLog(`📋 Enqueued ${job.tokenAddress}, queue: ${this.queue.length}`);
    if (!this.processing) {
      this.process();
    }
  }

  async process() {
    this.processing = true;
    debugLog(`🔄 Processing ${this.queue.length} jobs`);
    
    while (this.queue.length > 0) {
      const job = this.queue.shift();
      await transferWithRetry(job.tokenAddress);
    }
    
    this.processing = false;
    debugLog(`✅ Queue finished`);
  }
}

const txQueue = new TxQueue();

function enqueueTransfer(tokenAddress) {
  const addr = tokenAddress.toLowerCase();
  const isProcessing = tokenProcessing.get(addr);
  
  if (isProcessing) {
    debugLog(`⚠️ ${addr} already processing, skipping`);
    return;
  }
  
  txQueue.enqueue({ tokenAddress: addr });
}

//
// === PROCESSING LOCK MANAGEMENT ===
//

function resetProcessingLocks() {
  let resetCount = 0;
  for (const [address, isProcessing] of tokenProcessing) {
    if (isProcessing) {
      tokenProcessing.set(address, false);
      resetCount++;
    }
  }
  if (resetCount > 0) {
    log(`🔓 Reset ${resetCount} stuck processing locks`);
    updateMetrics('lock_reset');
  }
}

// Reset stuck locks every 5 minutes
setInterval(resetProcessingLocks, 300000);

//
// === TRANSFER WITH RETRY + ERROR RECOVERY ===
//

async function transferWithRetry(tokenAddress) {
  const addr = tokenAddress.toLowerCase();
  
  if (tokenProcessing.get(addr)) {
    debugLog(`⚠️ ${addr} already processing, skipping`);
    return;
  }
  
  tokenProcessing.set(addr, true);

  try {
    const tokenInfo = tokenContracts.get(addr);
    if (!tokenInfo) {
      log(`❌ Token contract not found for ${addr}`);
      return;
    }
    const { contract, decimals, symbol } = tokenInfo;

    // Fast balance check - use current provider directly
    let currentBalance;
    try {
      currentBalance = await contract.balanceOf(wallet.address);
      debugLog(`💰 ${symbol}: ${ethers.utils.formatUnits(currentBalance, decimals)}`);
    } catch (err) {
      // Fallback to multi-provider only on error
      currentBalance = await multiProvider.callWithFallback(async (provider) => {
        const contractWithProvider = new ethers.Contract(addr, ERC20_ABI, new ethers.Wallet(PRIVATE_KEY, provider));
        return await contractWithProvider.balanceOf(wallet.address);
      });
    }

    if (currentBalance.lte(0)) {
      debugLog(`💸 No balance for ${symbol}`);
      return;
    }

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
        // Fast gas price - use current provider first
        try {
          gasPrice = await multiProvider.currentProvider.getGasPrice();
        } catch (err) {
          gasPrice = await multiProvider.callWithFallback(async (provider) => {
            return await provider.getGasPrice();
          });
        }
        gasPrice = gasPrice.mul(120).div(100);

        const gasEstimate = await contract.estimateGas.transfer(SAFE_WALLET, currentBalance, { gasPrice, nonce });

        const unsignedTx = await contract.populateTransaction.transfer(SAFE_WALLET, currentBalance);
        unsignedTx.gasLimit = gasEstimate.mul(120).div(100);
        unsignedTx.gasPrice = gasPrice;
        unsignedTx.nonce = nonce;
        unsignedTx.chainId = await wallet.getChainId();

        const signedTx = await wallet.signTransaction(unsignedTx);

        // Fast send - use current provider first
        let txResponse;
        try {
          txResponse = await multiProvider.currentProvider.sendTransaction(signedTx);
        } catch (err) {
          txResponse = await multiProvider.callWithFallback(async (provider) => {
            return await provider.sendTransaction(signedTx);
          });
        }

        log(`✅ ${symbol} tx: ${txResponse.hash} (${ethers.utils.formatUnits(gasPrice, 'gwei')} gwei)`);

        await txResponse.wait(1);

        log(`✅ Confirmed ${symbol}: ${txResponse.hash}`);

        nonce++;
        txQueue.nonce = nonce;
        updateMetrics('transaction', true);

        await telegramNotify(`✅ ${symbol} ${ethers.utils.formatUnits(currentBalance, decimals)} TX: ${txResponse.hash}`);

        return true;
      } catch (err) {
        debugLog(`⚠️ ${symbol} attempt ${attempt} failed: ${err.message}`);

        if (err.message.includes('nonce too low')) {
          nonce = await wallet.getTransactionCount('pending');
          txQueue.nonce = nonce;
        } else if (err.message.includes('replacement transaction underpriced')) {
          gasPrice = gasPrice.mul(110).div(100);
        } else if (err.message.includes('insufficient funds')) {
          log(`❌ Insufficient funds for gas!`);
          await telegramNotify(`❌ Insufficient funds for gas!`);
          updateMetrics('transaction', false);
          return false;
        }

        gasPrice = gasPrice.mul(110).div(100);
        nonce++;
        txQueue.nonce = nonce;

        await delay(2 ** attempt * 1000);
      }
    }
    
    log(`❌ Failed ${symbol} after ${maxRetries} attempts`);
    updateMetrics('transaction', false);
    
    if (metrics.consecutiveFailures >= 5) {
      await telegramNotify(`🚨 ALERT: ${metrics.consecutiveFailures} consecutive failures!`);
    }
    
    await telegramNotify(`❌ Failed ${symbol} after ${maxRetries} attempts`);
    return false;
    
  } finally {
    tokenProcessing.set(addr, false);
    debugLog(`🔓 Unlocked ${addr}`);
  }
}

//
// === POLLING BALANCES ===
//

async function pollingBalances() {
  for (const t of tokens) {
    try {
      const tokenInfo = tokenContracts.get(t.address.toLowerCase());
      if (!tokenInfo) continue;

      // Fast polling - use current provider first
      let balance;
      try {
        balance = await tokenInfo.contract.balanceOf(wallet.address);
      } catch (err) {
        balance = await multiProvider.callWithFallback(async (provider) => {
          const contractWithProvider = new ethers.Contract(t.address, ERC20_ABI, new ethers.Wallet(PRIVATE_KEY, provider));
          return await contractWithProvider.balanceOf(wallet.address);
        });
      }

      if (balance.gt(0)) {
        debugLog(`⌚ Poll: ${tokenInfo.symbol} ${ethers.utils.formatUnits(balance, tokenInfo.decimals)}`);
        enqueueTransfer(t.address);
      }
    } catch (err) {
      debugLog(`Polling error for ${t.symbol}: ${err.message}`);
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
// === HEALTH MONITORING SERVER ===
//

function startHealthServer() {
  const app = express();

  app.get('/health', (req, res) => {
    const uptime = Date.now() - metrics.startTime;
    const successRate = metrics.totalTransactions > 0 ? 
      (metrics.successfulTx / metrics.totalTransactions * 100).toFixed(2) : 0;

    res.json({
      status: metrics.consecutiveFailures < 5 ? 'healthy' : 'unhealthy',
      uptime: Math.floor(uptime / 1000),
      wallet: wallet?.address,
      rpc: {
        current: multiProvider?.currentIndex,
        latencies: multiProvider?.latencies,
        failures: metrics.rpcFailures,
        reconnections: metrics.wsReconnections
      },
      transactions: {
        total: metrics.totalTransactions,
        successful: metrics.successfulTx,
        failed: metrics.failedTx,
        successRate: `${successRate}%`,
        lastTxTime: metrics.lastTxTime,
        consecutiveFailures: metrics.consecutiveFailures
      },
      queue: {
        size: txQueue.queue.length,
        processing: txQueue.processing
      },
      processing: {
        locks: Array.from(tokenProcessing.entries()).filter(([_, v]) => v).map(([k, _]) => k),
        lockResets: metrics.processingLockResets
      },
      debug: DEBUG,
      lastHeartbeat: metrics.lastHeartbeat
    });
  });

  app.get('/metrics', (req, res) => {
    res.json(metrics);
  });

  app.get('/reset-locks', (req, res) => {
    resetProcessingLocks();
    res.json({ message: 'Processing locks reset', timestamp: Date.now() });
  });

  app.listen(HEALTH_PORT, () => {
    log(`🏥 Health server started on port ${HEALTH_PORT}`);
  });
}

//
// === HEARTBEAT SYSTEM ===
//

function startHeartbeat() {
  setInterval(() => {
    const successRate = metrics.totalTransactions > 0 ? 
      (metrics.successfulTx / metrics.totalTransactions * 100).toFixed(1) : 0;
    
    const activeLocks = Array.from(tokenProcessing.entries()).filter(([_, v]) => v).length;
    
    log(`💓 Queue=${txQueue.queue.length} RPC=#${multiProvider.currentIndex} Success=${successRate}% Locks=${activeLocks}`);
    
    if (metrics.lastTxTime && Date.now() - metrics.lastTxTime > 300000) {
      log(`⚠️ No transactions for 5+ minutes`);
    }
  }, 30000);
}

//
// === START BOT ===
//

(async () => {
  multiProvider = new MultiRpcProvider(WS_RPC_URLS);

  multiProvider.on('providerChanged', async (newProvider) => {
    debugLog(`Provider switched to: ${newProvider.connection.url}`);
    wallet = new ethers.Wallet(PRIVATE_KEY, newProvider);
    await setupContracts();
    txQueue.nonce = null;
  });

  multiProvider.on('reconnected', async (index) => {
    debugLog(`RPC #${index} reconnected, re-setting up contracts...`);
    await setupContracts();
  });

  wallet = new ethers.Wallet(PRIVATE_KEY, multiProvider.currentProvider);
  await setupContracts();
  await txQueue.initNonce();

  startHealthServer();
  startHeartbeat();

  log(`🚀 Bot started on wallet: ${wallet.address}`);
  log(`🏥 Health: http://localhost:${HEALTH_PORT}/health`);
  if (DEBUG) log(`🔍 Debug mode enabled`);

  startPolling();

  process.on('SIGINT', async () => {
    log('Received SIGINT, exiting...');
    await cleanupContracts();
    multiProvider.stopHealthCheck();
    process.exit();
  });
  process.on('SIGTERM', async () => {
    log('Received SIGTERM, exiting...');
    await cleanupContracts();
    multiProvider.stopHealthCheck();
    process.exit();
  });
})();