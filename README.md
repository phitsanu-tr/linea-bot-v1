# ⚡ Linea Bot

![Node.js](https://img.shields.io/badge/Node.js-20.x-green)
![License](https://img.shields.io/github/license/phitsanu-tr/linea-bot-v1)
![Status](https://img.shields.io/badge/status-active-brightgreen)

A high-speed ERC-20 auto-transfer bot for the [Linea](https://linea.build) network. Designed to quickly detect and transfer tokens when they appear in your wallet. Ideal for wallet protection, frontrun defense, or catching airdrops. Supports multi-RPC fallback and Flashbots for maximum performance.

---

## ✅ Requirements

- Ubuntu Server 24.04 LTS (tested)
- `curl`, `git`, `bash` (default on most Linux)
- Internet connection

---

## 🚀 Quick Install (Recommended)

Use the automated installer script:

```bash
curl -fsSL https://raw.githubusercontent.com/phitsanu-tr/linea-bot-v1/main/installer.sh | bash
```

> 💡 This script installs Node.js, Git, PM2, clones the bot, installs dependencies, and starts the bot.

---

## ⚙️ What You Still Need to Do

After installation, make sure to:

### 🧾 1. Configure `.env`

Open the `.env` file and set your values:

| Variable             | Description                               |
|----------------------|-------------------------------------------|
| `PRIVATE_KEY`        | Your wallet private key (keep it secure)  |
| `SAFE_WALLET`        | Target wallet to send detected tokens     |
| `WS_RPC_URLS`        | Comma-separated list of WebSocket URLs    |
| `TELEGRAM_BOT_TOKEN` | *(Optional)* Telegram bot token           |
| `TELEGRAM_CHAT_ID`   | *(Optional)* Your chat ID                 |

> 📁 `.env` file is already created by the installer.

---

### 🪙 2. Edit `tokens.json`

List the ERC-20 tokens you want the bot to detect and transfer:

```json
[
  {
    "symbol": "USDC",
    "address": "0x1234567890abcdef...",
    "decimals": 6
  },
  {
    "symbol": "USDT",
    "address": "0xabcdef1234567890...",
    "decimals": 6
  }
]
```

> 📁 Sample `tokens.json` is already created by the installer.

---

## 🧪 Usage

After editing `.env` and `tokens.json`, start or restart the bot:

```bash
pm2 restart linea-bot-v1
```

Check logs:

```bash
pm2 logs linea-bot-v1
```

> 🔄 The bot will auto-restart on crash or reboot (PM2 handles this).

---

## 🧰 PM2 Commands (Quick Reference)

| Command                   | Description              |
|---------------------------|--------------------------|
| `pm2 ls`                  | Show running processes   |
| `pm2 logs linea-bot-v1`   | Show bot logs            |
| `pm2 restart linea-bot-v1`| Restart the bot          |
| `pm2 stop linea-bot-v1`   | Stop the bot             |
| `pm2 delete linea-bot-v1` | Remove bot from PM2      |
| `pm2 save`                | Save PM2 startup config  |
| `pm2 startup`             | Generate boot startup    |

---

## 📁 Project Structure

```
linea-bot-v1/
├── bot.js              # Main bot logic
├── tokens.json         # Token list (user-defined)
├── .env                # Your secret config
├── .env.example        # Sample config
├── installer.sh        # Auto-install script
├── package.json        # NPM dependencies
├── README.md           # This file
├── LICENSE             # License info
```

---

## 📜 License

Distributed under the MIT License.  
See [`LICENSE`](./LICENSE) for details.

---

## 🤝 Contributing

Pull requests are welcome!  
Feel free to fork the repo and submit improvements or features.

---

## 👨‍💻 Author

Phitsanu Trutsat  
📫 GitHub: [@phitsanu-tr](https://github.com/phitsanu-tr)

---

## ⚠️ Security Warning

> Never share your `.env` or private key.  
> Treat your credentials as highly sensitive information.# linea-bot-v1
