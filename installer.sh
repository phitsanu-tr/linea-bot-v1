#!/bin/bash

# === Linea Bot Auto Installer (Latest Versions) ===
# Tested on: Ubuntu 22.04 LTS+
# Author: ChatGPT

set -e

# === ANSI Colors ===
GREEN='\e[1;32m'
YELLOW='\e[1;33m'
BLUE='\e[1;34m'
RESET='\e[0m'

# Configurable variables
BOT_REPO_URL="https://github.com/phitsanu-tr/linea-bot-v1.git"   # <<< CHANGE THIS
BOT_DIR="linea-bot-v1"

# === [1/7] Update System Packages ===
echo -e "\n${BLUE}🧰 [1/7] Updating system packages...${RESET}\n"
sudo apt update && sudo apt upgrade -y

# === [2/7] Install Latest Git ===
echo -e "\n${BLUE}🧰 [2/7] Installing latest Git...${RESET}\n"
sudo add-apt-repository ppa:git-core/ppa -y
sudo apt update
sudo apt install -y git

# === [3/7] Install Latest Node.js (LTS) ===
echo -e "\n${BLUE}🧰 [3/7] Installing latest Node.js (LTS)...${RESET}\n"
curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
sudo apt install -y nodejs

# === [4/7] Upgrade NPM to Latest ===
echo -e "\n${BLUE}🧰 [4/7] Installing latest npm...${RESET}\n"
sudo npm install -g npm@latest

# === Show Versions ===
echo -e "\n${YELLOW}Installed Versions:${RESET}"
echo -e "${GREEN}Git:$(git --version)${RESET}"
echo -e "${GREEN}Node.js:$(node -v)${RESET}"
echo -e "${GREEN}npm:$(npm -v)${RESET}\n"

# === [5/7] Clone Bot Repository ===
echo -e "\n${BLUE}🧰 [5/7] Cloning bot repository...${RESET}\n"
git clone $BOT_REPO_URL
cd $BOT_DIR

# === [6/7] Install NPM Dependencies ===
echo -e "\n${BLUE}🧰 [6/7] Installing dependencies...${RESET}\n"
npm install

# === [7/7] Install PM2 and Start Bot ===
echo -e "\n${BLUE}🧰 [7/7] Installing PM2 and starting bot...${RESET}\n"
sudo npm install -g pm2@latest
pm2 start bot.js --name linea-bot-v1
pm2 save
pm2 startup | tail -n 1 | bash

# === DONE ===
echo -e "\n${GREEN}✅ Setup complete! Your bot is now running under PM2.${RESET}\n"
echo "⚠️ Please configure your .env and tokens.js before running the bot."
echo ""
echo -e "${YELLOW}📘 PM2 Basic Commands:${RESET}"
echo "   pm2 ls                   # Show running processes"
echo "   pm2 logs                 # Show live logs"
echo "   pm2 restart linea-bot-v1 # Restart the bot"
echo "   pm2 stop linea-bot-v1    # Stop the bot"
echo "   pm2 delete linea-bot-v1  # Remove the bot from PM2"
echo "   pm2 save                 # Save the current process list (auto-start on boot)"
echo "   pm2 startup              # Generate startup script for auto-start on reboot"
echo ""
echo -e "${YELLOW}🔄 Your bot will automatically restart if it crashes or the system reboots.${RESET}\n"

exit 0