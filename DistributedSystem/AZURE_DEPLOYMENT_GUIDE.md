# 🚀 AutoTrader Azure 部署完整指南

## ⚙️ **部署配置設定**

### 📊 **可調整參數**
```bash
# 設定 Slave VM 數量 (可調整為任意台數)
export NUM_SLAVES=3           # ← 🔧 修改這裡：3台、5台、10台都可以
export MASTER_VM_SIZE="Standard_B2s"   # Master VM 規格
export SLAVE_VM_SIZE="Standard_B1s"    # Slave VM 規格  
export AZURE_REGION="East Asia"        # Azure 區域
export RESOURCE_GROUP="AutoTrader-RG"  # 資源群組名稱

# 計算相關數值
TOTAL_VMS=$((NUM_SLAVES + 1))
TOTAL_IPS=$TOTAL_VMS
SLAVE_COST=$((NUM_SLAVES * 500))        # 每台 Slave 約 500 TWD/月
TOTAL_IP_COST=$((TOTAL_IPS * 30))       # 每個 IP 約 30 TWD/月
ESTIMATED_TOTAL=$((1200 + SLAVE_COST + TOTAL_IP_COST + 1200))

echo "=== 部署配置摘要 ==="
echo "Master VM: 1 台 ($MASTER_VM_SIZE)"
echo "Slave VM: $NUM_SLAVES 台 ($SLAVE_VM_SIZE)"  
echo "預估月費: $ESTIMATED_TOTAL TWD"
echo "========================"
```

### 🎯 **常見配置範例**

| 場景 | Slave 數量 | 每台處理 symbols | 月費估算 |
|------|-----------|-----------------|---------|
| **小規模測試** | 3 台 | ~175 個 | ~3,590 TWD |
| **中等規模** | 5 台 | ~105 個 | ~5,080 TWD |
| **大規模** | 10 台 | ~53 個 | ~8,900 TWD |

## 🏗️ Azure VM 架構設計 (動態調整)

```
Azure 資源群組: $RESOURCE_GROUP
├── 1 台 Master VM ($MASTER_VM_SIZE)    - 協調中心 + MongoDB
├── $NUM_SLAVES 台 Slave VM ($SLAVE_VM_SIZE)  - 資料收集 (可調整)
├── 1 個 Virtual Network               - 內網通訊
├── $TOTAL_IPS 個 公用 IP              - 每台VM獨立IP
└── 1 個 NSG 安全群組                  - 防火牆規則

範例 (NUM_SLAVES=3):
├── Master VM:  10.0.1.100
├── Slave-1:    10.0.1.101  
├── Slave-2:    10.0.1.102
└── Slave-3:    10.0.1.103
```

## 💰 成本預算 (動態計算)

### 📊 **各規模成本對比**

| 規模 | Slave 數量 | Master VM | Slave VMs | 公用 IPs | 儲存 | **月費總計** |
|------|-----------|-----------|-----------|----------|------|-------------|
| 小規模 | 3 台 | 1,200 | 1,500 | 120 | 1,200 | **4,020 TWD** |
| 中規模 | 5 台 | 1,200 | 2,500 | 180 | 1,200 | **5,080 TWD** |
| 大規模 | 10 台 | 1,200 | 5,000 | 330 | 1,200 | **7,730 TWD** |

### 🧮 **成本計算公式**
```bash
# 動態成本計算
MASTER_COST=1200                    # B2s: 1,200 TWD/月
SLAVE_UNIT_COST=500                 # B1s: 500 TWD/月  
IP_UNIT_COST=30                     # 每個 IP: 30 TWD/月
STORAGE_COST=1200                   # 1TB HDD: 1,200 TWD/月

TOTAL_COST=$((MASTER_COST + (NUM_SLAVES * SLAVE_UNIT_COST) + (TOTAL_VMS * IP_UNIT_COST) + STORAGE_COST))

echo "NUM_SLAVES=$NUM_SLAVES 的月費: $TOTAL_COST TWD"
```

---

## 🔧 **前置需求**

### **本地工具安裝**

#### **必要工具**
```bash
# 檢查 Azure CLI
az --version

# 檢查 Git
git --version

# 檢查 SSH
ssh -V
```

#### **安裝 sshpass (推薦)**
```bash
# Ubuntu/Debian
sudo apt install sshpass

# CentOS/RHEL
sudo yum install sshpass

# macOS (需要 Homebrew)
brew install hudochenkov/sshpass/sshpass

# 檢查安裝
sshpass -V
```

### **認證方式設定**

#### **方式 1: sshpass 密碼認證 (推薦)**
```bash
# 設定 VM 密碼
export SSH_PASSWORD="your_secure_password"

# 在部署過程中將會使用:
# sshpass -p "$SSH_PASSWORD" ssh azureuser@VM_IP
```

#### **方式 2: SSH 金鑰認證**
```bash
# 生成 SSH 金鑰 (如果沒有)
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"

# Azure CLI 會自動使用 ~/.ssh/id_rsa.pub
```

### **環境變數設定**
```bash
# 必要配置
export NUM_SLAVES=3                    # Slave VM 數量
export SSH_PASSWORD="your_password"    # VM 密碼 (使用 sshpass)
export RESOURCE_GROUP="AutoTrader-RG"  # 資源群組名稱
export AZURE_REGION="East Asia"        # Azure 區域

# 可選配置
export MASTER_VM_SIZE="Standard_B2s"   # Master VM 規格
export SLAVE_VM_SIZE="Standard_B1s"    # Slave VM 規格
export GIT_REPO_URL="https://github.com/pjwu1997/AutoTraderV2.git"
export GIT_BRANCH="main"               # Git 分支

# === 1分鐘精度 + WebSocket 配置 ===
export TIMEFRAME="1m"                  # 資料精度 (1分鐘)
export KLINE_INTERVAL="1m"             # WebSocket K線間隔
export KLINE_SPOT_WS_URL="wss://stream.binance.com:9443/ws/{streams}"
export KLINE_FUTURES_WS_URL="wss://fstream.binance.com/ws/{streams}"
export LIQUIDATION_WS_URL="wss://fstream.binance.com/ws/!forceOrder@arr"
export LIQUIDATION_CLEANUP_MINUTES=5
export LIQUIDATION_RECONNECT_INTERVAL=86100
```

---

## 📝 部署步驟詳解

### **1. 創建 Azure 資源**

#### 登入 Azure
```bash
# 登入 Azure CLI
az login
```
**目的**: 驗證你的 Azure 帳戶，獲得操作權限  
**實際作用**: 開啟瀏覽器讓你登入，或使用設備代碼認證

#### 創建資源群組
```bash
# 創建資源群組
az group create --name AutoTrader-RG --location "East Asia"
```
**目的**: 創建邏輯容器來組織所有相關資源  
**實際作用**: 
- 所有 VM、網路、IP 都放在這個群組裡
- 方便統一管理和計費
- 刪除群組時會清理所有資源

#### 創建虛擬網路
```bash
# 創建虛擬網路
az network vnet create \
  --resource-group AutoTrader-RG \
  --name AutoTrader-VNet \
  --address-prefix 10.0.0.0/16 \
  --subnet-name default \
  --subnet-prefix 10.0.1.0/24
```
**目的**: 創建私有網路讓 VM 之間可以內網通訊  
**實際作用**:
- `10.0.0.0/16`: 整個虛擬網路範圍 (65,536 個 IP)
- `10.0.1.0/24`: 子網路範圍 (256 個 IP)
- Master: 10.0.1.100, Slaves: 10.0.1.101-105
- 內網通訊不經過公網，更快更安全

---

### **2. 創建安全群組 (防火牆規則)**

#### 創建網路安全群組
```bash
az network nsg create \
  --resource-group AutoTrader-RG \
  --name AutoTrader-NSG
```

#### 允許 SSH 連接
```bash
# 允許 SSH (22)
az network nsg rule create \
  --resource-group AutoTrader-RG \
  --nsg-name AutoTrader-NSG \
  --name SSH \
  --protocol tcp \
  --priority 1001 \
  --destination-port-range 22
```
**目的**: 允許從任何地方 SSH 連接到 VM  
**實際作用**: 你可以用 `ssh azureuser@公用IP` 連接管理 VM

#### 允許 MongoDB 內網存取
```bash
# 允許 MongoDB (27017) - 只給內網
az network nsg rule create \
  --resource-group AutoTrader-RG \
  --nsg-name AutoTrader-NSG \
  --name MongoDB \
  --protocol tcp \
  --priority 1002 \
  --destination-port-range 27017 \
  --source-address-prefix 10.0.0.0/16
```
**目的**: 只允許內網存取 MongoDB  
**實際作用**: 
- Slave VM 可以連接 Master 的 MongoDB
- 外網無法直接存取資料庫 (安全)
- `--source-address-prefix 10.0.0.0/16` 限制只有內網可存取

#### 允許 Master API 外網存取
```bash
# 允許 Master API (8080)
az network nsg rule create \
  --resource-group AutoTrader-RG \
  --nsg-name AutoTrader-NSG \
  --name MasterAPI \
  --protocol tcp \
  --priority 1003 \
  --destination-port-range 8080
```
**目的**: 允許外網存取 Master Dashboard 和 API  
**實際作用**: 你可以用瀏覽器開啟 `http://公用IP:8080/dashboard.html`

#### 允許 Slave Health Check 內網存取
```bash
# 允許 Slave Health (8081) - 只給內網
az network nsg rule create \
  --resource-group AutoTrader-RG \
  --nsg-name AutoTrader-NSG \
  --name SlaveHealth \
  --protocol tcp \
  --priority 1004 \
  --destination-port-range 8081 \
  --source-address-prefix 10.0.0.0/16
```

#### 允許 WebSocket 服務內網存取
```bash
# 允許 Kline WebSocket (8082) - 只給內網
az network nsg rule create \
  --resource-group AutoTrader-RG \
  --nsg-name AutoTrader-NSG \
  --name KlineWebSocket \
  --protocol tcp \
  --priority 1005 \
  --destination-port-range 8082 \
  --source-address-prefix 10.0.0.0/16

# 允許 Liquidation WebSocket (8083) - 只給內網  
az network nsg rule create \
  --resource-group AutoTrader-RG \
  --nsg-name AutoTrader-NSG \
  --name LiquidationWebSocket \
  --protocol tcp \
  --priority 1006 \
  --destination-port-range 8083 \
  --source-address-prefix 10.0.0.0/16
```

---

### **3. 創建 Master VM**

**使用 SSH 金鑰認證** (預設):
```bash
# 創建 Master VM (使用 SSH 金鑰)
az vm create \
  --resource-group $RESOURCE_GROUP \
  --name AutoTrader-Master \
  --image Ubuntu2204 \
  --size $MASTER_VM_SIZE \
  --vnet-name AutoTrader-VNet \
  --subnet default \
  --nsg AutoTrader-NSG \
  --public-ip-address-allocation static \
  --private-ip-address 10.0.1.100 \
  --admin-username azureuser \
  --generate-ssh-keys

# 獲取 Master 公用 IP
MASTER_PUBLIC_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Master --query publicIps -o tsv)
echo "Master Public IP: $MASTER_PUBLIC_IP"
```

**或使用密碼認證** (搭配 sshpass):
```bash
# 創建 Master VM (使用密碼認證)
az vm create \
  --resource-group $RESOURCE_GROUP \
  --name AutoTrader-Master \
  --image Ubuntu2204 \
  --size $MASTER_VM_SIZE \
  --vnet-name AutoTrader-VNet \
  --subnet default \
  --nsg AutoTrader-NSG \
  --public-ip-address-allocation static \
  --private-ip-address 10.0.1.100 \
  --admin-username azureuser \
  --admin-password "$SSH_PASSWORD" \
  --authentication-type password

# 獲取 Master 公用 IP
MASTER_PUBLIC_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Master --query publicIps -o tsv)
echo "Master Public IP: $MASTER_PUBLIC_IP"
```

**詳細解釋**:
- `--image Ubuntu2204`: 使用 Ubuntu 22.04 作業系統
- `--size Standard_B2s`: 2 個 vCPU, 4GB RAM (適合協調工作)
- `--public-ip-address-allocation static`: 固定公用 IP (不會變動)
- `--private-ip-address 10.0.1.100`: 固定內網 IP
- `--generate-ssh-keys`: 自動生成 SSH 金鑰對

**實際作用**: 創建協調中心，負責：
- 運行 MongoDB 資料庫
- 運行 Master API 分配工作
- 提供監控 Dashboard

---

### **4. 創建 Slave VMs (動態數量)**

```bash
# 創建 Slave VMs (使用 $NUM_SLAVES 變數)
for i in $(seq 1 $NUM_SLAVES); do
  az vm create \
    --resource-group $RESOURCE_GROUP \
    --name AutoTrader-Slave-$i \
    --image Ubuntu2204 \
    --size $SLAVE_VM_SIZE \
    --vnet-name AutoTrader-VNet \
    --subnet default \
    --nsg AutoTrader-NSG \
    --public-ip-address-allocation static \
    --private-ip-address 10.0.1.10$i \
    --admin-username azureuser \
    --generate-ssh-keys &
done
wait

# 獲取所有 Slave 公用 IP
for i in $(seq 1 $NUM_SLAVES); do
  SLAVE_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Slave-$i --query publicIps -o tsv)
  echo "Slave-$i Public IP: $SLAVE_IP"
done
```

**詳細解釋**:
- `for i in $(seq 1 $NUM_SLAVES)`: 迴圈創建 $NUM_SLAVES 台 VM (支援任意數量)
- `--size $SLAVE_VM_SIZE`: 使用變數指定 VM 規格 (可調整)
- `10.0.1.10$i`: IP 從 10.0.1.101 開始依序分配
- `&`: 背景執行，多台 VM 同時創建 (更快)
- `wait`: 等待所有背景任務完成

**實際作用**: 每台 Slave 負責：
- 收集約 (總symbols ÷ $NUM_SLAVES) 個 symbols 的市場資料
- 各自有獨立公用 IP (避開 API 限制)
- 將資料寫入 Master 的 MongoDB

**範例**: NUM_SLAVES=3 時，創建 Slave-1, Slave-2, Slave-3

---

### **5. 安裝 Docker (在所有 VM 上)**

#### Master VM 安裝 Docker

**使用 sshpass 密碼認證**:
```bash
# Master VM (使用 sshpass)
sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no azureuser@$MASTER_PUBLIC_IP << 'EOF'
sudo apt update
sudo apt install -y docker.io docker-compose-plugin git
sudo usermod -aG docker $USER
sudo systemctl enable docker
sudo systemctl start docker
EOF
```

**或使用 SSH 金鑰認證**:
```bash
# Master VM (使用 SSH 金鑰)
ssh -o StrictHostKeyChecking=no azureuser@$MASTER_PUBLIC_IP << 'EOF'
sudo apt update
sudo apt install -y docker.io docker-compose-plugin git
sudo usermod -aG docker $USER
sudo systemctl enable docker
sudo systemctl start docker
EOF
```

#### Slave VMs 並行安裝 Docker (動態數量)

**使用 sshpass 密碼認證**:
```bash
# Slave VMs (平行執行，使用 sshpass)
for i in $(seq 1 $NUM_SLAVES); do
  SLAVE_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Slave-$i --query publicIps -o tsv)
  sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no azureuser@$SLAVE_IP << 'EOF' &
  sudo apt update
  sudo apt install -y docker.io docker-compose-plugin git
  sudo usermod -aG docker $USER
  sudo systemctl enable docker
  sudo systemctl start docker
EOF
done
wait

echo "Docker 安裝完成於 1 台 Master + $NUM_SLAVES 台 Slave VMs"
```

**或使用 SSH 金鑰認證**:
```bash
# Slave VMs (平行執行，使用 SSH 金鑰)
for i in $(seq 1 $NUM_SLAVES); do
  SLAVE_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Slave-$i --query publicIps -o tsv)
  ssh -o StrictHostKeyChecking=no azureuser@$SLAVE_IP << 'EOF' &
  sudo apt update
  sudo apt install -y docker.io docker-compose-plugin git
  sudo usermod -aG docker $USER
  sudo systemctl enable docker
  sudo systemctl start docker
EOF
done
wait

echo "Docker 安裝完成於 1 台 Master + $NUM_SLAVES 台 Slave VMs"
```

**詳細解釋**:
- `ssh azureuser@$MASTER_PUBLIC_IP << 'EOF'`: 遠端執行指令
- `apt update`: 更新套件列表
- `docker.io docker-compose-plugin`: 安裝 Docker 和 Docker Compose
- `usermod -aG docker $USER`: 把使用者加入 docker 群組 (不用 sudo)
- `systemctl enable docker`: 開機自動啟動 Docker

**實際作用**: 讓每台 VM 都能運行 Docker 容器

---

### **6. 部署程式碼到 Master**

#### 克隆程式碼

**使用 sshpass 密碼認證**:
```bash
# 在 Master VM 上克隆程式碼 (使用 sshpass)
sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no azureuser@$MASTER_PUBLIC_IP << 'EOF'
# 移除舊的程式碼 (如果存在)
rm -rf ~/AutoTraderV2

# 從 GitHub 克隆最新程式碼
git clone https://github.com/pjwu1997/AutoTraderV2.git ~/AutoTraderV2

echo "程式碼克隆完成"
EOF
```

**或使用 SSH 金鑰認證**:
```bash
# 在 Master VM 上克隆程式碼 (使用 SSH 金鑰)
ssh -o StrictHostKeyChecking=no azureuser@$MASTER_PUBLIC_IP << 'EOF'
# 移除舊的程式碼 (如果存在)
rm -rf ~/AutoTraderV2

# 從 GitHub 克隆最新程式碼
git clone https://github.com/pjwu1997/AutoTraderV2.git ~/AutoTraderV2

echo "程式碼克隆完成"
EOF
```
**目的**: 直接從 GitHub 下載最新程式碼到 Master VM  
**實際作用**: 
- 確保使用最新版本程式碼
- 避免本地上傳大量檔案
- 支援版本控制和分支切換

#### 設定和部署 Master 服務

**使用 sshpass 密碼認證**:
```bash
# SSH 到 Master 並設定 (使用 sshpass)
sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no azureuser@$MASTER_PUBLIC_IP << EOF
cd ~/AutoTraderV2/DistributedSystem

# 拉取最新代碼 (包含統一的 requirements.txt)
git pull origin main

# 更新 Master 配置
sed -i "s/MASTER_VM_IP=.*/MASTER_VM_IP=10.0.1.100/" Config/master/master_full_collection.env

# 生成 Slave 配置
cd Common/utils
NUM_SLAVES=$NUM_SLAVES MASTER_VM_IP=10.0.1.100 python3 full_symbol_distributor.py

# 回到根目錄並部署 Master 服務
cd ~/AutoTraderV2/DistributedSystem
cp Config/master/master_full_collection.env Scripts/deployment/.env
sudo docker compose -f Scripts/deployment/docker-compose.master.yml up -d --build
EOF
```

**或使用 SSH 金鑰認證**:
```bash
# SSH 到 Master 並設定 (使用 SSH 金鑰)
ssh -o StrictHostKeyChecking=no azureuser@$MASTER_PUBLIC_IP << EOF
cd ~/AutoTraderV2/DistributedSystem

# 拉取最新代碼 (包含統一的 requirements.txt)
git pull origin main

# 更新 Master 配置
sed -i "s/MASTER_VM_IP=.*/MASTER_VM_IP=10.0.1.100/" Config/master/master_full_collection.env

# 生成 Slave 配置
cd Common/utils
NUM_SLAVES=$NUM_SLAVES MASTER_VM_IP=10.0.1.100 python3 full_symbol_distributor.py

# 回到根目錄並部署 Master 服務
cd ~/AutoTraderV2/DistributedSystem
cp Config/master/master_full_collection.env Scripts/deployment/.env
sudo docker compose -f Scripts/deployment/docker-compose.master.yml up -d --build
EOF
```

**詳細解釋**:

1. **拉取最新代碼**:
   ```bash
   git pull origin main
   ```
   **目的**: 確保獲取包含統一 requirements.txt 的最新版本  
   **實際作用**: 
   - 下載統一的依賴管理系統
   - 獲取修復的 Docker 配置檔案
   - 確保所有服務使用相同版本的依賴

2. **更新配置**:
   ```bash
   sed -i "s/MASTER_VM_IP=.*/MASTER_VM_IP=10.0.1.100/" Config/master/master_full_collection.env
   ```
   **目的**: 修改配置檔案中的 Master IP  
   **實際作用**: 用 `sed` 指令替換檔案中的 IP 地址

2. **生成 Slave 配置**:
   ```bash
   MASTER_VM_IP=10.0.1.100 python3 full_symbol_distributor.py
   ```
   **目的**: 生成每台 Slave 的專用配置檔案  
   **實際作用**: 
   - 自動分配 526 個 symbols 給 5 台 Slave
   - 生成 `slave-1.env` 到 `slave-5.env`
   - 每個檔案包含該 Slave 要收集的 symbols 清單

3. **啟動 Master 服務**:
   ```bash
   sudo docker compose -f docker-compose.master.yml up -d --build
   ```
   **目的**: 啟動 Master 服務容器  
   **實際作用**:
   - 啟動 MongoDB 容器 (資料庫)
   - 啟動 Master API 容器 (協調器)
   - `--build`: 重新建構 Docker 映像 (包含最新代碼)
   - `-d`: 背景執行 (不會佔用終端)

---

### **7. 部署 Slave VMs (動態數量)**

**使用 sshpass 密碼認證**:
```bash
# 分發 Slave 配置並部署 (支援任意台數，使用 sshpass)
for i in $(seq 1 $NUM_SLAVES); do
  SLAVE_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Slave-$i --query publicIps -o tsv)
  
  echo "正在部署 Slave-$i (IP: $SLAVE_IP)..."
  
  # 克隆程式碼並部署服務 (使用 sshpass)
  sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no azureuser@$SLAVE_IP << EOF
  # 移除舊的程式碼 (如果存在)
  rm -rf ~/AutoTraderV2
  
  # 從 GitHub 克隆最新程式碼 (包含統一 requirements.txt)
  git clone https://github.com/pjwu1997/AutoTraderV2.git ~/AutoTraderV2
  
  # 切換到部署目錄
  cd ~/AutoTraderV2/DistributedSystem/Scripts/deployment
  
  # 設定環境變數
  echo "SLAVE_ID=slave-$i" > .env
  echo "MASTER_URL=http://10.0.1.100:8080" >> .env
  echo "MONGO_URI=mongodb://10.0.1.100:27017/" >> .env
  echo "NUM_SLAVES=$NUM_SLAVES" >> .env
  
  # 啟動服務 (重新建構以使用統一依賴)
  sudo docker compose -f docker-compose.slave.yml up -d --build
  
  echo "Slave-$i 部署完成"
EOF
done

echo "=== 部署完成 ==="
echo "已部署 $NUM_SLAVES 台 Slave VMs"
echo "Symbol 分配: 約 $((526 / NUM_SLAVES)) 個 symbols/台"
```

**或使用 SSH 金鑰認證**:
```bash
# 分發 Slave 配置並部署 (支援任意台數，使用 SSH 金鑰)
for i in $(seq 1 $NUM_SLAVES); do
  SLAVE_IP=$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Slave-$i --query publicIps -o tsv)
  
  echo "正在部署 Slave-$i (IP: $SLAVE_IP)..."
  
  # 克隆程式碼並部署服務 (使用 SSH 金鑰)
  ssh -o StrictHostKeyChecking=no azureuser@$SLAVE_IP << EOF
  # 移除舊的程式碼 (如果存在)
  rm -rf ~/AutoTraderV2
  
  # 從 GitHub 克隆最新程式碼 (包含統一 requirements.txt)
  git clone https://github.com/pjwu1997/AutoTraderV2.git ~/AutoTraderV2
  
  # 切換到部署目錄
  cd ~/AutoTraderV2/DistributedSystem/Scripts/deployment
  
  # 設定環境變數
  echo "SLAVE_ID=slave-$i" > .env
  echo "MASTER_URL=http://10.0.1.100:8080" >> .env
  echo "MONGO_URI=mongodb://10.0.1.100:27017/" >> .env
  echo "NUM_SLAVES=$NUM_SLAVES" >> .env
  
  # 啟動服務 (重新建構以使用統一依賴)
  sudo docker compose -f docker-compose.slave.yml up -d --build
  
  echo "Slave-$i 部署完成"
EOF
done

echo "=== 部署完成 ==="
echo "已部署 $NUM_SLAVES 台 Slave VMs"
echo "Symbol 分配: 約 $((526 / NUM_SLAVES)) 個 symbols/台"
```

**詳細解釋**:

1. **克隆程式碼**:
   ```bash
   git clone https://github.com/pjwu1997/AutoTraderV2.git ~/AutoTraderV2
   ```
   **目的**: 從 GitHub 下載最新程式碼到每台 Slave VM  
   **實際作用**: 
   - $NUM_SLAVES 台 VM 都有完整且最新的程式碼
   - 避免本地網路上傳，直接從 GitHub 下載更快
   - 支援版本控制，確保代碼一致性

2. **設定環境變數**:
   ```bash
   echo "SLAVE_ID=slave-$i" > .env
   echo "MASTER_URL=http://10.0.1.100:8080" >> .env
   echo "MONGO_URI=mongodb://10.0.1.100:27017/" >> .env
   ```
   **目的**: 告訴每台 Slave：
   - 自己的身份 (slave-1, slave-2, ...)
   - Master API 的位置
   - MongoDB 的位置

3. **啟動 Slave 服務**:
   ```bash
   sudo docker compose -f docker-compose.slave.yml up -d
   ```
   **目的**: 啟動 Slave 服務容器  
   **實際作用**:
   - 啟動資料收集程序
   - 啟動健康檢查程序
   - 開始收集分配的 symbols 資料

---

### **8. 驗證部署**

#### 檢查 Master 狀態
```bash
# 檢查 Master 狀態
curl http://$MASTER_PUBLIC_IP:8080/api/status
```
**目的**: 確認 Master API 正常運作  
**期待結果**: 回傳 JSON 顯示系統狀態

#### 檢查系統總覽
```bash
# 檢查系統總覽
python3 DistributedSystem/Scripts/monitoring/system_status.py http://$MASTER_PUBLIC_IP:8080
```
**目的**: 檢查所有 Slave 是否成功連接  
**期待結果**: 顯示 5 台 Slave 都在線上

#### 檢查資料庫
```bash
# 檢查資料庫
ssh azureuser@$MASTER_PUBLIC_IP
sudo docker exec -it shared-mongo mongosh --eval "db.adminCommand('listCollections')"
```
**目的**: 確認 MongoDB 有接收到資料  
**期待結果**: 看到市場資料的 collections

---

## 🔄 整個流程的邏輯

1. **基礎建設**: 建立網路、安全規則、VM
2. **環境準備**: 安裝 Docker、上傳程式碼  
3. **服務啟動**: Master 先啟動，然後 Slaves 連接
4. **資料流向**: Slaves → 收集資料 → Master MongoDB
5. **監控管理**: 透過 Dashboard 監控整體狀態

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Slave-1   │    │   Slave-2   │    │   Slave-5   │
│  105 symbols │   │  105 symbols │   │  105 symbols │
│  獨立公用IP  │    │  獨立公用IP  │    │  獨立公用IP  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────┐
│            Master VM (10.0.1.100)                  │
│ ┌─────────────────┐  ┌─────────────────┐           │
│ │   Master API    │  │    MongoDB      │           │
│ │     :8080       │  │     :27017      │           │
│ └─────────────────┘  └─────────────────┘           │
└─────────────────────────────────────────────────────┘
```

---

## 🔧 管理指令

### VM 管理
```bash
# 重啟所有服務
az vm restart --ids $(az vm list -g AutoTrader-RG --query "[].id" -o tsv)

# 停止所有 VM (節省成本)
az vm deallocate --ids $(az vm list -g AutoTrader-RG --query "[].id" -o tsv)

# 啟動所有 VM
az vm start --ids $(az vm list -g AutoTrader-RG --query "[].id" -o tsv)
```

### 成本監控
```bash
# 查看成本
az consumption usage list --resource-group AutoTrader-RG
```

### 服務管理
```bash
# Master 服務管理
ssh azureuser@$MASTER_PUBLIC_IP
sudo docker compose -f docker-compose.master.yml logs
sudo docker compose -f docker-compose.master.yml restart
sudo docker compose -f docker-compose.master.yml down

# Slave 服務管理 (在各台 Slave VM 上)
sudo docker compose -f docker-compose.slave.yml logs
sudo docker compose -f docker-compose.slave.yml restart
sudo docker compose -f docker-compose.slave.yml down
```

---

## 📊 監控 Dashboard

部署完成後可透過以下方式監控：

### Web Dashboard
- **Master Dashboard**: `http://$MASTER_PUBLIC_IP:8080/dashboard.html`
- **API 狀態**: `http://$MASTER_PUBLIC_IP:8080/api/status`

### 命令行監控
```bash
# 系統狀態總覽
python3 DistributedSystem/Scripts/monitoring/system_status.py http://$MASTER_PUBLIC_IP:8080

# MongoDB 資料統計
ssh azureuser@$MASTER_PUBLIC_IP
sudo docker exec -it shared-mongo mongosh
use trading_data
db.market_data.countDocuments()
db.market_data.find().limit(5)

# 查看各 Symbol 資料量
db.market_data.aggregate([
  {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}}
])
```

---

## 🚨 注意事項

### 安全性
1. **防火牆**: MongoDB 只允許內網存取
2. **SSH 金鑰**: 妥善保存 SSH 私鑰  
3. **網路隔離**: 敏感服務不對外開放

### 成本控制
1. **不用時停機**: 使用 `az vm deallocate` 節省成本
2. **監控資源**: 定期檢查 CPU、記憶體使用率
3. **儲存優化**: 定期清理舊資料

### 運維管理
1. **備份**: 定期備份 MongoDB 資料
2. **監控**: 設定 Azure Monitor 告警
3. **日誌**: 配置日誌收集和輪轉
4. **更新**: 定期更新 Docker images 和系統

### 故障排除
1. **連接問題**: 檢查網路安全群組規則
2. **服務故障**: 查看 Docker 容器日誌
3. **資料問題**: 檢查 MongoDB 連接和權限
4. **效能問題**: 監控 VM 資源使用率

---

## 📈 效能優化建議

### 負載分散
- 每台 Slave 處理 ~105 symbols
- API 使用率約 44% (526/1200 請求/分鐘)
- 有足夠餘量應對高峰流量

### 擴展策略
1. **水平擴展**: 增加更多 Slave VM
2. **垂直擴展**: 升級 VM 規格
3. **區域分散**: 在多個 Azure 區域部署

### 資料庫優化
1. **索引優化**: 針對查詢模式建立索引
2. **分片**: 大量資料時考慮 MongoDB 分片
3. **備份策略**: 自動化備份和還原流程

---

## 🚀 **快速開始 - 不同規模部署**

### **3台 VM 配置 (小規模測試)**

**使用 sshpass 密碼認證**:
```bash
# 設定配置
export NUM_SLAVES=3
export SSH_PASSWORD="your_secure_password"  # ← 設定 VM 密碼
export MASTER_VM_SIZE="Standard_B2s"
export SLAVE_VM_SIZE="Standard_B1s"
export AZURE_REGION="East Asia"
export RESOURCE_GROUP="AutoTrader-RG"

# 預估成本: ~4,020 TWD/月
# 每台 Slave 處理: ~175 個 symbols

# 依照指南步驟執行部署
```

**或使用 SSH 金鑰認證**:
```bash
# 設定配置 (不需要密碼)
export NUM_SLAVES=3
export MASTER_VM_SIZE="Standard_B2s"
export SLAVE_VM_SIZE="Standard_B1s"
export AZURE_REGION="East Asia"
export RESOURCE_GROUP="AutoTrader-RG"

# 確保有 SSH 金鑰
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"  # 如果沒有的話

# 依照指南步驟執行部署
```

### **5台 VM 配置 (中等規模)**
```bash
# 設定配置 (使用 sshpass 或 SSH 金鑰)
export NUM_SLAVES=5
export SSH_PASSWORD="your_secure_password"  # sshpass 使用 (可選)
export MASTER_VM_SIZE="Standard_B2s"
export SLAVE_VM_SIZE="Standard_B1s"

# 預估成本: ~5,080 TWD/月
# 每台 Slave 處理: ~105 個 symbols

# 依照指南步驟執行部署
```

### **10台 VM 配置 (大規模生產)**
```bash
# 設定配置 (使用 sshpass 或 SSH 金鑰)
export NUM_SLAVES=10
export SSH_PASSWORD="your_secure_password"  # sshpass 使用 (可選)
export MASTER_VM_SIZE="Standard_B4ms"    # 升級 Master
export SLAVE_VM_SIZE="Standard_B2s"      # 升級 Slave

# 預估成本: ~12,000 TWD/月
# 每台 Slave 處理: ~53 個 symbols

# 依照指南步驟執行部署
```

### **一鍵部署腳本範例**
```bash
#!/bin/bash
# deploy_autotrader.sh

# 讀取配置
echo "=== AutoTrader Azure 部署 ==="
echo "Slave VMs: $NUM_SLAVES 台"
echo "預估月費: $((1200 + NUM_SLAVES * 500 + (NUM_SLAVES + 1) * 30 + 1200)) TWD"
read -p "確認部署? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # 執行所有部署步驟
    echo "開始部署..."
    
    # 1. 創建資源群組
    az group create --name $RESOURCE_GROUP --location "$AZURE_REGION"
    
    # 2. 創建虛擬網路
    az network vnet create --resource-group $RESOURCE_GROUP --name AutoTrader-VNet --address-prefix 10.0.0.0/16 --subnet-name default --subnet-prefix 10.0.1.0/24
    
    # 3. 創建 Master VM
    # ... (使用本指南中的所有步驟)
    
    echo "部署完成！"
    echo "Master Dashboard: http://$(az vm show -d -g $RESOURCE_GROUP -n AutoTrader-Master --query publicIps -o tsv):8080"
else
    echo "取消部署"
fi
```

---

## 🎯 **總結**

現在 AutoTrader Azure 部署指南已**完全支援任意台數的 VM**！

### ✅ **新功能**:
- 🔧 **可配置 VM 數量**: 從 3 台到 10+ 台都支援
- 💰 **動態成本計算**: 自動計算不同規模的月費
- 📊 **彈性架構**: 根據需求選擇最適合的配置
- 🚀 **一鍵部署**: 支援腳本化自動部署
- 🔗 **Git 部署**: 使用 Git clone 取代 scp，更快更可靠
- 🔐 **多重認證**: 支援 sshpass 密碼認證 + SSH 金鑰認證
- 📦 **統一依賴管理**: 使用單一 requirements.txt 避免版本衝突

### 🔧 **重要系統改進**:
- ✅ **統一 Python 依賴**: 所有服務使用同一個 requirements.txt 檔案
- ✅ **Docker 構建優化**: 修正建構上下文，支援統一依賴載入
- ✅ **自動代碼更新**: 部署時自動拉取最新代碼 (`git pull`)
- ✅ **環境變數修正**: 修正 PYTHONPATH 讓模組正確載入
- ✅ **MongoDB 配置簡化**: 移除不相容選項，確保穩定啟動

### 🎯 **使用建議**:
- **測試階段**: 3台 VM (每月 ~4,020 TWD)
- **小規模生產**: 5台 VM (每月 ~5,080 TWD)  
- **大規模生產**: 10台 VM (每月 ~7,730 TWD)

只需修改 `NUM_SLAVES` 變數，整個部署流程自動適應！🎉