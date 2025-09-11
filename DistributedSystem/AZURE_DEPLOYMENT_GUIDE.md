# 🚀 AutoTrader Azure 部署完整指南

## 🏗️ Azure VM 架構設計

```
Azure 資源群組: AutoTrader-RG
├── 1 台 Master VM (B2s)  - 協調中心 + MongoDB
├── 5 台 Slave VM (B1s)   - 資料收集
├── 1 個 Virtual Network  - 內網通訊
├── 6 個 公用 IP         - 每台VM獨立IP
└── 1 個 NSG 安全群組    - 防火牆規則
```

## 💰 成本預算 (台幣/月)

| 資源 | 規格 | 數量 | 月費 |
|------|------|------|------|
| Master VM | B2s (2C4G) | 1 | 1,200 |
| Slave VM | B1s (1C2G) | 5 | 2,500 |
| 公用 IP | Static | 6 | 180 |
| 儲存空間 | Standard HDD | 1TB | 1,200 |
| **總計** | | | **5,080** |

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

---

### **3. 創建 Master VM**

```bash
# 創建 Master VM
az vm create \
  --resource-group AutoTrader-RG \
  --name AutoTrader-Master \
  --image Ubuntu2204 \
  --size Standard_B2s \
  --vnet-name AutoTrader-VNet \
  --subnet default \
  --nsg AutoTrader-NSG \
  --public-ip-address-allocation static \
  --private-ip-address 10.0.1.100 \
  --admin-username azureuser \
  --generate-ssh-keys

# 獲取 Master 公用 IP
MASTER_PUBLIC_IP=$(az vm show -d -g AutoTrader-RG -n AutoTrader-Master --query publicIps -o tsv)
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

### **4. 創建 5 台 Slave VMs**

```bash
# 創建 Slave VMs
for i in {1..5}; do
  az vm create \
    --resource-group AutoTrader-RG \
    --name AutoTrader-Slave-$i \
    --image Ubuntu2204 \
    --size Standard_B1s \
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
for i in {1..5}; do
  SLAVE_IP=$(az vm show -d -g AutoTrader-RG -n AutoTrader-Slave-$i --query publicIps -o tsv)
  echo "Slave-$i Public IP: $SLAVE_IP"
done
```

**詳細解釋**:
- `for i in {1..5}`: 迴圈創建 5 台 VM
- `--size Standard_B1s`: 1 個 vCPU, 2GB RAM (適合資料收集)
- `10.0.1.10$i`: IP 分別是 10.0.1.101, 102, 103, 104, 105
- `&`: 背景執行，5 台 VM 同時創建 (更快)
- `wait`: 等待所有背景任務完成

**實際作用**: 每台 Slave 負責：
- 收集約 105 個 symbols 的市場資料
- 各自有獨立公用 IP (避開 API 限制)
- 將資料寫入 Master 的 MongoDB

---

### **5. 安裝 Docker (在所有 VM 上)**

#### Master VM 安裝 Docker
```bash
# Master VM
ssh azureuser@$MASTER_PUBLIC_IP << 'EOF'
sudo apt update
sudo apt install -y docker.io docker-compose-plugin git
sudo usermod -aG docker $USER
sudo systemctl enable docker
sudo systemctl start docker
EOF
```

#### Slave VMs 並行安裝 Docker
```bash
# Slave VMs (平行執行)
for i in {1..5}; do
  SLAVE_IP=$(az vm show -d -g AutoTrader-RG -n AutoTrader-Slave-$i --query publicIps -o tsv)
  ssh azureuser@$SLAVE_IP << 'EOF' &
  sudo apt update
  sudo apt install -y docker.io docker-compose-plugin git
  sudo usermod -aG docker $USER
  sudo systemctl enable docker
  sudo systemctl start docker
EOF
done
wait
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

#### 上傳程式碼
```bash
# 上傳到 Master VM
scp -r ./AutoTraderV2 azureuser@$MASTER_PUBLIC_IP:~/
```
**目的**: 把本機的程式碼複製到 Master VM  
**實際作用**: 使用 SCP 安全複製整個專案資料夾

#### 設定和部署 Master 服務
```bash
# SSH 到 Master 並設定
ssh azureuser@$MASTER_PUBLIC_IP << 'EOF'
cd ~/AutoTraderV2/DistributedSystem

# 更新 Master 配置
sed -i "s/MASTER_VM_IP=.*/MASTER_VM_IP=10.0.1.100/" Config/master/master_full_collection.env

# 生成 Slave 配置
cd Common/utils
MASTER_VM_IP=10.0.1.100 python3 full_symbol_distributor.py

# 部署 Master 服務
cd ../../Scripts/deployment
cp ../../Config/master/master_full_collection.env .env
sudo docker compose -f docker-compose.master.yml up -d
EOF
```

**詳細解釋**:

1. **更新配置**:
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
   sudo docker compose -f docker-compose.master.yml up -d
   ```
   **目的**: 啟動 Master 服務容器  
   **實際作用**:
   - 啟動 MongoDB 容器 (資料庫)
   - 啟動 Master API 容器 (協調器)
   - `-d`: 背景執行 (不會佔用終端)

---

### **7. 部署 Slave VMs**

```bash
# 分發 Slave 配置並部署
for i in {1..5}; do
  SLAVE_IP=$(az vm show -d -g AutoTrader-RG -n AutoTrader-Slave-$i --query publicIps -o tsv)
  
  # 上傳程式碼
  scp -r ./AutoTraderV2 azureuser@$SLAVE_IP:~/
  
  # 部署 Slave 服務
  ssh azureuser@$SLAVE_IP << EOF
  cd ~/AutoTraderV2/DistributedSystem/Scripts/deployment
  
  # 設定環境變數
  echo "SLAVE_ID=slave-$i" > .env
  echo "MASTER_URL=http://10.0.1.100:8080" >> .env
  echo "MONGO_URI=mongodb://10.0.1.100:27017/" >> .env
  
  # 啟動服務  
  sudo docker compose -f docker-compose.slave.yml up -d
EOF
done
```

**詳細解釋**:

1. **上傳程式碼**:
   ```bash
   scp -r ./AutoTraderV2 azureuser@$SLAVE_IP:~/
   ```
   **目的**: 把程式碼複製到每台 Slave VM  
   **實際作用**: 5 台 VM 都有完整的程式碼

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

這樣就完成了 AutoTrader 分散式系統在 Azure 的完整部署！每個步驟都有詳細說明，確保你能理解每一步的目的和作用。