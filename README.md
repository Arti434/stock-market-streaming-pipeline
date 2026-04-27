# 📈 Stock Market Real-Time Streaming Pipeline

A real-time streaming pipeline processing **live stock market data** 
for 5 major stocks using Google Cloud Platform.

## 🏗️ Architecture
Yahoo Finance API (every 5 seconds)
↓
Python Producer (Cloud Shell/Run)
↓
Cloud Pub/Sub (message queue)
↓
Dataflow Streaming (Apache Beam)
↓
BigQuery (streaming table)
↓
Looker Studio (live dashboard)
## ⚡ Pipeline Stats
| Metric | Value |
|--------|-------|
| Stocks Tracked | 5 (GOOGL, MSFT, AAPL, AMZN, TSLA) |
| Update Frequency | Every 5 seconds |
| End-to-End Latency | <10 seconds |
| Messages Processed | 50+ per run |
| Cost | ~$0.20/hour |

## 🛠️ Tech Stack
| Service | Purpose |
|---------|---------|
| Yahoo Finance API | Live stock data source |
| Python Producer | Fetches + publishes data |
| Cloud Pub/Sub | Message queue |
| Apache Beam | Stream processing |
| Dataflow Streaming | Managed runner |
| BigQuery | Real-time analytics |
| Looker Studio | Live dashboard |

## 📊 Stocks Tracked
- 🔵 GOOGL (Alphabet) — $344.40
- 🟢 MSFT (Microsoft) — $424.62
- 🍎 AAPL (Apple) — $271.06
- 📦 AMZN (Amazon) — $263.99
- ⚡ TSLA (Tesla) — $376.30

## 🚀 How to Run

### 1. Install Dependencies
```bash
pip3 install yfinance google-cloud-pubsub \
             google-cloud-bigquery apache-beam[gcp]
```

### 2. Start Streaming Pipeline
```bash
python3 stream_pipeline.py
```

### 3. Run Stock Producer
```bash
python3 producer.py
```

### 4. Query Live Data
```sql
SELECT symbol, price, change_pct, event_time
FROM stock-stream-pipeline-001.stock_data.stock_ticks
ORDER BY event_time DESC
LIMIT 10
```

## 📁 Project Structure
stock-market-streaming-pipeline/
├── producer.py          # Yahoo Finance → Pub/Sub
├── stream_pipeline.py   # Beam: Pub/Sub → BigQuery
└── README.md