<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://arweave.net/A-GUH6P1PJVYcgMVdA_CWPVfrOEk8e8S8P4lQVz3P7s">
  <source media="(prefers-color-scheme: light)" srcset="https://arweave.net/A-GUH6P1PJVYcgMVdA_CWPVfrOEk8e8S8P4lQVz3P7s">
  <img alt="Arweave Logo" src="https://arweave.net/A-GUH6P1PJVYcgMVdA_CWPVfrOEk8e8S8P4lQVz3P7s" width="200">
</picture>

# dbt-arweave

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![dbt](https://img.shields.io/badge/dbt-1.7.10-FF694B?logo=dbt)](https://www.getdbt.com/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![Archived](https://img.shields.io/badge/status-archived-lightgrey)](https://github.com/roark-technology/dbt-arweave)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-no-red.svg)](https://github.com/roark-technology/dbt-arweave)

> **⚠️ ARCHIVED PROJECT**  
> This repository is no longer maintained and is provided as-is for reference purposes only.
> No support, updates, or bug fixes will be provided.

---

A comprehensive **dbt (data build tool)** project for analyzing and transforming Arweave blockchain data. This project provides SQL models and transformations for building analytics pipelines on Arweave L1 and L2 (SmartWeave/AO) transaction data, using ClickHouse as the data warehouse.

## Overview

**dbt-arweave** was developed by [Roark Technology](https://roark.com) to power analytics dashboards and insights for the Arweave ecosystem. It transforms raw blockchain data into meaningful metrics, aggregations, and statistics.

### Key Features

- 📊 **L1 Analytics** — Transaction volumes, file storage metrics, protocol adoption, user growth
- 🔄 **L2 SmartWeave Analytics** — Contract interactions, action tracking, user behavior
- 🌐 **AO Protocol Metrics** — Message counts, process creation, module deployment statistics
- 💰 **Financial Analytics** — Block rewards, network fees, endowment tracking
- 📈 **Growth Metrics** — Rolling averages, cumulative statistics, trend analysis
- 🏷️ **Application Tracking** — Protocol categorization, content-type analysis

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Data Sources                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │   Goldsky    │  │   Gateway    │  │    Blocks    │                   │
│  │ Transactions │  │  Block Dates │  │     Data     │                   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                   │
└─────────┼─────────────────┼─────────────────┼───────────────────────────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Core Silver Layer                               │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │              transactions_silver_tbl (Incremental)              │     │
│  │  • Tag extraction & normalization                               │     │
│  │  • Content type classification                                  │     │
│  │  • Application identification                                   │     │
│  │  • SmartWeave/AO detection                                     │     │
│  └────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  L1 Models   │   │  L2 Models   │   │  AO Models   │
│  ──────────  │   │  ──────────  │   │  ──────────  │
│ • Stats      │   │ • SmartWeave │   │ • Metrics    │
│ • Financials │   │ • User Stats │   │ • Rolling    │
│ • Trends     │   │ • Actions    │   │   Metrics    │
│ • ANS        │   │              │   │              │
└──────────────┘   └──────────────┘   └──────────────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            ▼
                    ┌──────────────┐
                    │  Messenger   │
                    │  (Optional)  │
                    │  ──────────  │
                    │ Pushes data  │
                    │ to AO chain  │
                    └──────────────┘
```

## Project Structure

```
dbt-arweave/
├── models/
│   ├── core/                    # Base transformations
│   │   ├── transactions_silver_tbl.sql
│   │   ├── functions.sql
│   │   └── indexes.sql
│   ├── ao/                      # AO protocol analytics
│   │   └── aolink/
│   │       ├── ao_metrics.sql
│   │       └── ao_rolling_metrics.sql
│   ├── apps/                    # Application-specific models
│   │   ├── redstone/           # Oracle analytics
│   │   └── stamps/             # Stamps protocol
│   ├── l1/                      # Layer 1 analytics
│   │   ├── stats/              # Usage statistics
│   │   ├── financials/         # Economic metrics
│   │   ├── trends/             # Growth analysis
│   │   └── ans/                # ANS adoption
│   └── l2/                      # SmartWeave analytics
│       ├── actions/
│       └── stats/
├── seeds/                       # Static reference data
├── messenger/                   # AO integration service
├── macros/                      # Custom dbt macros
└── .github/workflows/           # CI/CD examples
```

## Models Reference

### Core Layer
| Model | Description | Materialization |
|-------|-------------|-----------------|
| `transactions_silver_tbl` | Enriched transaction data with parsed tags | Incremental |

### L1 Analytics
| Model | Description |
|-------|-------------|
| `l1_summary_stats_daily` | Daily transaction, file, and user counts |
| `l1_summary_stats_monthly` | Monthly aggregations |
| `l1_protocol_stats` | Per-protocol statistics |
| `l1_user_stats` | User activity metrics |
| `l1_rolling_*` | Cumulative rolling metrics |
| `block_rewards` | Mining reward tracking |
| `daily_fees` | Network fee analysis |
| `endowment` | Storage endowment balance |

### L2 Analytics
| Model | Description |
|-------|-------------|
| `smartweave_tags_mv` | Parsed SmartWeave tags |
| `smartweave_pre_agg_tbl` | Pre-aggregated contract interactions |
| `l2_user_stats` | SmartWeave user statistics |
| `l2_rolling_*` | Cumulative L2 metrics |

### AO Analytics
| Model | Description |
|-------|-------------|
| `ao_metrics` | Daily AO protocol metrics |
| `ao_rolling_metrics` | Cumulative AO statistics |

## Prerequisites

- **Python** 3.11+
- **dbt-core** 1.7.10+
- **dbt-clickhouse** 1.7.3+
- **ClickHouse** database with Arweave data

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/roark-technology/dbt-arweave.git
   cd dbt-arweave
   ```

2. **Install dbt and ClickHouse adapter**
   ```bash
   pip install dbt-core==1.7.10 dbt-clickhouse==1.7.3
   ```

3. **Configure database connection**
   ```bash
   cp profiles.example.yml profiles.yml
   # Edit profiles.yml with your ClickHouse credentials
   ```

4. **Install dbt packages**
   ```bash
   dbt deps
   ```

5. **Verify connection**
   ```bash
   dbt debug
   ```

## Usage

### Run all models
```bash
dbt run
```

### Run specific model groups
```bash
# L1 models only
dbt run --select l1.*

# AO metrics
dbt run --select ao.*

# Specific model
dbt run --select l1_summary_stats_daily
```

### Build documentation
```bash
dbt docs generate
dbt docs serve
```

## Data Sources

This project expects the following source tables in your ClickHouse database:

| Source | Table | Description |
|--------|-------|-------------|
| `goldsky` | `transactions` | Raw Arweave transactions |
| `gateway` | `block_dates` | Block timestamp mappings |
| `default` | `blocks` | Block metadata |

See `models/sources.yml` for complete schema definitions.

## Messenger Service

The `messenger/` directory contains a Node.js service that pushes aggregated metrics to an AO process, enabling on-chain analytics access.

### Setup
```bash
cd messenger
npm install @permaweb/aoconnect clickhouse
```

### Environment Variables
```bash
export CLICKHOUSE_HOST="your-clickhouse-host"
export CLICKHOUSE_USER="your-username"
export CLICKHOUSE_PASSWORD="your-password"
export WALLET_JSON='{"kty":"RSA",...}'
export AGGREGATOR_PROCESS_ID="your-ao-process-id"
```

### Run
```bash
node main.js
```

## CI/CD

Example GitHub Actions workflows are provided in `.github/workflows/`:

- `dbt.example.yml` — Automated dbt runs
- `ao-messenger.example.yml` — Scheduled AO message publishing

Copy and rename these files (remove `.example`) and configure the required secrets in your GitHub repository settings.

## Contributing

This project is archived and not accepting contributions. However, you're welcome to fork it and adapt it for your own use under the MIT license.

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Attribution

**Originally developed by [Roark Technology](https://roark.com)**

Built for the [Arweave](https://arweave.org) ecosystem.

---

<p align="center">
  <sub>
    🗄️ Archived • 📦 Reference Only • ⚖️ MIT Licensed
  </sub>
</p>
