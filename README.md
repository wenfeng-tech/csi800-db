# 🚀 中证800成分股数据同步工具

这是一个基于 GitHub Actions 的自动化工作流，用于定期抓取中证800（000906）所有成分股的日线交易数据，并将其同步到你的 **Supabase** 数据库中。

该工具旨在帮助个人开发者或量化分析爱好者轻松维护一个实时更新的、可靠的股票历史数据源，无需手动操作。

## ✨ 主要功能

- **自动化同步：** 利用 GitHub Actions 定时任务，每日自动执行增量数据抓取和同步。
- **灵活的手动任务：** 提供两个手动触发的工作流，用于执行全量或指定日期范围的历史数据同步。
- **数据来源：** 使用强大的 `akshare` 库，从可靠的金融数据源获取数据。
- **目标数据库：** 将处理后的数据高效地写入 Supabase 数据库。

## 🔧 如何使用

### 1. 配置环境变量

为了安全地连接到你的 Supabase 数据库，你需要在 GitHub 仓库中配置以下两个 Secrets。

- 进入你的 GitHub 仓库主页。
- 点击 **Settings** -> **Secrets and variables** -> **Actions**。
- 点击 **New repository secret**，分别添加以下两项：
  - **`SUPABASE_URL`**：你的 Supabase 项目 URL。
  - **`SUPABASE_KEY`**：你的 Supabase 服务角色密钥（`service_role key`），因为它需要写入权限。

### 2. 数据库设置

在你的 Supabase 项目中，你需要创建一个名为 `csi800_daily_data` 的数据表，并为其配置正确的列，以匹配脚本写入的数据格式。

**表名：** `csi800_daily_data`

**推荐的列及其数据类型：**

| 列名          | 数据类型       | 描述                 |
| :------------ | :------------- | :------------------- |
| `trade_date`  | `date` 或 `text` | 交易日期             |
| `stock_code`  | `text`         | 股票代码             |
| `stock_name`  | `text`         | 股票名称             |
| `open`        | `numeric`      | 开盘价               |
| `high`        | `numeric`      | 最高价               |
| `low`         | `numeric`      | 最低价               |
| `close`       | `numeric`      | 收盘价               |
| `volume`      | `numeric`      | 成交量               |

### 3. 运行工作流

该工作流现在分为三个独立的任务，以满足不同的同步需求：

| 工作流名称              | 触发方式   | 描述                                     |
| :---------------------- | :--------- | :--------------------------------------- |
| **Daily Data Sync** | 每日自动   | 每日增量更新，同步最近 5 天的数据。     |
| **Manual Full Sync** | 手动触发   | 全量同步所有成分股自 2005 年至今的数据。 |
| **Manual Partial Sync** | 手动触发   | 同步所有成分股自 **2015 年 1 月 1 日** 至今的数据。 |

#### 如何手动触发：

1.  进入 GitHub 仓库的 **Actions** 页面。
2.  在左侧列表中选择你想要运行的工作流，例如 **Manual Full Sync**。
3.  点击 **Run workflow** 按钮，即可立即执行。

---

### ⚙️ 技术栈

- **Python**：核心脚本语言
- **akshare**：用于获取股票数据
- **pandas**：用于数据处理和格式化
- **supabase-py**：用于与 Supabase 数据库交互
- **GitHub Actions**：自动化 CI/CD 工作流
