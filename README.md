# Agent HEA6 DuckLake

> 🔧 **这是一个 AI Agent Skill** - 为编程助手提供高熵合金描述符数据查询能力

[![Skill](https://img.shields.io/badge/Type-Agent_Skill-blue)](SKILL.md)
[![Data](https://img.shields.io/badge/Data-17.5TB_DuckLake-green)](https://ducklake.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 给 Agent 的指引

如果你是正在阅读此文档的 AI Agent（如 Kimi Code CLI、Cursor、Claude Code 等），请按以下步骤获取本 Skill：

### Step 1: 安装 Skill

```bash
# 克隆到 Agent 的标准 skill 目录
mkdir -p ~/.config/agents/skills/
cd ~/.config/agents/skills/
git clone https://github.com/wolido/agent-hea6-ducklake.git
```

### Step 2: 阅读 SKILL.md

**重要**：完成安装后，立即阅读 SKILL.md 获取完整使用规范：

```bash
cat ~/.config/agents/skills/agent-hea6-ducklake/SKILL.md
```

SKILL.md 包含：
- 三层数据结构设计（hea_elements_6 → hea_con_6 → hea_6_c_{id}）
- 6 元素到表名的精确匹配逻辑
- 查询模式（M1-M6）
- 性能优化指南
- 故障排除

### Step 3: 验证安装

```python
import duckdb
from pathlib import Path

metadata = Path("~/.config/agents/skills/agent-hea6-ducklake/metadata/agent_hea6_ducklake.ducklake").expanduser()

conn = duckdb.connect()
conn.execute("INSTALL ducklake; LOAD ducklake;")
conn.execute("SET s3_endpoint='idmlakehouse.tmslab.cn';")
conn.execute("SET s3_url_style='path';")
conn.execute("SET s3_use_ssl='false';")
conn.execute(f"ATTACH 'ducklake:{metadata}' as hea (DATA_PATH 's3://idmdatabase/hea');")
conn.execute("USE hea;")

# 验证
tables = conn.execute("SHOW TABLES;").fetchall()
print(f"✅ Skill 安装成功！共 {len(tables)} 个表")
```

### 触发条件

当用户提出以下需求时，应该使用本 Skill：
- "查询高熵合金描述符数据"
- "根据元素组合找合金性质"
- "按 hmix_data/smix_data（混合焓/混合熵）筛选合金"
- "导出特定系统的描述符数据"
- 涉及 6 主元高熵合金的 194 个描述符查询

---

## ✨ 功能特性

安装本 Skill 后，Agent 可以为你执行以下操作：

### 1. 元素组合查询
- 输入任意 6 种元素（支持乱序、大小写混合），定位到对应的描述符表
- 示例：`["Fe", "Ni", "Mn", "Al", "Cr", "Cu"]` → `hea_6_c_1`

### 2. 描述符数据查询
- 访问 194 个描述符，包括：
  - **电子结构**: 电负性(ave_fe1-fe15)、电子浓度等
  - **物理性质**: 模量(ave_fp1-fp14)、密度、导电性等
  - **结构性质**: 原子半径(ave_fs1-fs9)、晶格常数等
  - **热力学数据**: 混合焓(hmix_data)、混合熵(smix_data)、原子尺寸失配(lambda_data)等

### 3. 成分比例 JOIN 查询
- 关联描述符与元素配比（con1-con6 表示 6 种元素的摩尔分数）
- 示例：查询 "电负性 > 1.7 且 Fe 含量 > 30% 的成分"

### 4. 跨系统批量搜索
- 在多个元素组合中筛选满足条件的合金
- 示例："搜索所有包含 CoCrFeMnNi 的系统中 hmix < -10 的成分"

### 5. 大数据导出
- 分批导出完整描述符表到 Parquet（避免内存溢出）
- 单表规模：1076 万行 × 194 列

### 6. 机器学习数据集构建
- 提取描述符作为特征（X）：194 个描述符 + 6 维成分比例
- 按描述符条件筛选样本（如 "hmix < -5 且 lambda > 0.1"）
- 导出为 CSV/Parquet 供模型训练
- ⚠️ **注意**：数据库只包含描述符，目标值（Y）需你自己提供或标注

### 7. 数据筛选与整合
- 根据描述符范围筛选候选成分（如 "混合焓在 -10 到 -5 之间"）
- 批量提取特定描述符用于后续分析
- 跨多表联合查询构建自定义数据视图

---

## 👤 给人类用户

如果你想让自己的 AI Agent 使用这个 Skill：

### 方式 1: 命令 Agent 安装（推荐）

直接告诉你的 Agent：

> "请安装 agent-hea6-ducklake skill：
> ```bash
> git clone https://github.com/wolido/agent-hea6-ducklake.git ~/.config/agents/skills/agent-hea6-ducklake
> ```
> 然后阅读 ~/.config/agents/skills/agent-hea6-ducklake/SKILL.md 了解如何使用。"

### 方式 2: 手动安装

```bash
# 1. 克隆项目
mkdir -p ~/.config/agents/skills/
cd ~/.config/agents/skills/
git clone https://github.com/wolido/agent-hea6-ducklake.git

# 2. 让你的 Agent 读取 SKILL.md
# 在 Agent 对话中输入：
# "请读取 ~/.config/agents/skills/agent-hea6-ducklake/SKILL.md"
```

### 数据范围

| 指标 | 数值 |
|------|------|
| 元素组合 | 5,005 种（15 元素选 6） |
| 成分/系统 | 1,076 万种 |
| 描述符 | 194 个 |
| 总数据量 | 500+ 亿条 |
| 本地占用 | 48 MB（仅元数据） |
| 原始数据 | 17.5 TB（云端 S3） |

### 支持的元素

`Al, Co, Cr, Cu, Fe, Hf, Mn, Mo, Nb, Ni, Ta, Ti, V, W, Zr`

---

## 🚀 快速使用示例

安装完成后，向你的 Agent 提问：

```
"查询 FeNiMnAlCrCu 系统中 ave_fe1 > 1.7 的成分"

"列出所有包含 CoCrFeMnNi 的组合"

"导出表 hea_6_c_1 的全部描述符数据到 Parquet"

"搜索 hmix_data < -10 的所有高熵合金"
```

Agent 会自动引用 SKILL.md 中的规范执行查询。

---

## 📁 项目结构

```
agent-hea6-ducklake/
├── SKILL.md              # 📖 Agent 必读：完整技能规范
├── README.md             # 本文件
├── examples/             # 示例代码
│   ├── basic_query.py
│   ├── search_by_elements.py
│   └── join_concentration.py
├── metadata/             # DuckLake 元数据 (~48MB)
│   └── agent_hea6_ducklake.ducklake
└── LICENSE               # MIT 许可证
```

---

## 🔗 相关链接

- **GitHub**: https://github.com/wolido/agent-hea6-ducklake
- **DuckLake**: https://ducklake.io/
- **DuckDB**: https://duckdb.org/

---

## 👨‍💻 作者

**小顺子** - Wolido 的 AI Agent

> 由小顺子（Kimi Code CLI Agent）为 Wolido 开发维护。

---

## License

MIT License - 详见 [LICENSE](LICENSE) 文件
