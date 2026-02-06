---
name: agent-hea6-ducklake
description: |
  当需要查询或分析高熵合金(HEA)描述符数据时使用。
  
  适用场景：
  - 根据6种元素组合查询对应的描述符数据表
  - 按描述符条件筛选满足特定性质的合金成分
  - 关联描述符与成分比例(con_index JOIN hea_con_6)
  - 跨多个元素组合批量搜索符合条件的成分
  - 导出特定系统的全部描述符数据用于ML训练
  
  数据范围：15个元素中任选6个组成的5005种高熵合金系统。
---

# HEA DuckLake Skill

高熵合金（HEA）描述符数据湖连接与查询规范。

---

## 1. 核心概念

### 1.1 这是什么？

- **数据内容**：6 主元高熵合金的描述符数据
- **数据规模**：5005 种元素组合 × 1076 万成分 × 194 个描述符 = 500+ 亿条数据
- **存储形式**：DuckLake 数据湖（实际数据在 S3，本地仅需 48MB 元数据）
- **物理大小**：原始数据 17.5TB，压缩列式存储

### 1.2 三层数据结构

```yaml
# Layer 1: 元素组合映射表
hea_elements_6:
  primary_key: id              # 范围: 1-5005
  columns: [id, elem1, elem2, elem3, elem4, elem5, elem6]
  purpose: 6种元素符号 → 描述符表ID
  row_count: 5005
  example: {id: 1, elems: [Fe, Ni, Mn, Al, Cr, Cu], table: hea_6_c_1}

# Layer 2: 成分比例表
hea_con_6:
  primary_key: id              # 又名 con_index, 范围: 1-10762647
  columns: [id, con1, con2, con3, con4, con5, con6]
  purpose: 成分唯一标识 → 6种元素的比例(和为1)
  row_count: 10762647

# Layer 3: 描述符数据表 (5005个表, 命名: hea_6_c_{id})
hea_6_c_{id}:
  primary_key: con_index       # 关联 hea_con_6.id
  columns: [con_index] + 194个描述符列
  purpose: 描述符数据
  row_count: 10762647 per table
  naming: id来自hea_elements_6.id, 如hea_6_c_1, hea_6_c_2等

# 关联关系
relationships:
  - from: hea_elements_6.id
    to: hea_6_c_{id}          # 通过id构造表名
    via: "6元素集合相等匹配（见下方精确逻辑）"
  
  - from: hea_6_c_{id}.con_index
    to: hea_con_6.id
    via: "直接相等"
```

**查询路径代码化：**
```python
# 路径1: 6种元素 → 描述符表名（精确匹配逻辑）
def elements_to_table(conn, elements: list[str]) -> str:
    """
    通过6种元素查找对应的描述符表名。
    
    元素符号大小写规范（如 Fe, Al, Ni，首字母大写）。
    匹配时不考虑输入顺序，但大小写必须标准化。
    
    算法：
    1. 标准化用户输入（首字母大写，如 'fe'→'Fe', 'FE'→'Fe'）
    2. 验证6个元素都在有效列表中
    3. 与 hea_elements_6 表的 elem1-6 比较集合相等性
    4. 返回匹配的表名 hea_6_c_{id}
    """
    VALID_ELEMENTS = ['Al', 'Co', 'Cr', 'Cu', 'Fe', 'Hf', 
                      'Mn', 'Mo', 'Nb', 'Ni', 'Ta', 'Ti', 'V', 'W', 'Zr']
    
    if len(elements) != 6:
        raise ValueError(f"必须提供恰好6种元素，实际提供了{len(elements)}种")
    
    # 步骤1: 标准化用户输入（capitalize）
    # 'fe' -> 'Fe', 'FE' -> 'Fe', 'Fe' -> 'Fe'
    user_elems = set(e.strip().capitalize() for e in elements)
    
    # 步骤2: 验证有效性
    invalid = user_elems - set(VALID_ELEMENTS)
    if invalid:
        raise ValueError(f"无效元素: {invalid}。有效元素: {VALID_ELEMENTS}")
    
    # 步骤3: 查询并匹配（数据库中elem1-6已是规范大小写）
    rows = conn.execute("SELECT id, elem1, elem2, elem3, elem4, elem5, elem6 FROM hea_elements_6;").fetchall()
    
    for row in rows:
        # 数据库中的元素已是规范大小写（如 Fe, Ni, Mn, Al, Cr, Cu）
        db_elems = {row[1], row[2], row[3], row[4], row[5], row[6]}
        if db_elems == user_elems:
            return f"hea_6_c_{row[0]}"
    
    raise ValueError(f"未找到元素组合: {elements}")

# 路径2: 描述符表 + con_index → 成分比例
def get_concentration(table: str, con_index: int):
    return query(f"""
        SELECT t.*, c.con1, c.con2, ..., c.con6
        FROM {table} t
        LEFT JOIN hea_con_6 c ON t.con_index = c.id
        WHERE t.con_index = {con_index}
    """)
```

### 1.3 关键字段

```yaml
hea_elements_6.id:
  type: INTEGER
  range: 1-5005
  usage: 描述符表 hea_6_c_{id} 的编号

hea_elements_6.elem1-elem6:
  type: VARCHAR
  format: 元素符号首字母大写（如 Fe, Al, Ni）
  valid_values: [Al, Co, Cr, Cu, Fe, Hf, Mn, Mo, Nb, Ni, Ta, Ti, V, W, Zr]
  constraint: |
    输入的6种元素顺序无关，但必须是这15个中的6个不同元素。
    如 ['Al', 'Cr', 'Cu', 'Fe', 'Mn', 'Ni'] 与 ['Fe', 'Ni', 'Mn', 'Al', 'Cr', 'Cu'] 是同一组合。

con_index:
  type: INTEGER
  range: 1-10762647
  aliases: [hea_con_6.id, hea_6_c_{id}.con_index]
  usage: 关联描述符表和成分表的唯一标识
```

---

## 2. 连接方法

### 2.1 元数据位置

Skill 内置元数据文件：
```
~/.config/agents/skills/agent-hea6-ducklake/metadata/agent_hea6_ducklake.ducklake
```

### 2.2 标准连接代码

```python
import duckdb
from pathlib import Path

def connect_healake():
    """连接 HEA DuckLake 数据湖"""
    skill_dir = Path("~/.config/agents/skills/agent-hea6-ducklake").expanduser()
    metadata = skill_dir / "metadata/agent_hea6_ducklake.ducklake"
    
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake;")
    conn.execute("LOAD ducklake;")
    
    # S3 配置
    conn.execute("SET s3_endpoint='idmlakehouse.tmslab.cn';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl='false';")
    
    # Attach 数据湖
    conn.execute(f"""
        ATTACH 'ducklake:{metadata}' as hea (
            DATA_PATH 's3://idmdatabase/hea'
        );
    """)
    conn.execute("USE hea;")
    
    return conn

# 使用
conn = connect_healake()
```

### 2.3 连接验证

```python
# 步骤1: 验证连接成功
tables = conn.execute("SHOW TABLES;").fetchall()
assert len(tables) == 5008, f"期望5008个表，实际{len(tables)}个"

# 步骤2: 确认辅助表存在
required_tables = {'hea_elements_6', 'hea_con_6', 'descriptor_names'}
existing_tables = {t[0] for t in tables}
assert required_tables.issubset(existing_tables), f"缺少辅助表: {required_tables - existing_tables}"

# 步骤3: 设置查询限制（建议）
DEFAULT_LIMIT = 100  # 所有查询默认加LIMIT，防止OOM
```

---

## 3. 查询模式

### M1: 列出元素组合

```python
# 列出所有 5005 种元素组合
all_combinations = conn.execute("SELECT * FROM hea_elements_6;").fetchdf()

# 或查看前 N 个
sample = conn.execute("SELECT * FROM hea_elements_6 LIMIT 20;").fetchdf()
```

### M2: 6元素→表名转换

```python
def find_table_id(conn, elements: list[str]) -> int | None:
    """
    输入: 6个元素符号（标准格式，顺序无关）
    输出: 对应的表 ID，未找到返回 None
    
    数据库中元素为标准格式（首字母大写，如 Fe, Al, Ni）
    匹配逻辑: 集合相等（不考虑顺序）
    """
    # 标准化输入（容错处理）
    user_elems = set(e.strip().capitalize() for e in elements)
    
    # 查询并比较集合
    rows = conn.execute("SELECT id, elem1, elem2, elem3, elem4, elem5, elem6 FROM hea_elements_6;").fetchall()
    
    for row in rows:
        db_elems = {row[1], row[2], row[3], row[4], row[5], row[6]}
        if db_elems == user_elems:
            return row[0]
    return None

# 使用（输入顺序无关）
table_id = find_table_id(conn, ["Al", "Cr", "Cu", "Fe", "Mn", "Ni"])  # 返回 1
table_id = find_table_id(conn, ["Fe", "Ni", "Mn", "Al", "Cr", "Cu"])  # 同样返回 1
```

### M3: 单系统条件查询

```python
def query_single_system(
    conn,
    table_id: int,
    columns: list[str] = None,
    filters: dict = None,
    limit: int = 100,
    as_dataframe: bool = True
):
    """
    查询特定元素组合中满足条件的成分
    
    Args:
        as_dataframe: True 返回 DataFrame（小数据量），False 返回 list（省内存）
    """
    table_name = f"hea_6_c_{table_id}"
    cols_str = ", ".join(columns) if columns else "*"
    
    where_clauses = []
    params = []
    if filters:
        for col, (op, val) in filters.items():
            where_clauses.append(f"{col} {op} ?")
            params.append(val)
    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    sql = f"SELECT {cols_str} FROM {table_name} {where_str} LIMIT {limit}"
    result = conn.execute(sql, params)
    
    if as_dataframe:
        return result.fetchdf()  # 小数据量用
    else:
        return result.fetchall()  # 大数据量用，返回 list of tuples

# 示例1: 小数据量，直接转 DataFrame
df = query_single_system(conn, table_id=1, columns=["con_index", "ave_fe1"], limit=100)

# 示例2: 大数据量，逐行处理节省内存
rows = query_single_system(conn, table_id=1, columns=["con_index", "ave_fe1"], 
                           limit=100000, as_dataframe=False)
for row in rows:
    con_index, ave_fe1 = row
    # 处理每一行...
```

### M4: 跨系统批量搜索

```python
# ⚠️ 警告：跨 5005 张表查询，必须分批和限制
def search_across_systems(
    conn,
    columns: list[str],
    filters: dict,
    max_systems: int = None,    # 最多查多少系统
    max_total: int = 10000      # 最多返回多少条
):
    """在多个元素组合中搜索满足条件的成分"""
    
    # 获取目标系统 ID 列表
    if max_systems:
        ids = list(range(1, max_systems + 1))
    else:
        ids = list(range(1, 5006))  # 全部 5005 个
    
    all_results = []
    remaining = max_total
    
    for tid in ids:
        if remaining <= 0:
            break
            
        try:
            rows = query_single_system(conn, tid, columns, filters, limit=remaining, as_dataframe=False)
            # 逐行处理，避免 DataFrame 内存开销
            for row in rows:
                # 处理每一行...
                pass
            df["hea_system_id"] = tid  # 标记来源
            all_results.append(df)
            remaining -= len(df)
        except Exception as e:
            print(f"查询 hea_6_c_{tid} 失败: {e}")
            continue
    
    # 合并结果
    import pandas as pd
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
```

### M5: 成分比例 JOIN 查询

```python
def query_with_concentration(conn, table_id: int, columns: list[str] = None, filters: dict = None, limit: int = 10):
    """
    查询描述符并关联成分比例
    
    Args:
        table_id: 元素组合 ID
        columns: 要查询的描述符列，None 则查询全部
        filters: 过滤条件，如 {"ave_fe1": (">", 1.7)}
        limit: 返回行数限制
    """
    table_name = f"hea_6_c_{table_id}"
    
    # 选择描述符列
    cols_str = ", ".join(columns) if columns else f"{table_name}.*"
    
    # 构建 WHERE
    where_clauses = []
    params = []
    if filters:
        for col, (op, val) in filters.items():
            where_clauses.append(f"{table_name}.{col} {op} ?")
            params.append(val)
    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    sql = f"""
    SELECT 
        {table_name}.con_index,
        hea_con_6.con1, hea_con_6.con2, hea_con_6.con3,
        hea_con_6.con4, hea_con_6.con5, hea_con_6.con6,
        {cols_str}
    FROM {table_name}
    LEFT JOIN hea_con_6 ON {table_name}.con_index = hea_con_6.id
    {where_str}
    LIMIT {limit}
    """
    # 小数据量用 fetchdf()，大数据量建议用 fetchall()
    return conn.execute(sql, params).fetchdf()

# 示例：查询表 id=1 中 ave_fe1 > 1.7 的成分及比例
df = query_with_concentration(conn, table_id=1, columns=["ave_fe1", "hmix_data"], filters={"ave_fe1": (">", 1.7)}, limit=10)
```

### M6: 全表导出

```python
# ⚠️ 严重警告：单表 1076 万行 × 195 列 (含 con_index) ≈ 30GB 内存（Pandas）
def export_full_table(
    conn,
    table_id: int,
    output_path: str,
    batch_size: int = 100000
):
    """
    分批导出全表数据到 Parquet
    使用分批查询避免内存溢出
    """
    import pyarrow.parquet as pq
    import pyarrow as pa
    
    table_name = f"hea_6_c_{table_id}"
    offset = 0
    writer = None
    
    while True:
        # 分批查询
        query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        df = conn.execute(query).fetchdf()
        
        if len(df) == 0:
            break
        
        # 转换为 Arrow 并写入（使用 fetchdf() 因为已经分批，每批可控）
        table = pa.Table.from_pandas(df)
        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema)
        writer.write_table(table)
        
        offset += batch_size
        print(f"已导出 {offset} 行...")
    
    if writer:
        writer.close()
    print(f"导出完成: {output_path}")
```

---

## 4. 描述符说明

### 4.1 描述符命名规则

基于 `descriptor_names` 表的实际数据：

```yaml
前缀:
  ave_:  6种元素的平均值 (the average value of...)
  pair_: 二元对的加权平均值 (weighted average of... of binary pairs)
  
后缀:
  fe1-fe15: 电子结构相关 (electronegativity, ionization energy, valence electron等)
  fp1-fp14: 物理性质相关 (modulus, density, conductivity, hardness等)
  fs1-fs9:  结构性质相关 (atomic radius, lattice constant等)
  
特殊后缀:
  _data: 热力学/物理计算数据
    - hmix_data: 混合焓
    - smix_data: 混合熵
    - lambda_data: 原子尺寸失配 Λ
    - gamma_data: 拓扑描述符 γ
    - omega_data: 参数
    - ev_data: 空位形成能
    - gg0_data: 剪切模量比 G/G0
    - tbtm_data: 熔点相关
```

### 4.2 描述符查询方法

```python
# 列出所有描述符及其说明
all_descriptors = conn.execute("SELECT * FROM descriptor_names;").fetchdf()

# 按关键词搜索描述符
search_results = conn.execute("""
    SELECT * FROM descriptor_names 
    WHERE description LIKE '%electronegativity%'
    OR description LIKE '%modulus%'
""").fetchdf()
```

### 4.3 描述符命名规律与反查方法

**命名结构**: `{前缀}{编号}` 或 `{名称}_data`

```yaml
前缀含义:
  ave_:   6种元素的简单平均值 (average)
  pair_:  二元对的加权平均值 (pairwise weighted average)
  rmse_:  均方根误差 (root mean square error)
  range_: 最大值与最小值之差 (range = max - min)

编号含义:
  fe1-fe15: 电子结构相关 (electronegativity, ionization energy, electron concentration等)
  fp1-fp14: 物理性质相关 (modulus, density, conductivity, hardness等)
  fs1-fs9:  结构性质相关 (atomic radius, molar volume等)
  ft1-ft8:  温度相关 (melting temperature, boiling temperature等)

_data后缀:
  表示热力学或物理计算数据，非元素属性直接计算:
    - hmix_data: 混合焓
    - smix_data: 混合熵
    - lambda_data: 原子尺寸失配
    - gamma_data: 拓扑描述符
    - omega_data: 参数Ω
    - tbtm_data: 熔点相关
    - ev_data: 空位形成能
    - gg0_data: 剪切模量比
    - kg_data: 体积模量比
    - rmse_hmix_data: 混合焓的RMSE
```

**反查方法（必须先查 descriptor_names 表）**:

```python
# 步骤1: 必须先看 descriptor_names 表的全貌
all_descriptors = conn.execute("SELECT name, description FROM descriptor_names;").fetchdf()
# 浏览或搜索这个 DataFrame，理解有哪些描述符可用

# 步骤2: 根据需求定位描述符
# 例如: 需要"电负性"相关的描述符
# 先在 all_descriptors 中搜索关键词 "electronegativity"
# 找到: ave_fe1 (Pauling), ave_fe2 (Allred-Rochow), pair_fe1, range_fe1 等

# 步骤3: 根据命名规律扩展
# 找到 fe1 相关后，理解命名规律:
#   ave_fe1 = 6元素平均Pauling电负性
#   pair_fe1 = 二元对加权Pauling电负性  
#   range_fe1 = Pauling电负性最大最小差
#   rmse_fe1 = Pauling电负性RMSE

# 禁止: 不查表直接猜测描述符名称
# 错误: 猜测 "electronegativity_mean" 或 "en_avg"
# 正确: 先查 descriptor_names 确认实际是 "ave_fe1"
```

**重要**: 194个描述符的名称和含义必须通过 `descriptor_names` 表确认，不可凭经验猜测。

---

## 5. 性能与安全

### 5.1 内存限制与查询策略

```yaml
场景1: 单表全量查询 (1076万行 × 195列)
  转Pandas: 约30GB内存，64GB+内存机器可行
  不转DataFrame (fetchall): 约4-8GB内存消耗
  分批导出 (Parquet): 内存占用可控，任何机器都可
  
场景2: 单列/少列全量查询
  例如只查 con_index + ave_fe1: 约1-2GB (Pandas)
  建议: 指定必要列，避免 SELECT *

场景3: 小结果集查询
  LIMIT 1000以下: 安全，可直接转Pandas
  内存占用: 几十MB到几百MB

决策流程:
  IF 结果行数 < 10000:
      使用 fetchdf() 转 Pandas，方便处理
  ELIF 机器内存 >= 64GB AND 需要全表:
      可使用 fetchall() 逐行处理，或分批处理
  ELSE:
      必须分批查询 (LIMIT/OFFSET) 或只查必要列
```

### 5.2 并发限制

**重要**：DuckLake 元数据文件不支持多进程并发访问。

- ✅ 单个 Python 脚本内顺序查询
- ❌ 多进程同时连接同一元数据文件
- ✅ 解决方案：复制多份元数据文件，或改用 SQLite 转换

### 5.3 列式存储优化（重要）

**数据存储格式**: 列式存储（Parquet），对列裁剪极度友好

```yaml
优势:
  只查必要列: 只读取需要的列，不读的不从S3传输
  代价对比:
    SELECT con_index, ave_fe1 FROM hea_6_c_1:  传输约 80MB (2列 × 1000万行)
    SELECT * FROM hea_6_c_1:                    传输约 4GB (195列 × 1000万行)
    倍数: 约50倍差距

最佳实践:
  1. 永远不要 SELECT *，明确列出需要的列
  2. 分析前确定需要哪些描述符，只查那些列
  3. 多列查询时，列数增加与传输量线性增长
  4. 利用 DuckDB 的列式特性，查询计划会自动优化
```

```python
# ✅ 好的查询 - 只查必要列
sql = "SELECT con_index, ave_fe1, ave_fe2, hmix_data FROM hea_6_c_1 WHERE ave_fe1 > 1.7 LIMIT 100"

# ❌ 差的查询 - 传输大量无用数据
sql = "SELECT * FROM hea_6_c_1 LIMIT 100"  # 即使LIMIT小，也要读取所有列的元数据
```

### 5.4 网络优化

```python
# 1. 行限制 - 加 LIMIT 减少结果行数
SELECT con_index, ave_fe1 FROM hea_6_c_1 LIMIT 100;

# 2. 条件过滤 - WHERE 减少传输行数
SELECT con_index, ave_fe1 FROM hea_6_c_1 WHERE ave_fe1 > 1.7 LIMIT 100;

# 3. 避免 SELECT * 即使加了 LIMIT
# ❌ SELECT * FROM hea_6_c_1 LIMIT 100   still needs to read all column metadata
```

### 5.5 工具选择

```yaml
大数据量查询:
  推荐: DuckDB 原生操作 (fetchall)
  理由: 不转换DataFrame，内存占用最小
  备选: Polars (惰性加载)

小数据量查询 (<1万行):
  推荐: Pandas (fetchdf)
  理由: 生态丰富，后续分析方便

数据导出:
  推荐: PyArrow 写 Parquet
  理由: 流式处理，不占用内存

复杂分析:
  推荐: SQL + Python
  理由: SQL处理过滤聚合，Python处理业务逻辑
```

---

## 6. 故障排除

### 6.1 "表不存在"

**原因**：
- 元素组合不在 5005 个有效组合中
- 表名拼写错误（注意 `hea_6_c_{id}` 格式）
- id 超出 1-5005 范围

**解决**：
```python
# 先验证元素组合是否存在
def validate_elements(elements):
    # 标准化输入（首字母大写）
    normalized = set(e.strip().capitalize() for e in elements)
    valid_set = set(VALID_ELEMENTS)
    
    if len(normalized) != 6:
        raise ValueError(f"需要恰好 6 种元素，提供了 {len(normalized)} 种")
    
    invalid = normalized - valid_set
    if invalid:
        raise ValueError(f"无效元素: {invalid}")
```

### 6.2 "内存溢出 (OOM)"

**原因**：
- 使用了 `SELECT *` 导致传输数据量过大
- 将大数据量转换为 Pandas DataFrame
- 没有限制结果行数

**解决**：
```python
# 方案1: 使用 DuckDB 原始操作（推荐，最省内存）
# 使用 fetchall() 返回 list of tuples，不转换 DataFrame
rows = conn.execute("SELECT con_index, ave_fe1 FROM hea_6_c_1 LIMIT 10000").fetchall()
for row in rows:
    con_index, ave_fe1 = row
    # 处理数据...

# 方案2: 分批查询（超大表）
batch_size = 100000
offset = 0
while True:
    rows = conn.execute(f"SELECT ... FROM hea_6_c_1 LIMIT {batch_size} OFFSET {offset}").fetchall()
    if not rows:
        break
    # 处理本批数据...
    offset += batch_size

# 方案3: 只查必要列 + 加 LIMIT
# 避免 SELECT *，明确指定需要的列
```

**避免**: 大数据量时直接使用 `fetchdf()` 转为 Pandas

### 6.3 "连接被拒绝 / 数据库被锁定"

**原因**：多个进程同时访问同一 `.ducklake` 文件

**解决**：
- 确保单进程访问
- 或复制元数据文件：`cp agent_hea6_ducklake.ducklake agent_hea6_ducklake_copy.ducklake`

### 6.4 查询结果为空

**检查**：
- `con_index` 是否在 1-10762647 范围内
- 过滤条件是否过于严格
- 元素组合是否正确匹配

---

## 7. 示例代码

见 `examples/` 目录：
- `basic_query.py` - 基础查询示例
- `search_by_elements.py` - 按元素搜索
- `join_concentration.py` - JOIN 成分数据


