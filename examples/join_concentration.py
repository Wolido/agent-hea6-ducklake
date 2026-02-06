"""
JOIN 查询示例：关联描述符表和成分比例表
"""
import duckdb
from pathlib import Path


METADATA_PATH = Path.home() / ".config/agents/skills/agent-hea6-ducklake/metadata/agent_hea6_ducklake.ducklake"


def connect_healake():
    """连接 HEA DuckLake 数据湖"""
    if not METADATA_PATH.exists():
        raise FileNotFoundError(
            f"元数据文件不存在: {METADATA_PATH}\n"
            "请先安装 skill:\n"
            "git clone https://github.com/wolido/agent-hea6-ducklake.git "
            "~/.config/agents/skills/agent-hea6-ducklake"
        )
    
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake;")
    conn.execute("LOAD ducklake;")
    conn.execute("SET s3_endpoint='idmlakehouse.tmslab.cn';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl='false';")
    conn.execute(f"""
        ATTACH 'ducklake:{METADATA_PATH}' as hea (DATA_PATH 's3://idmdatabase/hea');
    """)
    conn.execute("USE hea;")
    
    return conn


def query_with_concentration(conn, table_id: int, filters: dict = None, limit: int = 10):
    """
    查询描述符同时获取成分比例
    
    Args:
        table_id: 元素组合 ID (1-5005)
        filters: 过滤条件，如 {"ave_fe1": (">", 1.7)}
        limit: 返回行数限制
    """
    table_name = f"hea_6_c_{table_id}"
    
    # 获取元素信息
    elem_info = conn.execute(
        "SELECT * FROM hea_elements_6 WHERE id = ?;", [table_id]
    ).fetchone()
    elements = elem_info[1:7] if elem_info else None
    
    # 构建 WHERE 子句
    where_clauses = []
    params = []
    if filters:
        for col, (op, val) in filters.items():
            where_clauses.append(f"{table_name}.{col} {op} ?")
            params.append(val)
    
    where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    # JOIN 查询
    sql = f"""
    SELECT 
        {table_name}.con_index,
        hea_con_6.con1, hea_con_6.con2, hea_con_6.con3,
        hea_con_6.con4, hea_con_6.con5, hea_con_6.con6,
        {table_name}.ave_fe1, 
        {table_name}.ave_fe2,
        {table_name}.hmix_data
    FROM {table_name}
    LEFT JOIN hea_con_6 ON {table_name}.con_index = hea_con_6.id
    {where_str}
    LIMIT {limit}
    """
    
    result = conn.execute(sql, params).fetchall()
    return elements, result


def main():
    print("=" * 80)
    print("JOIN 查询示例：描述符 + 成分比例")
    print("=" * 80)
    
    conn = connect_healake()
    
    # 查询 FeNiMnAlCrCu 系统 (id=1)
    table_id = 1
    
    print(f"\n查询元素组合 ID={table_id}")
    
    # 示例 1: 基础 JOIN 查询
    print("\n【示例1】基础 JOIN 查询（前5行）")
    elements, result = query_with_concentration(conn, table_id, limit=5)
    print(f"元素: {elements}")
    print(f"{'con_index':>10} | {'Fe':>8} | {'Ni':>8} | {'Mn':>8} | {'Al':>8} | {'Cr':>8} | {'Cu':>8} | {'ave_fe1':>10}")
    print("-" * 90)
    for row in result:
        print(f"{row[0]:>10} | {row[1]:>8.4f} | {row[2]:>8.4f} | {row[3]:>8.4f} | {row[4]:>8.4f} | {row[5]:>8.4f} | {row[6]:>8.4f} | {row[7]:>10.4f}")
    
    # 示例 2: 带条件的 JOIN 查询
    print("\n【示例2】ave_fe1 > 1.7 的成分（前5行）")
    elements, result = query_with_concentration(
        conn, table_id, 
        filters={"ave_fe1": (">", 1.7)}, 
        limit=5
    )
    print(f"元素: {elements}")
    print(f"{'con_index':>10} | {'Fe':>8} | {'Ni':>8} | {'ave_fe1':>10} | {'hmix':>10}")
    print("-" * 65)
    for row in result:
        print(f"{row[0]:>10} | {row[1]:>8.4f} | {row[2]:>8.4f} | {row[7]:>10.4f} | {row[9]:>10.4f}")
    
    # 示例 3: 查询特定 con_index
    print("\n【示例3】查询特定 con_index = 1000000")
    target_con_index = 1000000
    result = conn.execute("""
    SELECT 
        hea_6_c_1.con_index,
        hea_con_6.con1, hea_con_6.con2, hea_con_6.con3,
        hea_con_6.con4, hea_con_6.con5, hea_con_6.con6,
        hea_6_c_1.ave_fe1, hea_6_c_1.hmix_data
    FROM hea_6_c_1
    LEFT JOIN hea_con_6 ON hea_6_c_1.con_index = hea_con_6.id
    WHERE hea_6_c_1.con_index = ?
    """, [target_con_index]).fetchone()
    
    if result:
        print(f"找到成分:")
        print(f"  con_index: {result[0]}")
        print(f"  成分比例: Fe={result[1]:.4f}, Ni={result[2]:.4f}, Mn={result[3]:.4f}, Al={result[4]:.4f}, Cr={result[5]:.4f}, Cu={result[6]:.4f}")
        print(f"  描述符: ave_fe1={result[7]:.4f}, hmix={result[8]:.4f}")
    else:
        print(f"未找到 con_index={target_con_index}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("查询完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()
