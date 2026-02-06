"""
基础查询示例：连接数据库并执行简单查询
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
    
    # S3 配置
    conn.execute("SET s3_endpoint='idmlakehouse.tmslab.cn';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl='false';")
    
    # Attach 数据湖
    conn.execute(f"""
        ATTACH 'ducklake:{METADATA_PATH}' as hea (
            DATA_PATH 's3://idmdatabase/hea'
        );
    """)
    conn.execute("USE hea;")
    
    return conn


def main():
    # 1. 连接
    print("连接数据库...")
    conn = connect_healake()
    
    # 2. 验证连接 - 查看表数量
    tables = conn.execute("SHOW TABLES;").fetchall()
    print(f"连接成功！共有 {len(tables)} 个表")
    
    # 3. 查看辅助表
    print("\n辅助表列表:")
    for t in tables[:10]:
        if not t[0].startswith("hea_6_c_"):
            print(f"  - {t[0]}")
    
    # 4. 查询描述符说明
    print("\n前5个描述符:")
    desc = conn.execute("SELECT * FROM descriptor_names LIMIT 5;").fetchall()
    for d in desc:
        print(f"  {d[0]}: {d[1]}")
    
    # 5. 查询特定表的数据（指定列 + LIMIT）
    print("\n查询 hea_6_c_1 表前5行（指定列）:")
    result = conn.execute("""
        SELECT con_index, ave_fe1, ave_fe2, hmix_data 
        FROM hea_6_c_1 
        LIMIT 5;
    """).fetchall()
    for row in result:
        print(f"  con_index={row[0]}, ave_fe1={row[1]:.4f}, ave_fe2={row[2]:.4f}, hmix={row[3]:.4f}")
    
    # 6. 带条件的查询
    print("\n查询 ave_fe1 > 1.7 的成分（前5个）:")
    result = conn.execute("""
        SELECT con_index, ave_fe1, hmix_data 
        FROM hea_6_c_1 
        WHERE ave_fe1 > 1.7 
        LIMIT 5;
    """).fetchall()
    for row in result:
        print(f"  con_index={row[0]}, ave_fe1={row[1]:.4f}, hmix={row[2]:.4f}")
    
    conn.close()
    print("\n查询完成！")


if __name__ == "__main__":
    main()
