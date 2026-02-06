"""
按元素搜索示例：处理乱序输入，查找对应描述符表
"""
import duckdb
from pathlib import Path


VALID_ELEMENTS = ['Al', 'Co', 'Cr', 'Cu', 'Fe', 'Hf', 
                  'Mn', 'Mo', 'Nb', 'Ni', 'Ta', 'Ti', 'V', 'W', 'Zr']

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


def find_table_id(conn, elements: list[str]) -> int | None:
    """
    输入: 6个元素符号（顺序无关，大小写无关）
    输出: 对应的表 ID，未找到返回 None
    
    匹配逻辑: capitalize + 集合相等
    """
    # 标准化输入（首字母大写）
    user_elems = set(e.strip().capitalize() for e in elements)
    valid_set = set(VALID_ELEMENTS)
    
    if len(user_elems) != 6:
        raise ValueError(f"需要恰好 6 种元素，提供了 {len(user_elems)} 种")
    
    invalid = user_elems - valid_set
    if invalid:
        raise ValueError(f"无效元素: {invalid}，有效元素: {VALID_ELEMENTS}")
    
    # 查询所有组合并匹配（集合相等，不考虑顺序）
    rows = conn.execute(
        "SELECT id, elem1, elem2, elem3, elem4, elem5, elem6 FROM hea_elements_6;"
    ).fetchall()
    
    for row in rows:
        db_elems = {row[1], row[2], row[3], row[4], row[5], row[6]}
        if db_elems == user_elems:
            return row[0]
    
    return None


def query_by_elements(conn, elements: list[str], limit: int = 10):
    """按元素查询描述符数据"""
    
    # 查找表 ID
    table_id = find_table_id(conn, elements)
    if table_id is None:
        available = ", ".join(sorted(elements))
        raise ValueError(f"未找到元素组合: {available}")
    
    print(f"找到表 ID: {table_id} (hea_6_c_{table_id})")
    
    # 查询元素详细信息
    elem_info = conn.execute(
        "SELECT * FROM hea_elements_6 WHERE id = ?;", [table_id]
    ).fetchone()
    print(f"元素组合: {elem_info[1:7]}")
    
    # 查询描述符数据（指定列）
    table_name = f"hea_6_c_{table_id}"
    result = conn.execute(f"""
        SELECT con_index, ave_fe1, ave_fe2, ave_fp1, hmix_data 
        FROM {table_name} 
        LIMIT {limit};
    """).fetchall()
    
    return result


def main():
    print("=" * 60)
    print("按元素搜索示例")
    print("=" * 60)
    
    conn = connect_healake()
    
    # 测试 1: 标准顺序
    print("\n测试1: ['Fe', 'Ni', 'Mn', 'Al', 'Cr', 'Cu']")
    try:
        result = query_by_elements(conn, ['Fe', 'Ni', 'Mn', 'Al', 'Cr', 'Cu'], limit=3)
        for row in result:
            print(f"  con_index={row[0]}, ave_fe1={row[1]:.4f}")
    except ValueError as e:
        print(f"  错误: {e}")
    
    # 测试 2: 乱序输入（应返回相同结果）
    print("\n测试2: ['Al', 'Cr', 'Cu', 'Fe', 'Mn', 'Ni'] (乱序)")
    try:
        result = query_by_elements(conn, ['Al', 'Cr', 'Cu', 'Fe', 'Mn', 'Ni'], limit=3)
        for row in result:
            print(f"  con_index={row[0]}, ave_fe1={row[1]:.4f}")
    except ValueError as e:
        print(f"  错误: {e}")
    
    # 测试 3: 大小写混合
    print("\n测试3: ['fe', 'NI', 'Mn', 'al', 'CR', 'cu'] (大小写混合)")
    try:
        result = query_by_elements(conn, ['fe', 'NI', 'Mn', 'al', 'CR', 'cu'], limit=3)
        for row in result:
            print(f"  con_index={row[0]}, ave_fe1={row[1]:.4f}")
    except ValueError as e:
        print(f"  错误: {e}")
    
    # 测试 4: 无效元素
    print("\n测试4: ['Fe', 'Ni', 'Mn', 'Al', 'Cr', 'Xx'] (无效元素)")
    try:
        result = query_by_elements(conn, ['Fe', 'Ni', 'Mn', 'Al', 'Cr', 'Xx'], limit=3)
    except ValueError as e:
        print(f"  预期错误: {e}")
    
    # 测试 5: 元素数量不对
    print("\n测试5: ['Fe', 'Ni', 'Mn', 'Al', 'Cr'] (只有5种)")
    try:
        result = query_by_elements(conn, ['Fe', 'Ni', 'Mn', 'Al', 'Cr'], limit=3)
    except ValueError as e:
        print(f"  预期错误: {e}")
    
    conn.close()
    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
