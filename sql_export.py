# cms_export.py
# 功能说明：
# 1. 从指定文件读取SQL查询语句
# 2. 连接Oracle数据库执行查询
# 3. 根据查询结果行数自动分块导出Excel文件（超过50万行时分20万行/块）
# 4. 全程日志记录（包含操作日志、错误追踪、资源释放状态）
# 5. 自动创建缺失的SQL参数文件
# 6. 支持运行时动态生成带时间戳的日志文件
# 7. 使用tqdm显示查询进度条
# 8. 异常处理机制确保资源安全释放

import os
import sys
import traceback
import oracledb
import sqlparse
import pandas as pd
from tqdm import tqdm
import logging
from datetime import datetime

# 日志系统初始化
def setup_logging():
    """初始化日志系统，创建带时间戳的独立日志文件并配置输出格式"""
    log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        filename=log_filename,          # 日志文件名
        level=logging.INFO,             # 最低记录级别
        format="%(asctime)s [%(levelname)s] %(message)s",  # 日志格式（时间+级别+消息）
        datefmt="%Y-%m-%d %H:%M:%S"     # 时间格式
    )
    # 添加控制台输出处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(console_handler)

setup_logging()  # 脚本启动时立即初始化日志
def read_db_config():
    """从database.txt读取数据库配置，不存在则创建示例配置"""
    config = {}
    if not os.path.exists('database.txt'):
        # 写入示例配置
        with open('database.txt', 'w') as f:
            f.write("user=123\n")
            f.write("password=456\n")
            f.write("dsn=localhost:1521/orcl\n")
        logging.info("database.txt不存在，已创建默认配置。")
        logging.info("请编辑database.txt配置文件")
        sys.exit(0)
    # 读取配置文件
    with open('database.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if line and '=' in line:
                key, value = line.split('=', 1)
                config[key] = value
    return config

def read_sql_from_file(file_path):
    """读取SQL文件内容，若文件不存在则创建示例文件并返回默认SQL"""
    if not os.path.exists(file_path):
        # 自动创建缺失的SQL文件
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("SELECT * FROM dual\n")
        logging.info(f"文件 {file_path} 自动生成成功！")
    # 读取文件内容并去除首尾空白字符
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read().strip()

def validate_sql(sql):
    """SQL语法验证函数"""
    try:
        parsed = sqlparse.parse(sql)
        if not parsed:
            raise ValueError("无效的 SQL 语句")
    except Exception as e:
        logging.error(f"SQL 校验失败: {e}")
        raise

def execute_query_and_export_to_excel(sql):
    """核心执行函数：数据库查询与结果导出"""
    connection = None
    cursor = None
    try:
        # 设置 Oracle Instant Client 路径
        # 注意：请将以下路径替换为你的 Instant Client 安装目录
        # 例如：Windows: "C:/oracle/instantclient_11_2"
        #       Linux/Mac: "/opt/oracle/instantclient_11_2"
        #修改为解压同级目录下zip压缩包，然后使用其目录
        #压缩包名称instantclient-basic-windows.x64-11.2.0.4.0.zip,目录名称为instantclient_11_2
        if getattr(sys, 'frozen', False):
            # 当前是打包后的 EXE，使用 PyInstaller 的临时目录
            base_dir = sys._MEIPASS
        else:
            base_dir = os.path.abspath(".")
        # ==== 新增：自动解压Oracle Instant Client ZIP文件 ====
        # zip_filename = "instantclient-basic-windows.x64-11.2.0.4.0.zip"
        # extract_dir = "instantclient_11_2"
        # # 检查是否已解压
        # if not os.path.exists(extract_dir):
        #     # 检查ZIP文件是否存在
        #     if not os.path.exists(zip_filename):
        #         raise FileNotFoundError(f"压缩包 {zip_filename} 未找到！请将压缩包放在当前目录下。")
        #
        #     # 开始解压
        #     with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        #         zip_ref.extractall(".")  # 解压到当前目录
        #     logging.info(f"成功解压 {zip_filename} 到 {extract_dir}")
        # else:
        #     logging.info(f"目录 {extract_dir} 已存在，跳过解压")

        #lib_dir = r"C:\app\lrt\product\11.2.0\dbhome_1\BIN\oci.dll"  # <-- 修改此路径
        lib_dir = os.path.join(base_dir, "instantclient_11_2")

        # 初始化 Oracle Client（必须用于厚模式连接）
        oracledb.init_oracle_client(lib_dir=lib_dir)
        logging.info("Oracle Client 初始化成功！路径：{}".format(lib_dir))

        # 数据库连接参数配置
        db_config = read_db_config()
        connection = oracledb.connect(
            user=db_config['user'],
            password=db_config['password'],
            dsn=db_config['dsn']
        )
        cursor = connection.cursor()
        cursor.arraysize = 10000        # 设置批量获取行数（优化大数据量查询）

        # SQL语法验证
        validate_sql(sql)

        # 计算总行数（性能优化：避免全表拉取）
        count_sql = f"SELECT COUNT(1) FROM ({sql})"
        cursor.execute(count_sql)
        total_rows = cursor.fetchone()[0]
        logging.info(f"总行数: {total_rows}")

        if total_rows <= 500_000:
            # 小数据量处理：一次性导出
            cursor.close()              # 关闭当前游标
            cursor = connection.cursor()# 创建新游标执行主查询
            cursor.arraysize = 10000
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]  # 获取列名
            data = cursor.fetchall()          # 获取所有数据
            df = pd.DataFrame(data, columns=columns)
            # 处理空值：字符串列替换为''，数值列替换为0
            # 优化：使用向量化字符串替换
            for col in df.columns:
                if df[col].dtype == 'object' or df[col].dtype.name == 'string':
                    df[col] = df[col].astype(str).str.replace('\x1f', '', regex=False)
            # 处理空值：字符串列替换为空字符串，数值列替换为0
            fill_dict = {col: '' if df[col].dtype == 'object' else 0 for col in df.columns}
            df = df.fillna(fill_dict)
            df = df.fillna(fill_dict)
            df.to_excel(                     # 补全文件名参数
                "output.xlsx",               # 显式指定文件名
                index=False,
                engine='openpyxl'            # 保留XLSX格式支持
            )
            logging.info("数据已导出到 output.xlsx")
        else:
            # 大数据量处理：分块导出
            cursor.close()
            cursor = connection.cursor()
            cursor.arraysize = 10000
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            output_base = "output"      # 文件名前缀
            split_size = 200_000        # 每个分块最大行数
            current_chunk = []
            current_count = 0
            file_counter = 1

            # 使用tqdm显示进度条
            for row in tqdm(cursor, total=total_rows):
                try:
                    current_chunk.append(row)
                    current_count += 1
                except Exception as e:
                    logging.error(f"数据行 {current_count} 处理失败: 字段名: {columns} 数据: {row}，错误信息: {e} ")
                if current_count >= split_size:
                    # 分块导出并重置计数器
                    df = pd.DataFrame(current_chunk, columns=columns)
                    # 优化：使用向量化字符串替换
                    for col in df.columns:
                        if df[col].dtype == 'object' or df[col].dtype.name == 'string':
                            df[col] = df[col].astype(str).str.replace('\x1f', '', regex=False)
                    # 处理空值：字符串列替换为空字符串，数值列替换为0
                    df = df.fillna({col: '' if df[col].dtype == 'object' else 0 for col in df.columns})
                    filename = f"{output_base}_{file_counter:03d}.xlsx"
                    df.to_excel(
                        filename,
                        index=False,
                        engine='openpyxl'            # 保留XLSX格式支持
                    )
                    logging.info(f"已保存文件：{filename}")

                    current_chunk = []
                    current_count = 0
                    file_counter += 1

            # 处理剩余数据
            if current_chunk:
                filename = f"{output_base}_{file_counter:03d}.xlsx"
                df = pd.DataFrame(current_chunk, columns=columns)
                df = df.applymap(lambda x: str(x).replace('\x1f', '') if isinstance(x, str) else x)
                # 处理空值：字符串列替换为''，数值列替换为0
                fill_dict = {col: '' if df[col].dtype == 'object' else 0 for col in df.columns}
                df = df.fillna(fill_dict)
                df.to_excel(
                    filename,
                    index=False,
                    engine='openpyxl'
                )
                logging.info(f"已保存文件：{filename}")

    except oracledb.DatabaseError as e:
        line_num = traceback.extract_tb(sys.exc_info()[2])[-1].lineno
        logging.error(f"数据库连接错误: {str(e)}")
    except ValueError as e:
        line_num = traceback.extract_tb(sys.exc_info()[2])[-1].lineno
        logging.error(f"数据格式错误: {str(e)}")
    except Exception as e:
        line_num = traceback.extract_tb(sys.exc_info()[2])[-1].lineno
        logging.error(f"行 {line_num} - 列 {columns} - 数据行 {str(row)} - 错误: {str(e)}")
    finally:
        # 资源释放保障（无论是否发生异常）
        if cursor:
            cursor.close()
            logging.debug("游标已关闭")
        if connection:
            connection.close()
            logging.debug("数据库连接已关闭")

if __name__ == "__main__":
    """主程序入口"""
    sql_file = "params.txt"             # SQL参数文件路径
    sql = read_sql_from_file(sql_file)  # 获取SQL查询语句
    execute_query_and_export_to_excel(sql)  # 执行查询与导出流程