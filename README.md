# SQL Export to Excel

## 项目概述
本工具实现Oracle数据库查询与Excel分块导出功能，核心特性包括：
- 自动处理超过50万行的大数据量查询（每20万行生成一个Excel文件）
- 严格的SQL语法验证与异常处理机制
- 实时进度条显示（使用tqdm）
- 独立带时间戳的日志记录系统
- 空值智能处理（字符串列空值替换为`''`，数值列替换为`0`）
- 未配置文件自动创建与提示

## 依赖环境
必须安装以下Python库：
```bash
pip install oracledb pandas sqlparse tqdm
```

> **注意**：`oracledb`包需要额外配置Oracle Instant Client

## 快速开始指南
```bash
# 1. 安装依赖
pip install oracledb pandas sqlparse tqdm

# 2. 解压Oracle Instant Client（按以下步骤）
unzip instantclient-basic-windows.x64-11.2.0.4.0.zip
# 确保生成 instantclient_11_2 文件夹
#oracle数据库版本在12.1以上可使用瘦连接和厚连接两种方式，在12.1版本以下只能使用厚连接方式

# 3. 配置数据库连接
echo "user=your_username" > database.txt
echo "password=your_password" >> database.txt
echo "dsn=localhost:1521/orcl" >> database.txt

# 4. 准备SQL查询
echo "SELECT * FROM your_table" > params.txt

# 5. 执行导出
python sql_export.py
```

## 环境准备说明
1. 下载Oracle Instant Client Basic Package：[Windows 64-bit](https://www.oracle.com/database/technologies/instant-client/downloads.html)
2. 将下载的 `instantclient-basic-windows.x64-11.2.0.4.0.zip` 放置在项目根目录
3. 解压ZIP文件（无需手动配置）：
   ```bash
   # Linux/macOS
   unzip instantclient-basic-windows.x64-11.2.0.4.0.zip

   # Windows (PowerShell)
   Expand-Archive instantclient-basic-windows.x64-11.2.0.4.0.zip
   ```
4. 确认生成 `instantclient_11_2` 目录（如不存在将自动创建）

## 配置文件说明
### `database.txt` 格式示例
```ini
user=your_username
password=your_password
dsn=your_dsn
```
- **user**：数据库用户名
- **password**：数据库密码
- **dsn**：Oracle服务名（格式：`host:port/service`）

### `params.txt` 格式示例
```sql
SELECT id, name, value 
FROM large_table 
WHERE creation_date > TO_DATE('2023-01-01','YYYY-MM-DD')
```

## 附加特性
- 未找到配置文件时自动创建默认配置模板
- 大数据量时自动分块导出（每块20万行）
- 进度条实时显示查询进度
- 所有操作自动记录到时间戳日志文件