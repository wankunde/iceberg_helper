# Iceberg Metadata Viewer

一个基于 FastAPI 的本地 Web 工具，用于快速查看和浏览 Iceberg 表的元数据文件。

## 功能特性

- 📁 **目录浏览**: 选择 Iceberg 表的 metadata 目录，自动扫描并分类显示元数据文件
- 🔍 **Avro 解析**: 解析 Avro 格式的元数据文件
- 📄 **JSON 格式化**: 自动格式化 JSON 文件，提供美观的代码高亮展示
- 📊 **元数据概览**: 自动提取并展示表的关键信息（UUID、Location、Schema、Partition Spec 等）
- 🔎 **搜索功能**: 支持在文件内容中搜索关键字并高亮显示
- 📋 **一键复制**: 快速复制文件内容到剪贴板
- 🎨 **美观界面**: 使用 Bootstrap 5 构建的现代化响应式界面

## 环境要求

- Python 3.13.7

## 快速开始

### 1. 安装依赖

项目会自动创建虚拟环境并安装依赖，无需手动操作。

### 2. 启动服务

```bash
./scripts/start.sh
```

服务将在 `http://127.0.0.1:8000` 启动。

### 3. 停止服务

```bash
./scripts/stop.sh
```

或者直接按 `Ctrl+C` 停止服务。

## 使用说明

1. **选择 Metadata 目录**
   - 在首页的输入框中输入 Iceberg 表的 metadata 目录绝对路径
   - 例如：`/path/to/warehouse/db/table/metadata/`
   - 点击"加载"按钮或按回车键

2. **浏览文件**
   - 左侧边栏会显示分类的文件列表：
     - 表元数据版本（v*.metadata.json/avro）
     - 快照文件（snap-*.avro）
     - Manifest 文件（manifest-*.avro）
     - Manifest List（manifest-list-*.avro）
     - JSON 文件
     - 其他文件
   - 最新版本会显示"最新"标记

3. **查看文件内容**
   - 点击文件列表中的任意文件
   - 右侧会显示文件内容（自动格式化）
   - 如果是表元数据文件，顶部会显示元数据概览卡片

4. **使用功能**
   - **搜索**: 在搜索框中输入关键字，自动高亮匹配内容
   - **复制**: 点击"复制"按钮，将内容复制到剪贴板
   - **格式化切换**: 点击"格式化"按钮，切换原始/格式化视图

## 项目结构

```
iceberg_helper/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI 应用入口
│   ├── config.py            # 配置文件
│   ├── services/
│   │   ├── __init__.py
│   │   ├── iceberg_parser.py  # 元数据解析服务
│   │   └── json_utils.py      # JSON 工具函数
│   ├── templates/
│   │   └── index.html        # 主页面模板
│   └── static/               # 静态资源目录
├── scripts/
│   ├── start.sh             # 启动脚本
│   └── stop.sh              # 停止脚本
├── requirements.txt          # Python 依赖
└── README.md                # 项目说明
```

## API 接口

- `GET /`: 主页面
- `GET /api/list-dir?path=<目录路径>`: 列出目录下的文件
- `GET /api/avro?file_path=<文件路径>&formatted=true`: 解析 Avro 文件
- `GET /api/json?file_path=<文件路径>&formatted=true`: 读取 JSON 文件
- `GET /api/metadata-info?file_path=<文件路径>&file_type=<json|avro>`: 获取元数据概览

## 运行模式

* 本地运行: `./scripts/start.sh $META_DATA_PATH`
* 容器运行: 
```sh
# Build
./scripts/docker_build.sh

# Run
./scripts/docker_start.sh

# Log
docker logs -f iceberg-helper
```


## 技术栈

- **后端**: FastAPI + Uvicorn
- **前端**: Bootstrap 5 + Highlight.js
- **模板引擎**: Jinja2

## 许可证

MIT License
