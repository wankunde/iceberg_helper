# AGENTS.md — Iceberg Metadata Viewer (iceberg_helper)

本文件面向在本仓库中工作的代码 Agent（含代码生成/修复/重构/测试）。目标是让 Agent 快速理解项目结构、关键流程、约束与常见任务的落地方式。

---

## 1. 项目概览

**Iceberg Metadata Viewer** 是一个本地运行的 FastAPI Web 工具，用于浏览 Iceberg 表的 `metadata/` 目录，分类展示元数据文件，并解析/格式化展示内容：

- 目录扫描与分类：`v*.metadata.(json|avro)`、`snap-*.avro`、`manifest-*.avro`、`manifest-list-*.avro`、其他 JSON/杂项
- Avro 解析：读取 Avro 文件并转成可展示的 JSON 结构
- JSON 美化：对 JSON 文件做格式化输出（pretty print），前端高亮
- 元数据概览：从表元数据文件抽取关键信息（UUID、Location、Schema、Partition Spec 等）
- 前端：Bootstrap 5 + Highlight.js + Jinja2 模板

> 典型用户路径：输入 metadata 目录 → 左侧列表选择文件 → 右侧查看内容 →（若为表元数据）顶部展示概览卡片。

---

## 2. 运行与开发约定

### 2.1 环境
- Python: **3.13.7**（README 指定）
- 通过脚本启动：`./scripts/start.sh`（会创建 venv 并安装依赖）
- 停止：`./scripts/stop.sh` 或 `Ctrl+C`
- 服务默认：`http://127.0.0.1:8000`

### 2.2 代码风格与原则（Agent 必须遵守）
- 修改代码的时候，尽量避免单个程序文件超过500行
- 变更应尽量小且可回滚；避免“一次性大重构”
- 任何新增/修改接口应保持向后兼容（前端依赖现有字段/参数）
- 涉及文件路径的功能：必须考虑 **路径合法性、目录穿越、非本机路径** 等风险
- 解析/格式化：优先“容错 + 可观测性”（清晰错误信息），避免吞异常
- 避免引入重型依赖；优先复用已有模块

---

## 3. 仓库结构与职责（按 README 描述）

> 以下路径以 `iceberg_helper/` 为根目录。

- `app/main.py`
  - FastAPI 应用入口
  - 注册路由：页面渲染与 `/api/*` 接口
- `app/config.py`
  - 配置（例如：允许访问的路径策略、默认参数、缓存开关等——以实际代码为准）
- `app/services/iceberg_parser.py`
  - Iceberg 元数据解析服务（重点：Avro 读取、抽取 metadata 概览）
- `app/services/json_utils.py`
  - JSON 工具（读取、格式化、可能包含安全/编码处理）
- `app/templates/index.html`
  - 主页面模板（左侧文件列表、右侧内容展示、搜索/格式化/复制等交互）
- `app/static/`
  - 静态资源（Bootstrap/Highlight.js/自定义 JS/CSS）
- `scripts/start.sh` / `scripts/stop.sh`
  - 本地启动/停止脚本
- `requirements.txt`
  - Python 依赖列表

---

## 4. API 契约（Agent 修改需谨慎）

### 页面
- `GET /`：返回主页面（Jinja2 模板）

### 数据接口（前端依赖）
- `GET /api/list-dir?path=<目录路径>`
  - 输入：metadata 目录绝对路径
  - 输出：按类别组织的文件列表（以及“最新版本”标记）
- `GET /api/avro?file_path=<文件路径>&formatted=true`
  - 输入：文件路径、是否格式化
  - 输出：Avro 内容（通常为 JSON 可序列化结构）
- `GET /api/json?file_path=<文件路径>&formatted=true`
  - 输入：JSON 文件路径、是否格式化
  - 输出：JSON 内容（原始或 pretty）
- `GET /api/metadata-info?file_path=<文件路径>&file_type=<json|avro>`
  - 输入：表元数据文件路径 + 类型
  - 输出：概览字段（UUID/location/schema/partition spec 等）

**重要：**
- 若需变更返回结构，必须同时更新前端模板/JS，并保持旧字段至少一段时间（或提供兼容分支）。
- 错误响应建议统一为 `{ "detail": "...error..." }`（FastAPI 常规做法），并在前端可读。

---

## 5. 关键数据与文件类型（Agent 需要理解）

Iceberg metadata 目录常见文件：
- `v<N>.metadata.json`：表元数据（最常用）
- `v<N>.metadata.avro`：表元数据（Avro 版本）
- `snap-*.avro`：快照信息
- `manifest-*.avro`：manifest
- `manifest-list-*.avro`：manifest list

常见任务：
- 从 `v*.metadata.*` 中提取：`table-uuid`、`location`、`current-schema`、`schemas`、`partition-specs`、`current-snapshot-id` 等（字段名以 Iceberg 版本为准）
- Avro 文件通常包含记录数组、嵌套结构；展示层要考虑体积与可读性

---

## 6. 安全与健壮性要求（高优先级）

### 6.1 文件路径访问
- 不要允许任意路径读取（除非项目明确就是本机任意读取，并在文档中声明风险）
- 建议策略（若代码尚未实现，Agent 可作为增强项提出 PR）：
  - 仅允许读取用户输入目录下的文件（以及其子路径）
  - `realpath` 归一化，拒绝 `..`、符号链接逃逸等
  - 限制最大文件大小（避免读取超大 Avro/JSON 卡死）

### 6.2 性能与内存
- 大 Avro/JSON：避免一次性加载导致内存爆
- 输出：必要时提供截断、分页或“只展示前 N 条记录”的模式（需要前端配合）

### 6.3 错误处理
- 区分：路径不存在/无权限/解析失败/格式不支持
- 错误信息应对用户友好，同时在服务端日志保留堆栈（便于排查）

---

## 7. Agent 常见工作流（建议）

### 7.1 增加新特性（例：支持更多 Iceberg 字段）
1. 在 `app/services/iceberg_parser.py` 增加解析逻辑（保持函数纯度、可测试）
2. 在 `/api/metadata-info` 返回中新增字段（不要破坏旧字段）
3. 更新 `templates/index.html` 展示（容错：字段为空时不显示）
4. 加单元测试（若项目已有测试框架；否则先补最小测试骨架）

### 7.2 修复 bug（例：某些 Avro 文件解析失败）
1. 在服务层增加更明确的异常分类
2. 为失败样例增加回归测试（可用最小 Avro fixture）
3. 确保前端能展示错误而不是空白

### 7.3 改善体验（例：搜索高亮更快）
- 优先在前端处理展示层逻辑
- 后端只提供稳定、结构化数据

---

## 8. 测试建议（如果仓库尚未包含测试）

若缺少测试目录，建议新增：
- `tests/` + `pytest`
- 针对 services 层的纯函数测试（JSON 格式化、分类规则、metadata 概览抽取）
- 针对 API 的最小集成测试：FastAPI `TestClient`

建议覆盖点：
- 目录分类：不同命名模式是否正确归类、“最新”版本识别
- JSON 格式化：非法 JSON、超大 JSON、编码问题
- Avro 解析：空文件、schema 不兼容、record 非预期结构
- 路径安全：`../`、符号链接、非绝对路径

---

## 9. 变更清单要求（Agent 提交 PR/变更时）

每次变更请附：
- 变更目的（1-2 句）
- 影响面：API/模板/脚本/依赖是否变化
- 本地验证方式（命令 + 预期结果）
- 若有安全相关改动：说明威胁模型与拒绝策略

---

## 10. 快速定位（给 Agent 的搜索关键词）

在 VS Code 中可用全文搜索：
- 路由：`/api/`、`FastAPI(`、`@app.get`
- 目录扫描：`list-dir`、`os.listdir`、`Path(`、`glob`
- Avro：`avro`、`fastavro`、`avro.schema`、`reader`
- metadata 概览：`table-uuid`、`location`、`schema`、`partition`
- 前端交互：`fetch(`、`highlight`、`clipboard`

---

## 11. 未决问题（可作为改进项）

（由 Agent 在实际读代码后补全）
- 当前允许读取的路径范围是什么？是否需要白名单/沙箱？
- 是否存在缓存？缓存是否会导致“显示旧内容”？
- 对大文件是否有阈值与提示？
- Avro 解析依赖库是哪一个（fastavro/avro-python3）？版本兼容性如何？

---