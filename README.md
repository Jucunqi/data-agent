# Data Agent - 智能数据分析助手

基于大语言模型（LLM）的智能数据分析系统，支持自然语言转 SQL、自动数据检索与多轮对话。

## 项目简介

Data Agent 是一个基于 LangGraph 构建的智能数据分析 Agent，能够理解自然语言查询，自动生成并执行 SQL 语句，返回数据分析结果。系统采用模块化架构设计，支持向量检索、元数据管理和多数据源集成。

## 核心功能

- **自然语言转 SQL**：用户通过自然语言描述数据需求，系统自动生成对应的 SQL 查询
- **智能 Schema 召回**：基于向量相似度检索相关的表、字段和指标信息
- **元数据管理**：支持表结构、指标定义、字段枚举值等元数据的统一管理
- **多数据源支持**：同时支持元数据库（Meta DB）和数据仓库（DW）
- **SQL 自动纠错**：生成的 SQL 出错时，系统会自动分析错误并重新生成
- **流式响应**：采用 SSE 流式输出，实时展示分析过程和结果

## 技术架构

### 核心组件

```
┌─────────────────────────────────────────────────────────────┐
│                        API 层 (FastAPI)                      │
│                    - 查询接口 /api/query                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                    Agent 层 (LangGraph)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ 关键词提取  │─▶│ 信息召回    │─▶│ Schema 过滤/生成 SQL │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│                           │                                  │
│              ┌────────────┴────────────┐                   │
│              ▼                         ▼                   │
│       ┌─────────────┐           ┌─────────────┐           │
│       │ SQL 验证    │◀─────────▶│ SQL 纠错    │           │
│       └─────────────┘           └─────────────┘           │
└─────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                     数据层                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ MySQL    │  │ Qdrant   │  │ES        │  │ LLM API  │   │
│  │(Meta/DW) │  │(向量存储)│  │(枚举值)  │  │(推理服务)│   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 技术栈

| 组件 | 技术选型 |
|------|----------|
| Web 框架 | FastAPI |
| Agent 框架 | LangGraph |
| LLM 接口 | LangChain OpenAI |
| 向量数据库 | Qdrant |
| 搜索引擎 | Elasticsearch |
| 关系数据库 | MySQL (asyncmy) |
| 配置管理 | OmegaConf |
| 日志 | Loguru |
| 包管理 | uv |

## 项目结构

```
data-agent/
├── app/
│   ├── agent/              # LangGraph Agent 实现
│   │   ├── nodes/          # 各处理节点（关键词提取、召回、生成 SQL 等）
│   │   ├── graph.py        # 状态图定义
│   │   ├── state.py        # 状态定义
│   │   └── llm.py          # LLM 客户端
│   ├── api/                # FastAPI 接口层
│   │   ├── routers/        # 路由定义
│   │   └── schemas/        # Pydantic 模型
│   ├── clients/            # 各类客户端管理器
│   │   ├── qdrant_client_manager.py
│   │   ├── es_client_manager.py
│   │   ├── mysql_client_manager.py
│   │   └── embedding_client_manager.py
│   ├── conf/               # 配置模块
│   ├── core/               # 核心工具（日志、上下文）
│   ├── entities/           # 实体定义
│   ├── models/             # SQLAlchemy 模型
│   ├── prompt/             # Prompt 加载器
│   ├── repositories/       # 数据访问层
│   │   ├── es/             # Elasticsearch 仓库
│   │   ├── mysql/          # MySQL 仓库
│   │   └── qdrant/         # Qdrant 仓库
│   ├── scripts/            # 数据初始化脚本
│   └── services/           # 业务服务层
├── conf/                   # YAML 配置文件
├── prompts/                # LLM Prompt 模板
├── main.py                 # 应用入口
└── pyproject.toml          # 项目依赖
```

## 快速开始

### 环境要求

- Python >= 3.12
- MySQL 8.0+
- Qdrant
- Elasticsearch 8.x
- LLM API 服务（OpenAI 兼容接口）

### 安装依赖

```bash
# 使用 uv 安装（推荐）
uv sync

# 或使用 pip
pip install -r requirements.txt
```

### 配置

编辑 `conf/app_config.yaml`：

```yaml
# 数据库配置
db_meta:
  host: localhost
  port: 3306
  user: root
  password: your_password
  database: meta

db_dw:
  host: localhost
  port: 3306
  user: root
  password: your_password
  database: dw

# Qdrant 向量数据库
qdrant:
  host: localhost
  port: 6333
  embedding_size: 1024

# Embedding 服务
embedding:
  host: localhost
  port: 8081
  model: BAAI/bge-large-zh-v1.5

# Elasticsearch
es:
  host: localhost
  port: 9200
  index_name: data_agent

# LLM 配置
llm:
  model_name: gpt-4
  api_key: your_api_key
  base_url: https://api.openai.com/v1
```

### 初始化元数据

```bash
python -m app.scripts.build_meta_knowledge
```

### 启动服务

```bash
# 开发模式
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 生产模式
uvicorn main:app --host 0.0.0.0 --port 8000
```

## 使用示例

### API 调用

```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "查询上个月的销售额"}'
```

### 响应格式

采用 Server-Sent Events (SSE) 流式输出：

```
data: {"type": "step", "step": "关键词提取", "content": "上月, 销售额"}

data: {"type": "sql", "sql": "SELECT SUM(amount) FROM sales WHERE date >= '2024-01-01'"}

data: {"type": "result", "rows": [[1000000]]}
```

## Agent 工作流程

1. **关键词提取** (`extract_keywords`)：从用户查询中提取核心关键词
2. **信息召回** (`recall_*`)：
   - `recall_column`：召回相关字段
   - `recall_metric`：召回相关指标
   - `recall_value`：召回归举值
3. **信息合并** (`merge_retrieved_info`)：合并召回的元数据
4. **Schema 过滤** (`filter_*`)：过滤相关的表和指标
5. **SQL 生成** (`generate_sql`)：基于上下文生成 SQL
6. **SQL 验证与执行** (`validate_sql`, `run_sql`)：验证语法并执行
7. **错误处理** (`correct_sql`)：出错时自动纠错重试

## 开发指南

### 添加新的 Agent 节点

1. 在 `app/agent/nodes/` 创建节点文件
2. 实现节点函数：`async def node_name(state: DataAgentState, context: DataAgentContext) -> DataAgentState`
3. 在 `app/agent/graph.py` 注册节点和边

### 添加新的数据源

1. 在 `app/clients/` 创建客户端管理器
2. 在 `app/repositories/` 实现仓库层
3. 在 `app/agent/context.py` 添加上下文

## 注意事项

1. **模块导入**：本项目使用绝对导入，确保项目根目录在 PYTHONPATH 中
2. **异步支持**：所有数据库操作均为异步实现，使用 `async`/`await`
3. **配置热加载**：修改 YAML 配置后需重启服务生效

