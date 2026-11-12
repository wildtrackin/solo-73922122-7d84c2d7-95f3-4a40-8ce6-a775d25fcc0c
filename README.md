# 冷库温度采集 API（FastAPI + Modbus TCP）

单进程冷链温度采集服务。排查冷库超温时，通过 **Modbus TCP 功能码 03** 读取各回路
当前温度（unit=1，保持寄存器地址 0~7，有符号 16 位整数 ÷ 10 得摄氏温度），
返回温度、回路编号与接收时间。**读数不缓存**，每次调用都发起真实采集。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

启动后：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集 API | http://localhost:8000 | FastAPI 采集服务 |
| 交互式文档 | http://localhost:8000/docs | Swagger UI，可直接 Try it out |
| 演示页面 | http://localhost:8000/ | 并发采集 + 故障注入可视化 |
| 模拟控制器 Modbus | `simulator:5020`（宿主 `localhost:5020`） | Modbus TCP 从站 |
| 模拟器控制 API | http://localhost:8081 | 切换倒序回复 / 延迟 / 断线 / 异常 |

停止：`docker compose down`

## 二、采集 API

### `GET /read/{circuit}` — 读取单个回路

`circuit` 即寄存器地址，范围 0~7。

```bash
curl http://localhost:8000/read/0
# {
#   "circuit": 0,
#   "address": 0,
#   "temperature_c": -18.0,        # 有符号整数 -180 ÷ 10
#   "raw_register": -180,
#   "received_at": "2026-11-12T04:13:03.376+00:00"
# }
```

### `GET /read/all` — 并发读取全部 8 个回路

8 笔读取在服务端通过同一条 TCP 连接**同时在途**发送（流水线），结果按 TID 匹配，
单项失败不影响其他回路：

```bash
curl http://localhost:8000/read/all
```

### `POST /read/batch` — 并发读取指定回路

```bash
curl -X POST http://localhost:8000/read/batch \
  -H 'Content-Type: application/json' \
  -d '{"circuits": [0, 3, 7]}'
```

### 错误响应

所有失败统一返回中文「采集失败」说明，并带机器可读的 `error` 类别：

| HTTP | error | 触发条件 |
| --- | --- | --- |
| 400 | `illegal_address` | 回路编号超出 0~7 |
| 504 | `timeout` | 发出后 2 秒未收到响应 |
| 502 | `connection_lost` | 连接断开（含断线后重连失败） |
| 502 | `connect_failed` | 无法建立到控制器的 TCP 连接 |
| 502 | `exception_response` | 控制器返回 Modbus 异常响应（功能码 \| 0x80） |
| 502 | `protocol_error` | MBAP 头或响应内容核对不通过 |

```bash
curl -i http://localhost:8000/read/99
# HTTP/1.1 400 ...
# {"error":"illegal_address","message":"采集失败：回路编号必须在 0~7 之间，收到 99"}
```

## 三、并发调用示例

容器内自带示例脚本，对 8 个回路同时发起读取：

```bash
docker compose exec collector python scripts/demo_concurrent.py
```

或本地（安装 `requirements-dev.txt` 后）：

```bash
python scripts/demo_concurrent.py http://localhost:8000
```

也可直接在浏览器打开 http://localhost:8000/ 点击「并发读取全部 8 回路」。

### 在 `/docs` 中演示「采集 → 超时 → 恢复调用」

交互式文档 http://localhost:8000/docs 的 `GET /read/{circuit}` 已内置成功与
`504 timeout` 示例，可直接 **Try it out** 复现完整恢复过程：

```bash
# 1) 正常采集
curl http://localhost:8000/read/0

# 2) 让模拟控制器接收变慢（每笔应答延迟 2.5s，超过 2s 超时）
curl -X POST localhost:8081/mode -H 'Content-Type: application/json' \
  -d '{"delay_seconds":2.5}'

# 3) 再采集：约 2 秒返回 504；采集端关闭该连接、释放发送锁
curl -i http://localhost:8000/read/0
# HTTP/1.1 504 ...
# {"error":"timeout","message":"采集失败：寄存器 0 读取超时（2s 未响应）"}

# 4) 控制器恢复正常
curl -X POST localhost:8081/reset

# 5) 再次采集：自动重连，立即恢复 200（无需重启采集服务）
curl http://localhost:8000/read/0
```

> 即使卡在 `drain()`（控制器不接收），第 3 步也会在 2 秒整失败而不是无限挂起；
> 拆除连接后被占住的发送锁随之释放，其他回路不再被拖死。

## 四、关键设计：如何避免并发采样串值

核心实现见 `app/modbus_client.py`。

1. **全调用共用一条 TCP 连接**：进程内单例客户端懒连接，所有请求复用同一 socket。
2. **流水线发送**：发送下一笔不等待上一笔应答。仅用一把短临界区锁串行化
   `socket.write` 动作，不限制在途请求数量；每笔请求分配唯一事务标识符 TID。
3. **乱序/分片/粘包处理**：后台读取循环按 MBAP 的 `length` 字段精确重组帧
   （自动处理 TCP 分片与粘包），按 **TID 查找在途请求**完成匹配，
   因此控制器倒序回复也不会串值。
4. **逐字段核对**：校验协议标识符（=0）、单元标识符（=1）、功能码（=0x03）、
   字节计数（=2）与正文长度；功能码最高位置位时按 Modbus 异常响应报采集失败。
5. **2 秒超时（从写出请求起算，覆盖 drain 与等待应答）**：超时窗口在调用
   `writer.write` 写出请求那一刻开始，用同一个截止时刻先后约束 `drain()` 与
   应答等待——即使 `drain()` 因控制器接收过慢而长时间阻塞，也会按时失败，
   不会占住发送锁把其他回路的采样一起挂死。超时即关闭**所属代连接**、释放
   发送锁，使该连接上其余在途读取一并失败；下一次调用自动重连。触发拆除的
   请求自身抛出准确的 `timeout` 错误，不被连接拆除的统一异常反向覆盖。
6. **代际隔离，旧任务不碰新连接**：连接断开后下一次调用自动重连，每代连接带
   `generation` 编号。旧连接的 drain 迟到完成/迟到报错、旧读取循环的残余事件，
   都带着旧代际，代际不符的拆除动作一律忽略——既不会关闭新连接，也不会清掉
   新连接上的待响应请求；旧连接收到的残余报文同样按代际丢弃。
7. **发送期取消无残留**：在持锁、drain 过程中被取消时，必定清掉自身待响应项、
   释放发送锁并关闭所属代连接，不遗留待响应项、不遗留被占住的发送锁。
8. **不缓存**：无任何读数缓存，寄存器值变化在下一次调用立即可见。

## 五、模拟控制器与故障注入

`simulator/app.py` 在同一进程内提供 Modbus TCP 从站（5020）与控制 API（8081）。
初始寄存器（有符号整数）：

| 地址 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 原始值 | -180 | -175 | 50 | 235 | -200 | 300 | -45 | 105 |
| 温度 ℃ | -18.0 | -17.5 | 5.0 | 23.5 | -20.0 | 30.0 | -4.5 | 10.5 |

控制 API（均为 POST，演示页面有对应按钮）：

```bash
# 倒序回复：同批请求按相反顺序应答，验证 TID 匹配不串值
curl -X POST localhost:8081/mode -H 'Content-Type: application/json' \
  -d '{"reply_mode":"reverse"}'

# 回复延迟 2.5 秒：触发采集端 2 秒超时（504）
curl -X POST localhost:8081/mode -H 'Content-Type: application/json' \
  -d '{"delay_seconds":2.5}'

# 断线：next=下一笔请求到达后断线；all=拒绝所有新连接
curl -X POST localhost:8081/disconnect -H 'Content-Type: application/json' \
  -d '{"mode":"next"}'

# 对地址 2 注入 2 次异常码 0x02 的 Modbus 异常响应
curl -X POST localhost:8081/exception -H 'Content-Type: application/json' \
  -d '{"address":2,"code":2,"count":2}'

# 修改寄存器值（验证不缓存）
curl -X POST localhost:8081/registers -H 'Content-Type: application/json' \
  -d '{"values":[999]}'

# 恢复全部默认
curl -X POST localhost:8081/reset
```

### 推荐演示流程

1. 打开 http://localhost:8000/ ，点「并发读取全部 8 回路」，观察 8 笔同时返回。
2. 点「倒序回复」后再次并发采集——结果仍与回路一一对应，无串值。
3. 点「延迟 2.5s」后采集——2 秒整返回 504 超时；点「恢复正常」后自动重连成功。
4. 点「下一笔后断线」后采集——首笔 502，再次点击即自动重连成功。
5. 点「注入异常响应」后采集回路 2——返回 502 异常响应，其余回路不受影响。

## 六、本地开发（无 Docker）

```bash
pip install -r requirements-dev.txt

# 终端 1：启动模拟器
python -m simulator.app

# 终端 2：启动采集 API
MODBUS_HOST=127.0.0.1 MODBUS_PORT=5020 \
  uvicorn app.main:app --reload --port 8000

# 测试（36 个用例，覆盖流水线/乱序/分片粘包/超时/drain 阻塞/发送取消/
# 旧发送任务迟到/断线/代际隔离/异常响应）
pytest -q
```

## 七、配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MODBUS_HOST` | `simulator` | 控制器地址，配置固定 |
| `MODBUS_PORT` | `5020` | 控制器 Modbus TCP 端口 |
| `MODBUS_UNIT_ID` | `1` | 单元标识符 |
| `MODBUS_TIMEOUT_SECONDS` | `2` | 单笔读取超时（秒） |
| `REGISTER_COUNT` | `8` | 回路数量（地址 0~N-1） |

模拟器变量：`REGISTER_VALUES`、`REPLY_MODE`、`DELAY_SECONDS`、
`DISCONNECT_MODE`、`MODBUS_PORT`、`CONTROL_PORT`。

## 八、目录结构

```
.
├── app/
│   ├── main.py              # FastAPI 路由、温度换算、错误映射
│   ├── modbus_client.py     # 单连接流水线 Modbus 客户端（核心）
│   ├── config.py            # 环境变量配置
│   └── static/index.html    # 演示页面
├── simulator/app.py         # 本地模拟控制器（Modbus + 故障注入 API）
├── scripts/demo_concurrent.py
├── tests/
│   ├── test_modbus_client.py          # 协议/并发/超时/断线/代际隔离
│   ├── test_api.py                    # FastAPI 接口与错误映射
│   └── test_timeout_drain_cancel.py   # drain 阻塞/发送取消/旧任务迟到回归
├── Dockerfile
└── docker-compose.yml
```
