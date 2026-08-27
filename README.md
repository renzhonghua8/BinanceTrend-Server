# Binance 合约榜单钉钉提醒服务

Linux/Docker 常驻服务。持续监控 Binance USDⓈ-M USDT 永续合约 24H 涨幅榜前 10，计算 Supertrend 后取“方向合格”币种中原榜单名次最高的一名作为合格第一名。只有合格第一名发生变化时，才向钉钉发送绿色“方向合格”榜单。

## 行为

- 首次启动只记录合格第一名，不发送。
- 原始涨幅榜第一名变化，但合格第一名不变时，不发送。
- 合格第一名变化后，发送当时前 10 中的全部方向合格币种。
- 当前没有合格币种时不发送；之后重新出现合格第一名时发送。
- 消息包含钉钉机器人关键词“榜单”。
- 合格第一名持久化到 Docker volume，重启不会重复提醒。
- 趋势结果缓存 30 秒；榜单名次仍按实时行情更新。
- WebSocket 断线指数退避重连，容器异常退出自动重启。
- 使用 Binance 新版官方 `/market/ws/` USDⓈ-M WebSocket 路径。
- 健康检查仅监听服务器本机 `127.0.0.1:18080/health`。

## 部署

```bash
cp .env.example .env
# 编辑 .env，填入 DINGTALK_WEBHOOK
docker compose up -d --build
docker compose logs -f --tail=100
curl http://127.0.0.1:18080/health
```

不要把 `.env` 上传到代码仓库或发给他人。服务器关机、休眠、断网，或服务器所在网络无法连接 Binance 时，服务无法监控。
