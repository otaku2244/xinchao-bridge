"""
xincha-bridge — 心潮桥接服务
在 AI 客户端与 ombre-gateway 之间注入心潮动态状态。
每次请求前从心潮获取 agent-context 注入 system prompt，
请求完成后向心潮发送 conversation-event。
"""

import os
import json
import logging
import asyncio
import re
import uuid
from datetime import datetime, timezone
import httpx
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse, Response
from starlette.routing import Route

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("xincha-bridge")

XINCHAO_URL = os.environ.get("XINCHAO_URL", "http://127.0.0.1:18110")
XINCHAO_TOKEN = os.environ.get("XINCHAO_TOKEN", "")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:18002")
BRIDGE_TOKEN = os.environ.get("BRIDGE_TOKEN", "")
BRIDGE_MACHINE_TOKEN = os.environ.get("BRIDGE_MACHINE_TOKEN", "")
XINCHAO_BRIDGE_WEBHOOK_URL = os.environ.get("XINCHAO_BRIDGE_WEBHOOK_URL", "")
XINCHAO_BRIDGE_WEBHOOK_TOKEN = os.environ.get("XINCHAO_BRIDGE_WEBHOOK_TOKEN", "")
OPERIT_CHAT_ID = os.environ.get("OPERIT_CHAT_ID", "")
# inject=收件箱+对话自动注入（默认）| webhook=external-chat 弹窗 | both=两者都要 | none=只存不收
XINCHAO_BRIDGE_INJECT_MODE = os.environ.get("XINCHAO_BRIDGE_INJECT_MODE", "inject")
XINCHAO_BRIDGE_INBOX_PATH = os.environ.get("XINCHAO_BRIDGE_INBOX_PATH", "/srv/xinchao-bridge/inbox.json")

logger.info(
    "xincha-bridge starting | xinchao=%s gateway=%s bridge_sse=%s",
    XINCHAO_URL,
    GATEWAY_URL,
    "on" if BRIDGE_MACHINE_TOKEN else "off",
)

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(180, connect=10))
    return _client


async def get_xinchao_context() -> str:
    """从心潮 /v1/context 获取 Context Envelope（mode=turn，轻量，不含 OB 长期记忆）。"""
    if not XINCHAO_TOKEN:
        return ""
    try:
        client = get_client()
        context_resp = await client.get(
            f"{XINCHAO_URL}/v1/context",
            params={"mode": "turn", "max_tokens": 1200},
            headers={"Authorization": f"Bearer {XINCHAO_TOKEN}"},
        )
        if context_resp.status_code != 200:
            logger.warning("xinchao context returned %d", context_resp.status_code)
            return ""

        data = context_resp.json()
        ctx_text = (data.get("additionalContext") or "").strip()
        if not ctx_text:
            return ""

        tokens = data.get("estimatedTokens", 0)
        sections = [s.get("id", "") for s in (data.get("sections") or [])]
        logger.info("xinchao context | tokens=%d sections=%s", tokens, ",".join(sections))
        return ctx_text
    except Exception as exc:
        logger.warning("xinchao context failed: %s", exc)
        return ""


def build_handoff_note(messages: list[dict]) -> str | None:
    """从对话末尾构造一条脱水进度便签。只写"进行到哪、下一步"。"""
    if not messages:
        return None
    last_user = ""
    last_assistant = ""
    for msg in reversed(messages):
        role = str(msg.get("role", "")).strip()
        content_text = str(msg.get("content", "")).strip()
        if not last_assistant and role == "assistant":
            last_assistant = content_text
        if not last_user and role == "user":
            last_user = content_text
        if last_user and last_assistant:
            break
    if not last_user:
        return None

    # 脱水：用户问题（截断）+ 回复首句
    user_brief = last_user.replace("\n", " ").strip()[:150]
    assistant_brief = last_assistant.replace("\n", " ").strip()[:200]
    note = f"已处理：{user_brief}"
    if assistant_brief and assistant_brief != user_brief:
        note += f"；回复开头：{assistant_brief}"
    return note[:1200]


async def notify_xinchao(messages: list = None) -> None:
    if not XINCHAO_TOKEN:
        return
    handoff_note = build_handoff_note(messages or [])
    try:
        interaction_type = guess_interaction(messages or []) if messages else None
        async with httpx.AsyncClient(timeout=5) as client:
            payload = {"eventId": str(uuid.uuid4())}
            if interaction_type:
                payload["interactionType"] = interaction_type
            resp = await client.post(
                f"{XINCHAO_URL}/v1/conversation-event",
                headers={
                    "Authorization": f"Bearer {XINCHAO_TOKEN}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            label = interaction_type or "tick"
            logger.info("xinchao event %s \u2192 %d", label, resp.status_code)

            # Save handoff note for cross-window continuity
            if handoff_note:
                note_payload = {
                    "eventId": str(uuid.uuid4()),
                    "session_id": "xincha-bridge",
                    "note": handoff_note,
                }
                note_resp = await client.post(
                    f"{XINCHAO_URL}/v1/handoff-note",
                    headers={
                        "Authorization": f"Bearer {XINCHAO_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json=note_payload,
                )
                logger.info("xinchao handoff-note \u2192 %d | chars=%d", note_resp.status_code, len(handoff_note))
    except Exception as exc:
        logger.warning("xinchao notify failed: %s", exc)


# ── 交互类型猜测（官方设计：不确定时省略 interaction_type）──

INTERACTION_SIGNALS = {
    "companionship": [r"陪", r"聊", r"谢谢", r"辛苦了"],
    "affection": [r"想你了", r"抱", r"亲", r"爱你", r"喜欢你"],
    "intimacy": [r"亲亲", r"吻", r"贴贴", r"蹭蹭", r"撒娇", r"要抱抱", r"想要", r"暖暖"],
    "sharing": [r"分享", r"你看看", r"这个有意思", r"看看这个"],
    "discovery": [r"学到了", r"原来如此", r"有意思", r"涨知识"],
    "task_progress": [r"解决了", r"搞定了", r"完成了", r"做完了"],
    "reflection": [r"总结", r"回顾", r"反思", r"复盘"],
    "conflict": [r"吵架", r"生气", r"不想理", r"别烦我", r"无语"],
    "loss": [r"难过", r"伤心", r"想哭", r"崩溃", r"撑不住", r"绝望"],
    "reconciliation": [r"和好", r"原谅", r"对不起", r"我错了", r"不生气", r"和好吧"],
}


def guess_interaction(messages: list[dict]) -> str | None:
    """根据对话内容猜测交互类型，覆盖心潮原生全部 10 种类型。只有明确信号时才返回，否则返回 None。\ncompanionship=陪伴, affection=关心, intimacy=亲密, sharing=分享, discovery=探索,\ntask_progress=任务, reflection=沉淀, conflict=冲突, loss=失落, reconciliation=和解。"""
    if not messages:
        return None
    user_text = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            user_text = str(msg.get("content", "")).lower()
            break
    if not user_text:
        return None
    for itype, patterns in INTERACTION_SIGNALS.items():
        for pat in patterns:
            if re.search(pat, user_text):
                return itype
    return None



def inject_xinchao_context(messages: list[dict], xinchao_text: str) -> list[dict]:
    if not xinchao_text:
        return messages
    msgs = [dict(m) for m in messages]
    if msgs and msgs[0].get("role") == "system":
        msgs[0] = {
            **msgs[0],
            "content": f"[心潮状态]\n{xinchao_text}\n\n{msgs[0]['content']}",
        }
    else:
        msgs.insert(0, {"role": "system", "content": f"[心潮状态]\n{xinchao_text}"})
    return msgs


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True, "service": "xincha-bridge"})


async def chat_completions(request: Request) -> JSONResponse | StreamingResponse:
    # Auth
    if BRIDGE_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {BRIDGE_TOKEN}":
            return JSONResponse({"error": "unauthorized"}, status_code=401)

    body = await request.json()
    stream = body.get("stream", False)

    # Fetch xinchao state
    xinchao_text = await get_xinchao_context()
    if xinchao_text:
        logger.info("xinchao context injected | chars=%d", len(xinchao_text))

    # Inject into system prompt
    body["messages"] = inject_xinchao_context(body.get("messages", []), xinchao_text)

    # Inject pending xinchao interactions (inbox → 对话自动注入)
    pending = await inbox_take_pending()
    if pending:
        body["messages"] = inject_pending_interactions(body.get("messages", []), pending)
        logger.info("已注入 %d 条未读心潮互动到对话", len(pending))

    # Build forward headers (strip host, preserve auth)
    fwd_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length", "transfer-encoding")
    }

    if stream:
        async def stream_forward():
            status = 200
            try:
                async with get_client().stream(
                    "POST",
                    f"{GATEWAY_URL}/v1/chat/completions",
                    json=body,
                    headers=fwd_headers,
                ) as resp:
                    status = resp.status_code
                    async for chunk in resp.aiter_bytes():
                        yield chunk
            finally:
                asyncio.create_task(notify_xinchao(body.get("messages")))

        return StreamingResponse(stream_forward(), media_type="text/event-stream")
    else:
        resp = await get_client().post(
            f"{GATEWAY_URL}/v1/chat/completions",
            json=body,
            headers=fwd_headers,
        )
        asyncio.create_task(notify_xinchao(body.get("messages")))

        return JSONResponse(
            content=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text},
            status_code=resp.status_code,
        )


async def passthrough(request: Request):
    """Forward all other requests directly to gateway."""
    if BRIDGE_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {BRIDGE_TOKEN}":
            return JSONResponse({"error": "unauthorized"}, status_code=401)

    body = await request.body()
    fwd_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length", "transfer-encoding")
    }
    resp = await get_client().request(
        method=request.method,
        url=f"{GATEWAY_URL}{request.url.path}",
        headers=fwd_headers,
        content=body,
    )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )


# ── 心潮 Bridge SSE 订阅（用户互动反向推送）──

_processed_deliveries: set[str] = set()
delivery_queue: asyncio.Queue = asyncio.Queue()

# 收件箱：未读心潮互动（持久化到文件，重启不丢）
_inbox: list[dict] = []
_inbox_lock: asyncio.Lock = asyncio.Lock()


async def inbox_load() -> None:
    global _inbox
    try:
        with open(XINCHAO_BRIDGE_INBOX_PATH, encoding="utf-8") as f:
            _inbox = json.load(f)
        if _inbox:
            logger.info("收件箱加载 %d 条未读互动", len(_inbox))
    except (FileNotFoundError, json.JSONDecodeError):
        _inbox = []


async def inbox_save() -> None:
    with open(XINCHAO_BRIDGE_INBOX_PATH, "w", encoding="utf-8") as f:
        json.dump(_inbox, f, ensure_ascii=False, indent=1)


async def inbox_push(item: dict) -> None:
    global _inbox
    async with _inbox_lock:
        _inbox.append(item)
        if len(_inbox) > 100:
            _inbox = _inbox[-100:]
        await inbox_save()


async def inbox_take_pending() -> list[dict]:
    """取出全部未读互动并清空（对话注入用，原子操作）。"""
    global _inbox
    async with _inbox_lock:
        if not _inbox:
            return []
        items = list(_inbox)
        _inbox.clear()
        await inbox_save()
        return items


def inject_pending_interactions(messages: list[dict], pending: list[dict]) -> list[dict]:
    """把未读互动作为一条 user 消息注入到对话（紧跟用户最新输入之后）。"""
    if not pending:
        return messages
    lines = ["（心潮小屋通知：你上次离开后，用户留下了这些互动）"]
    for item in pending:
        lines.append(f"- {item.get('message', '')}")
    note = "\n".join(lines)
    msgs = [dict(m) for m in messages]
    last_user = -1
    for i in range(len(msgs) - 1, -1, -1):
        if msgs[i].get("role") == "user":
            last_user = i
            break
    insert_at = last_user + 1 if last_user >= 0 else 1
    msgs.insert(min(insert_at, len(msgs)), {"role": "user", "content": note})
    return msgs


async def bridge_sse_loop() -> None:
    """订阅心潮 /bridge/v1/events，收到 delivery 通知后进入投递队列。"""
    headers = {
        "Authorization": f"Bearer {BRIDGE_MACHINE_TOKEN}",
        "Accept": "text/event-stream",
    }
    while True:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(120, connect=10)) as client:
                async with client.stream("GET", f"{XINCHAO_URL}/bridge/v1/events", headers=headers) as resp:
                    if resp.status_code != 200:
                        logger.warning("bridge SSE 连接失败: %d，10s 后重连", resp.status_code)
                        await asyncio.sleep(10)
                        continue
                    logger.info("bridge SSE 已连接 (%s)", resp.status_code)
                    async for line in resp.aiter_lines():
                        line = line.strip()
                        if not line.startswith("data:"):
                            continue
                        raw = line[5:].strip()
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if payload.get("deliveryId"):
                            await delivery_queue.put(payload["deliveryId"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("bridge SSE 连接异常: %s，10s 后重连", exc)
            await asyncio.sleep(10)


async def delivery_worker() -> None:
    """串行处理投递：读取正文 → POST Operit → ACK。"""
    while True:
        delivery_id = await delivery_queue.get()
        try:
            await handle_delivery(delivery_id)
        except Exception as exc:
            logger.warning("delivery %s 处理异常: %s", delivery_id, exc)


async def handle_delivery(delivery_id: str) -> None:
    if delivery_id in _processed_deliveries:
        logger.info("delivery %s 已处理过，跳过", delivery_id)
        return
    async with httpx.AsyncClient(timeout=httpx.Timeout(320, connect=10)) as client:
        # 1) 读取投递正文（一次性投递）
        deliver_resp = await client.get(
            f"{XINCHAO_URL}/bridge/v1/deliveries/{delivery_id}",
            headers={"Authorization": f"Bearer {BRIDGE_MACHINE_TOKEN}"},
        )
        if deliver_resp.status_code != 200:
            logger.warning("读取投递 %s 失败: %d %s", delivery_id, deliver_resp.status_code, deliver_resp.text[:200])
            return
        delivery = deliver_resp.json()
        reason = delivery.get("reason", "user_interaction")
        message = delivery.get("message", "")
        logger.info("delivery %s | reason=%s | msg=%s", delivery_id, reason, message[:80])

        # 2) 按注入模式处理投递
        ack_status = "delivered"
        received_item = {
            "deliveryId": delivery_id,
            "reason": reason,
            "message": message,
            "receivedAt": datetime.now(timezone.utc).isoformat(),
        }
        if XINCHAO_BRIDGE_INJECT_MODE in ("inject", "both"):
            await inbox_push(received_item)
            logger.info(
                "delivery %s 已入收件箱（reason=%s，mode=%s）",
                delivery_id, reason, XINCHAO_BRIDGE_INJECT_MODE,
            )
        if XINCHAO_BRIDGE_INJECT_MODE in ("webhook", "both"):
            if not XINCHAO_BRIDGE_WEBHOOK_URL:
                logger.warning("webhook 模式但未配置 XINCHAO_BRIDGE_WEBHOOK_URL，投递 %s 仅入收件箱/记录", delivery_id)
            else:
                try:
                    inject_payload = {
                        "message": f"[心潮] {message}",
                        "response_mode": "sync",
                        "show_floating": True,
                        "initial_mode": "WINDOW",
                        "return_tool_status": False,
                    }
                    if OPERIT_CHAT_ID:
                        inject_payload["chat_id"] = OPERIT_CHAT_ID
                    inject_resp = await client.post(
                        XINCHAO_BRIDGE_WEBHOOK_URL,
                        headers={
                            "Authorization": f"Bearer {XINCHAO_BRIDGE_WEBHOOK_TOKEN}",
                            "Content-Type": "application/json; charset=utf-8",
                            "X-Xinchao-Protocol": "xinchao-runtime-wake/1",
                            "X-Xinchao-Delivery-Id": delivery_id,
                        },
                        json=inject_payload,
                    )
                    accepted = False
                    if inject_resp.status_code == 200:
                        try:
                            accepted = bool(inject_resp.json().get("success"))
                        except json.JSONDecodeError:
                            accepted = True  # 200 但无 JSON，视为已接收
                    ack_status = "delivered" if accepted else "retryable_failed"
                except Exception as exc:
                    logger.warning("注入 Operit 失败: %s", exc)
                    ack_status = "retryable_failed"
        if XINCHAO_BRIDGE_INJECT_MODE == "none":
            logger.warning("mode=none，投递 %s 仅记录不投递", delivery_id)

        # 3) ACK 回心潮
        ack_payload = {"status": ack_status}
        if ack_status != "delivered":
            ack_payload["code"] = "injector_failed"
        ack_resp = await client.post(
            f"{XINCHAO_URL}/bridge/v1/deliveries/{delivery_id}/ack",
            headers={"Authorization": f"Bearer {BRIDGE_MACHINE_TOKEN}"},
            json=ack_payload,
        )
        logger.info(
            "delivery %s → Operit(%s) → ACK %s (%d)",
            delivery_id,
            ack_status,
            ack_payload["status"],
            ack_resp.status_code,
        )
        if ack_status == "delivered" and ack_resp.status_code == 200:
            _processed_deliveries.add(delivery_id)
            if len(_processed_deliveries) > 500:
                _processed_deliveries.clear()


async def xinchao_snapshot(request: Request) -> JSONResponse:
    """透传心潮 /v1/dashboard/snapshot，供 Operit 中 AI 查询播报。"""
    if BRIDGE_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {BRIDGE_TOKEN}":
            return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not XINCHAO_TOKEN:
        return JSONResponse({"error": "XINCHAO_TOKEN 未配置"}, status_code=500)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{XINCHAO_URL}/v1/dashboard/snapshot",
                headers={"Authorization": f"Bearer {XINCHAO_TOKEN}"},
            )
        return JSONResponse(resp.json(), status_code=resp.status_code)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)


@asynccontextmanager
async def lifespan(app):
    await inbox_load()
    tasks = []
    if BRIDGE_MACHINE_TOKEN:
        tasks.append(asyncio.create_task(bridge_sse_loop()))
        tasks.append(asyncio.create_task(delivery_worker()))
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass


app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/v1/chat/completions", chat_completions, methods=["POST"]),
        Route("/v1/snapshot", xinchao_snapshot, methods=["GET"]),
        Route("/{path:path}", passthrough),
    ],
    lifespan=lifespan,
)
