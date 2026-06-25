#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务抢占工具 v2.0
功能: 高速轮询 + 并发认领 + 双线路切换 + 关键词记忆下拉 + 成功提示音 + 防休眠
依赖: pip install aiohttp
"""

import asyncio
import json
import os
import sys
import time
import random
import threading
import datetime
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from pathlib import Path
from typing import Optional, Tuple

# ─── 依赖检查 ─────────────────────────────────────────────────────────────────

try:
    import aiohttp
except ImportError:
    _root = tk.Tk()
    _root.withdraw()
    messagebox.showerror(
        "缺少依赖",
        "请先在终端运行以下命令安装依赖，然后重新启动脚本：\n\n"
        "    pip install aiohttp\n\n"
        "如果没有 pip，请先安装 Python 3.8+（https://python.org）",
    )
    sys.exit(1)

# ─── 防休眠 ────────────────────────────────────────────────────────────────────

if sys.platform == "win32":
    import ctypes
    _ES_CONTINUOUS       = 0x80000000
    _ES_SYSTEM_REQUIRED  = 0x00000001
    _ES_DISPLAY_REQUIRED = 0x00000002

    def _prevent_sleep():
        ctypes.windll.kernel32.SetThreadExecutionState(
            _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_DISPLAY_REQUIRED
        )

    def _allow_sleep():
        ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)

elif sys.platform == "darwin":
    _caffeinate_proc: Optional["subprocess.Popen"] = None  # type: ignore[type-arg]

    def _prevent_sleep():
        import subprocess
        global _caffeinate_proc
        if _caffeinate_proc is None:
            _caffeinate_proc = subprocess.Popen(["caffeinate", "-d"])

    def _allow_sleep():
        global _caffeinate_proc
        if _caffeinate_proc:
            _caffeinate_proc.terminate()
            _caffeinate_proc = None

else:
    def _prevent_sleep(): pass
    def _allow_sleep():   pass

# ─── 提示音 ────────────────────────────────────────────────────────────────────

def _beep_success():
    """成功后三声上升提示音（阻塞，请在子线程中调用）"""
    try:
        if sys.platform == "win32":
            import winsound
            for freq in (880, 1100, 1400):
                winsound.Beep(freq, 220)
        elif sys.platform == "darwin":
            import subprocess
            subprocess.call(["afplay", "/System/Library/Sounds/Glass.aiff"])
        else:
            sys.stdout.write("\a\a\a")
            sys.stdout.flush()
    except Exception:
        pass

# ─── 配置 ─────────────────────────────────────────────────────────────────────

_CFG_PATH = Path.home() / "Desktop" / ".task_claimer_cfg.json"

_DEFAULTS: dict = {
    # 两条线路的 Base URL（不含路径）
    "line1_base": "",
    "line2_base": "",
    # 接口路径
    "list_path":  "/api/task/list",
    "claim_path": "/api/task/receive",
    # 认证
    "cookie":          "",
    "extra_headers":   "",   # JSON 字符串，如 {"Authorization":"******"}
    # 列表请求体（留空则使用 GET；填 JSON 则用 POST）
    "list_payload":    "",
    # 响应解析：任务数组在响应 JSON 中的点分路径，如 data.list / data / result.tasks
    "tasks_array_path": "data.list",
    # 任务字段名
    "claim_id_field":   "taskId",
    "claim_name_field": "taskName",
    "claim_qty_field":  "remain",
    # 认领请求体模板（{id} 会被替换为实际任务 ID）
    "claim_body_template": '{"taskId":"{id}"}',
    # 性能参数
    "concurrency":            6,
    "poll_interval":          0.3,   # 轮询间隔（秒）
    "list_connect_timeout":   0.6,   # 列表连接超时（秒）
    "list_read_timeout":      1.8,   # 列表读取超时（秒）
    "claim_connect_timeout":  0.3,   # 认领连接超时（秒）
    "claim_read_timeout":     0.8,   # 认领读取超时（秒）
    "burst_window_ms":        800,   # 发现目标后持续抢占的窗口（毫秒）
    # 杂项
    "anti_sleep":    True,
    "beep":          True,
    "last_line":     "线路1",
    "last_keyword":  "",
    "keywords":      [],
}


def _load_cfg() -> dict:
    cfg = dict(_DEFAULTS)
    if _CFG_PATH.exists():
        try:
            with open(_CFG_PATH, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:
            pass
    return cfg


def _save_cfg(cfg: dict):
    try:
        _CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_CFG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _cfg_base_url(cfg: dict) -> str:
    key = "line2_base" if cfg.get("last_line") == "线路2" else "line1_base"
    return str(cfg.get(key, "")).rstrip("/")


def _cfg_headers(cfg: dict) -> dict:
    headers: dict = {"Content-Type": "application/json"}
    if cfg.get("cookie"):
        headers["Cookie"] = cfg["cookie"]
    try:
        extra = json.loads(cfg.get("extra_headers") or "{}")
        if isinstance(extra, dict):
            headers.update(extra)
    except Exception:
        pass
    return headers


def _get_nested_value(obj, path: str):
    for key in path.split("."):
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, list) and key.isdigit():
            obj = obj[int(key)]
        else:
            return None
    return obj


async def _fetch_tasks_with_session(
    session: aiohttp.ClientSession, cfg: dict, log_fn=None, action_name: str = "查询"
) -> list:
    url     = _cfg_base_url(cfg) + str(cfg["list_path"])
    payload = str(cfg.get("list_payload", "")).strip()
    timeout = aiohttp.ClientTimeout(
        connect=float(cfg.get("list_connect_timeout", 0.6)),
        sock_read=float(cfg.get("list_read_timeout", 1.8)),
    )

    try:
        if payload:
            async with session.post(url, data=payload, timeout=timeout) as r:
                data, _ = await _read_json_or_text(r)
        else:
            async with session.get(url, timeout=timeout) as r:
                data, _ = await _read_json_or_text(r)
        arr = _get_nested_value(data, str(cfg["tasks_array_path"]))
        return arr if isinstance(arr, list) else []
    except asyncio.TimeoutError:
        if log_fn:
            if action_name == "查询":
                log_fn("⚠ 查询超时（已重连，将继续轮询）")
            else:
                log_fn(f"⚠ {action_name}超时")
        return []
    except Exception as exc:
        if log_fn:
            log_fn(f"⚠ {action_name}异常: {exc}")
        return []


async def _fetch_tasks_once(cfg: dict, log_fn=None, action_name: str = "查询") -> list:
    connector = aiohttp.TCPConnector(
        limit=8,
        limit_per_host=8,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    async with aiohttp.ClientSession(
        connector=connector, headers=_cfg_headers(cfg)
    ) as session:
        return await _fetch_tasks_with_session(session, cfg, log_fn, action_name)


async def _read_json_or_text(resp: aiohttp.ClientResponse) -> Tuple[dict, str]:
    try:
        data = await resp.json(content_type=None)
        if isinstance(data, dict):
            return data, ""
        return {}, json.dumps(data, ensure_ascii=False)
    except Exception:
        try:
            text = await resp.text()
        except Exception:
            text = ""
        try:
            parsed = json.loads(text) if text else {}
            return parsed if isinstance(parsed, dict) else {}, text
        except Exception:
            return {}, text

# ─── 异步引擎 ─────────────────────────────────────────────────────────────────

class Engine:
    """
    轮询器 (poller) 和认领器 (claimer) 完全解耦：
      poller  → 持续 list 查询，发现目标后写入 asyncio.Queue
      claimer → 监听队列，收到任务后在窗口内高并发认领
    """

    def __init__(self, cfg: dict, log_fn, status_fn):
        self._cfg       = cfg
        self._log       = log_fn
        self._set_status = status_fn
        self._stop      = asyncio.Event()
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=4)
        self.poll_count = 0
        self._start_ts  = 0.0

    # ── 工具 ──

    def _base_url(self) -> str:
        return _cfg_base_url(self._cfg)

    def _headers(self) -> dict:
        return _cfg_headers(self._cfg)

    @staticmethod
    def _get_nested(obj, path: str):
        """按点分路径从嵌套 dict/list 中取值，如 'data.list'"""
        return _get_nested_value(obj, path)

    @staticmethod
    def _is_success(data: dict, raw_text: str = "", http_status: Optional[int] = None) -> bool:
        """从响应 JSON 判断认领是否成功（兼容多种 code 字段约定）"""
        for field in ("success", "ok", "isSuccess"):
            val = data.get(field)
            if val is True:
                return True
            if isinstance(val, str) and val.lower() in ("true", "yes", "ok", "success"):
                return True

        for field in ("code", "status", "errno", "errCode", "err_code"):
            val = data.get(field)
            if val is None:
                continue
            if isinstance(val, int) and val in (0, 200):
                return True
            if isinstance(val, str) and val.lower() in ("0", "200", "ok", "success"):
                return True
        # 部分接口用 "msg":"ok" 或 "message":"success"
        for field in ("msg", "message"):
            val = str(data.get(field, "")).lower()
            if val in ("ok", "success", "成功"):
                return True

        for container_field in ("data", "result"):
            child = data.get(container_field)
            if isinstance(child, dict) and Engine._is_success(child):
                return True

        raw = raw_text.lower()
        if any(word in raw for word in ("success", "ok", "成功", "领取成功", "认领成功")):
            return True

        if http_status in (200, 201, 204) and not data and not raw.strip():
            return True
        return False

    # ── 轮询 ──

    async def _list_tasks(self, session: aiohttp.ClientSession) -> list:
        return await _fetch_tasks_with_session(session, self._cfg, self._log, "查询")

    async def _poller(self, session: aiohttp.ClientSession):
        keyword = self._cfg["last_keyword"]
        name_f  = self._cfg["claim_name_field"]
        id_f    = self._cfg["claim_id_field"]
        qty_f   = self._cfg["claim_qty_field"]
        interval = float(self._cfg["poll_interval"])

        self._start_ts = time.time()
        while not self._stop.is_set():
            self.poll_count += 1
            tasks = await self._list_tasks(session)

            for task in tasks:
                name = str(task.get(name_f, ""))
                if keyword and keyword not in name:
                    continue
                task_id = task.get(id_f)
                if task_id is None:
                    continue
                qty     = task.get(qty_f, "?")
                elapsed = int((time.time() - self._start_ts) * 1000)
                self._log(
                    f"🎯 发现目标 [{qty}余量] {name} "
                    f"(启动后 {elapsed}ms，第{self.poll_count}查)"
                )
                # 非阻塞放入队列（满了则跳过，避免堆积）
                try:
                    self._queue.put_nowait((task_id, name, time.time()))
                except asyncio.QueueFull:
                    pass
                break  # 本轮只处理第一个命中

            if not self._stop.is_set():
                await asyncio.sleep(interval)

    # ── 认领 ──

    async def _claim_once(self, session: aiohttp.ClientSession, task_id) -> bool:
        url  = self._base_url() + self._cfg["claim_path"]
        body = self._cfg["claim_body_template"].replace("{id}", str(task_id))
        timeout = aiohttp.ClientTimeout(
            connect=float(self._cfg["claim_connect_timeout"]),
            sock_read=float(self._cfg["claim_read_timeout"]),
        )
        try:
            async with session.post(url, data=body, timeout=timeout) as r:
                data, raw_text = await _read_json_or_text(r)
                return self._is_success(data, raw_text, r.status)
        except asyncio.TimeoutError:
            return False
        except Exception:
            return False

    async def _claimer(self, session: aiohttp.ClientSession):
        concurrency  = int(self._cfg["concurrency"])
        burst_window = float(self._cfg["burst_window_ms"]) / 1000.0

        while not self._stop.is_set():
            try:
                task_id, name, found_at = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            if self._stop.is_set():
                break

            deadline = found_at + burst_window
            self._log(f"🚀 开始抢占: {name}  (并发×{concurrency}，窗口{int(burst_window*1000)}ms)")

            attempt = 0
            success = False

            while time.time() < deadline and not success and not self._stop.is_set():
                coros   = [self._claim_once(session, task_id) for _ in range(concurrency)]
                results = await asyncio.gather(*coros, return_exceptions=True)
                attempt += concurrency
                if any(r is True for r in results):
                    success = True
                    break
                # 随机抖动 20~80ms 再下一轮
                await asyncio.sleep(random.uniform(0.02, 0.08))

            elapsed = int((time.time() - found_at) * 1000)
            if success:
                self._log(f"✅ 认领成功！{name}  (共发 {attempt} 次，耗时 {elapsed}ms)")
                self._set_status(f"✅ 已抢到: {name}")
                if self._cfg.get("beep"):
                    threading.Thread(target=_beep_success, daemon=True).start()
            else:
                self._log(f"❌ 窗口结束未抢到: {name}  (共发 {attempt} 次，耗时 {elapsed}ms)")

    # ── 主入口 ──

    async def run(self):
        connector = aiohttp.TCPConnector(
            limit=64,
            limit_per_host=32,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        async with aiohttp.ClientSession(
            connector=connector, headers=self._headers()
        ) as session:
            poller  = asyncio.create_task(self._poller(session))
            claimer = asyncio.create_task(self._claimer(session))
            done, pending = await asyncio.wait(
                [poller, claimer], return_when=asyncio.FIRST_EXCEPTION
            )
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc:
                    self._log(f"⚠ 引擎异常: {exc}")

    def stop(self):
        self._stop.set()

# ─── GUI ──────────────────────────────────────────────────────────────────────

_SETTINGS_ROWS = [
    ("线路1 地址 (Base URL)",           "line1_base"),
    ("线路2 地址 (Base URL)",           "line2_base"),
    ("列表接口路径",                     "list_path"),
    ("认领接口路径",                     "claim_path"),
    ("Cookie",                          "cookie"),
    ("额外请求头 (JSON 对象)",           "extra_headers"),
    ("列表请求体 (JSON，留空=GET)",      "list_payload"),
    ("任务数组路径 (点分，如 data.list)","tasks_array_path"),
    ("任务 ID 字段名",                   "claim_id_field"),
    ("任务名称字段名",                   "claim_name_field"),
    ("余量字段名",                       "claim_qty_field"),
    ("认领请求体模板 ({id} 为任务ID)",   "claim_body_template"),
    ("并发数",                           "concurrency"),
    ("轮询间隔 (秒)",                    "poll_interval"),
    ("查询连接超时 (秒)",                "list_connect_timeout"),
    ("查询读取超时 (秒)",                "list_read_timeout"),
    ("认领连接超时 (秒)",                "claim_connect_timeout"),
    ("认领读取超时 (秒)",                "claim_read_timeout"),
    ("抢占窗口 (毫秒)",                  "burst_window_ms"),
]

_INT_FIELDS   = {"concurrency", "burst_window_ms"}
_FLOAT_FIELDS = {
    "poll_interval",
    "list_connect_timeout",
    "list_read_timeout",
    "claim_connect_timeout",
    "claim_read_timeout",
}


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.cfg: dict = _load_cfg()
        self._engine: Optional[Engine]                 = None
        self._loop:   Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread]       = None
        self._refreshing = False

        self._build_ui()
        self._load_ui_values()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── 构建界面 ──

    def _build_ui(self):
        self.title("任务抢占工具 v2.0")
        self.minsize(780, 580)

        # ── 顶部操作栏 ──────────────────────────────────────────────────────────
        top = ttk.Frame(self, padding=(8, 6, 8, 4))
        top.pack(fill="x")

        ttk.Label(top, text="线路:").pack(side="left")
        self._line_var = tk.StringVar()
        ttk.Combobox(
            top, textvariable=self._line_var,
            values=["线路1", "线路2"], width=7, state="readonly"
        ).pack(side="left", padx=(2, 14))

        ttk.Label(top, text="目标关键词:").pack(side="left")
        self._kw_var = tk.StringVar()
        self._kw_cb = ttk.Combobox(top, textvariable=self._kw_var, width=30)
        self._kw_cb.pack(side="left", padx=(2, 14))

        self._btn_refresh = ttk.Button(top, text="↻ 刷新任务列表", command=self._refresh_tasks)
        self._btn_refresh.pack(side="right", padx=(4, 0))

        self._btn_stop  = ttk.Button(top, text="■ 停止", command=self._stop,
                                      state="disabled")
        self._btn_stop.pack(side="right", padx=4)
        self._btn_start = ttk.Button(top, text="▶ 开始", command=self._start)
        self._btn_start.pack(side="right", padx=4)

        # ── 标签页 ─────────────────────────────────────────────────────────────
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=(0, 4))

        # 任务列表页
        tasks_frame = ttk.Frame(nb, padding=4)
        nb.add(tasks_frame, text=" 任务列表 ")

        self._tasks_tree = ttk.Treeview(
            tasks_frame,
            columns=("name", "qty", "task_id"),
            show="headings",
            height=16,
        )
        self._tasks_tree.heading("name", text="任务名称")
        self._tasks_tree.heading("qty", text="余量")
        self._tasks_tree.heading("task_id", text="任务ID")
        self._tasks_tree.column("name", width=420, anchor="w")
        self._tasks_tree.column("qty", width=80, anchor="center")
        self._tasks_tree.column("task_id", width=180, anchor="w")

        task_vsb = ttk.Scrollbar(tasks_frame, orient="vertical", command=self._tasks_tree.yview)
        self._tasks_tree.configure(yscrollcommand=task_vsb.set)
        self._tasks_tree.pack(side="left", fill="both", expand=True)
        task_vsb.pack(side="right", fill="y")

        # 日志页
        log_frame = ttk.Frame(nb, padding=4)
        nb.add(log_frame, text=" 运行日志 ")

        self._log_txt = scrolledtext.ScrolledText(
            log_frame, height=18, wrap="word", state="disabled",
            font=("Consolas", 9) if sys.platform == "win32" else ("Monaco", 10),
            bg="#0d0d0d", fg="#d4d4d4", insertbackground="white",
        )
        self._log_txt.pack(fill="both", expand=True)
        self._log_txt.tag_config("ok",   foreground="#4ec94e")
        self._log_txt.tag_config("fail", foreground="#f47575")
        self._log_txt.tag_config("warn", foreground="#e8c55a")
        self._log_txt.tag_config("info", foreground="#7ecfff")

        ttk.Button(log_frame, text="清空日志", command=self._clear_log
                   ).pack(side="right", pady=(4, 0))

        # 设置页
        cfg_outer = ttk.Frame(nb)
        nb.add(cfg_outer, text=" 设置 ")

        canvas = tk.Canvas(cfg_outer, borderwidth=0, highlightthickness=0)
        vsb    = ttk.Scrollbar(cfg_outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        cfg_frame = ttk.Frame(canvas, padding=12)
        cfg_frame.columnconfigure(1, weight=1)
        win_id = canvas.create_window((0, 0), window=cfg_frame, anchor="nw")

        def _on_frame_configure(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
        def _on_canvas_configure(e):
            canvas.itemconfig(win_id, width=e.width)
        cfg_frame.bind("<Configure>", _on_frame_configure)
        canvas.bind("<Configure>", _on_canvas_configure)

        self._cfg_vars: dict[str, tk.StringVar] = {}
        for i, (label, key) in enumerate(_SETTINGS_ROWS):
            ttk.Label(cfg_frame, text=label + ":", anchor="e").grid(
                row=i, column=0, sticky="e", padx=(0, 8), pady=3)
            v = tk.StringVar()
            ttk.Entry(cfg_frame, textvariable=v).grid(
                row=i, column=1, sticky="ew", pady=3)
            self._cfg_vars[key] = v

        chk_row = ttk.Frame(cfg_frame)
        chk_row.grid(row=len(_SETTINGS_ROWS), column=0, columnspan=2,
                      sticky="w", pady=(8, 2))
        self._anti_sleep_var = tk.BooleanVar()
        ttk.Checkbutton(chk_row, text="防休眠",
                         variable=self._anti_sleep_var).pack(side="left", padx=4)
        self._beep_var = tk.BooleanVar()
        ttk.Checkbutton(chk_row, text="成功提示音",
                         variable=self._beep_var).pack(side="left", padx=4)

        ttk.Button(cfg_frame, text="保存设置", command=self._save_settings
                   ).grid(row=len(_SETTINGS_ROWS) + 1, column=1,
                           sticky="e", pady=(6, 2))

        # ── 状态栏 ─────────────────────────────────────────────────────────────
        self._status_var = tk.StringVar(value="就绪")
        ttk.Label(self, textvariable=self._status_var,
                  relief="sunken", anchor="w", padding=(6, 2)
                  ).pack(fill="x", side="bottom")

    # ── 数据双向绑定 ──

    def _load_ui_values(self):
        self._line_var.set(self.cfg.get("last_line", "线路1"))
        self._kw_var.set(self.cfg.get("last_keyword", ""))
        self._kw_cb["values"] = self.cfg.get("keywords", [])
        self._anti_sleep_var.set(self.cfg.get("anti_sleep", True))
        self._beep_var.set(self.cfg.get("beep", True))
        for key, var in self._cfg_vars.items():
            var.set(str(self.cfg.get(key, "")))

    def _collect_ui_to_cfg(self):
        for key, var in self._cfg_vars.items():
            raw = var.get().strip()
            if key in _INT_FIELDS:
                try: raw = int(raw)
                except ValueError: pass
            elif key in _FLOAT_FIELDS:
                try: raw = float(raw)
                except ValueError: pass
            self.cfg[key] = raw
        self.cfg["anti_sleep"] = self._anti_sleep_var.get()
        self.cfg["beep"]       = self._beep_var.get()
        self.cfg["last_line"]  = self._line_var.get()
        self.cfg["last_keyword"] = self._kw_var.get().strip()

    def _save_settings(self):
        self._collect_ui_to_cfg()
        _save_cfg(self.cfg)
        self._status_var.set("设置已保存 ✓")

    def _add_keyword_to_history(self, kw: str):
        if not kw:
            return
        kws: list = list(self.cfg.get("keywords", []))
        if kw in kws:
            kws.remove(kw)
        kws.insert(0, kw)
        kws = kws[:40]
        self.cfg["keywords"] = kws
        self._kw_cb["values"] = kws

    def _refresh_tasks(self):
        if self._refreshing:
            return
        self._collect_ui_to_cfg()
        _save_cfg(self.cfg)

        base = self.cfg["line2_base"] if self.cfg["last_line"] == "线路2" \
               else self.cfg["line1_base"]
        if not base:
            messagebox.showwarning(
                "配置不完整",
                f'请先在"设置"页填写 {self.cfg["last_line"]} 的 Base URL'
            )
            return

        self._refreshing = True
        self._btn_refresh.config(state="disabled")
        self._status_var.set("正在刷新任务列表…")
        threading.Thread(target=self._run_refresh_tasks, daemon=True).start()

    def _run_refresh_tasks(self):
        try:
            snapshot = dict(self.cfg)
            tasks = asyncio.run(_fetch_tasks_once(snapshot, self._log, "刷新"))
            name_f = snapshot["claim_name_field"]
            qty_f = snapshot["claim_qty_field"]
            id_f = snapshot["claim_id_field"]
            rows = [
                (
                    str(task.get(name_f, "")),
                    str(task.get(qty_f, "?")),
                    str(task.get(id_f, "")),
                )
                for task in tasks
            ]
            rows.sort(key=lambda item: item[0])
            self.after(0, self._finish_refresh_tasks, rows)
        except Exception as exc:
            self._log(f"⚠ 刷新异常: {exc}")
            self.after(0, self._finish_refresh_tasks, [])

    def _finish_refresh_tasks(self, rows):
        for item in self._tasks_tree.get_children():
            self._tasks_tree.delete(item)
        for row in rows:
            self._tasks_tree.insert("", "end", values=row)

        self._refreshing = False
        self._btn_refresh.config(state="normal")
        self._status_var.set(f"任务列表已刷新，共 {len(rows)} 条")
        self._log(f"📋 已刷新任务列表，共 {len(rows)} 条")

    # ── 启动 / 停止 ──

    def _start(self):
        self._collect_ui_to_cfg()
        self._add_keyword_to_history(self.cfg["last_keyword"])
        _save_cfg(self.cfg)

        # 简单校验
        base = self.cfg["line2_base"] if self.cfg["last_line"] == "线路2" \
               else self.cfg["line1_base"]
        if not base:
            messagebox.showwarning(
                "配置不完整",
                f'请先在"设置"页填写 {self.cfg["last_line"]} 的 Base URL'
            )
            return
        if not self.cfg.get("cookie"):
            if not messagebox.askyesno(
                "未设置 Cookie",
                "Cookie 为空，接口可能返回未授权错误。\n确认继续？",
            ):
                return

        if self.cfg["anti_sleep"]:
            try: _prevent_sleep()
            except Exception: pass

        self._btn_start.config(state="disabled")
        self._btn_stop.config(state="normal")
        self._status_var.set(
            f'运行中…  线路: {self.cfg["last_line"]}'
            f'  关键词: {self.cfg["last_keyword"] or "(全部)"}'
        )

        self._loop   = asyncio.new_event_loop()
        self._engine = Engine(
            cfg       = dict(self.cfg),
            log_fn    = self._log,
            status_fn = lambda s: self.after(0, self._status_var.set, s),
        )
        self._thread = threading.Thread(target=self._run_engine, daemon=True)
        self._thread.start()

    def _run_engine(self):
        try:
            self._loop.run_until_complete(self._engine.run())
        except Exception as exc:
            self._log(f"⚠ 引擎意外退出: {exc}")
        finally:
            self.after(0, self._on_engine_done)

    def _stop(self):
        if self._engine:
            self._engine.stop()
        self._status_var.set("正在停止…")

    def _on_engine_done(self):
        self._btn_start.config(state="normal")
        self._btn_stop.config(state="disabled")
        if self.cfg.get("anti_sleep"):
            try: _allow_sleep()
            except Exception: pass
        if "✅" not in self._status_var.get():
            self._status_var.set("已停止")

    # ── 日志 ──

    def _log(self, msg: str):
        def _do():
            ts   = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            line = f"[{ts}] {msg}\n"
            tag  = ""
            m    = msg.lower()
            if "✅" in msg or "成功" in msg:
                tag = "ok"
            elif "❌" in msg or "失败" in msg:
                tag = "fail"
            elif "⚠" in msg or "超时" in msg or "异常" in msg:
                tag = "warn"
            elif "🎯" in msg or "🚀" in msg or "发现" in msg:
                tag = "info"
            self._log_txt.config(state="normal")
            self._log_txt.insert("end", line, tag)
            self._log_txt.see("end")
            self._log_txt.config(state="disabled")
        self.after(0, _do)

    def _clear_log(self):
        self._log_txt.config(state="normal")
        self._log_txt.delete("1.0", "end")
        self._log_txt.config(state="disabled")

    # ── 窗口关闭 ──

    def _on_close(self):
        self._stop()
        self._collect_ui_to_cfg()
        _save_cfg(self.cfg)
        self.destroy()


# ─── 入口 ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
