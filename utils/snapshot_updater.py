# -*- coding: utf-8 -*-
"""
收盘后补当日 K 线：新浪批量快照 + 腾讯历史接口补缺口。

为什么需要它
------------
仓库原本的每日增量走 TickFlow 批量接口（utils/kline_updater.py）。
实测该接口的数据当天并不更新（2026-10-15 检查，返回的最新日期是 2026-09-14），
于是 KlineUpdater 的“数据源尚未就绪”分支会把整次更新静默跳过，
云端选股便会一直用同一份陈旧数据重复选股——表现就是“天天推送同样的结果”。

新浪 hq.sinajs.cn 的实时快照在收盘后即为当日最终行情（开/高/低/收/量），
全市场 5000+ 只约 3 秒取完，不需要拉完整历史，非常适合每日增量。
若库里缺的是中间几天（例如上次运行失败/被跳过），再用腾讯 fqkline
接口按股票把缺口补齐。
"""
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

BEIJING = timezone(timedelta(hours=8))

SINA_URL = "https://hq.sinajs.cn/list="
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://finance.sina.com.cn",
}

_UPSERT_SQL = (
    "INSERT INTO stock_kline(code, date, open, high, low, close, volume) "
    "VALUES(?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT(code, date) DO UPDATE SET "
    "open = excluded.open, high = excluded.high, low = excluded.low, "
    "close = excluded.close, volume = excluded.volume"
)


def beijing_now() -> datetime:
    """当前北京时间。

    GitHub Actions 的 runner 时区是 UTC，直接 datetime.now() 会把
    北京时间 15:10（UTC 07:10）判成“尚未收盘”，导致每日更新永远被跳过。
    """
    return datetime.now(BEIJING)


def to_sina_symbol(code: str) -> Optional[str]:
    if not code or len(code) != 6 or not code.isdigit():
        return None
    if code[0] == "6":
        return "sh" + code
    if code[0] in ("0", "3"):
        return "sz" + code
    if code[0] in ("4", "8"):
        return "bj" + code
    return None


def _weekdays_between(start: str, end: str) -> List[str]:
    """(start, end) 之间严格开区间的工作日，格式 YYYY-MM-DD。"""
    try:
        d1 = datetime.strptime(start, "%Y-%m-%d").date()
        d2 = datetime.strptime(end, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return []
    out: List[str] = []
    cur = d1 + timedelta(days=1)
    while cur < d2:
        if cur.weekday() < 5:
            out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


class SnapshotUpdater:
    """用实时快照 + 腾讯历史补齐 stock_kline 的最近交易日数据。"""

    def __init__(self, db_manager, batch_size: int = 100, timeout: int = 15):
        self.db = db_manager
        self.batch_size = batch_size
        self.timeout = timeout

    # ------------------------------------------------------------------ 读
    def db_max_date(self) -> Optional[str]:
        try:
            row = self.db.query_one("SELECT MAX(date) AS d FROM stock_kline")
            return row.get("d") if row else None
        except Exception as e:  # pragma: no cover
            logger.warning("读取库内最新K线日期失败: %s", e)
            return None

    # -------------------------------------------------------------- 快照
    def fetch_snapshot(self, codes: List[str]) -> Dict[str, Tuple]:
        """批量拉新浪快照。

        返回 {code: (date, open, high, low, close, volume_hand)}
        """
        out: Dict[str, Tuple] = {}
        symbols = [(c, to_sina_symbol(c)) for c in codes]
        symbols = [(c, s) for c, s in symbols if s]

        session = requests.Session()
        session.trust_env = False  # 忽略系统/环境变量里的代理，避免被本地代理卡死

        for i in range(0, len(symbols), self.batch_size):
            chunk = symbols[i:i + self.batch_size]
            url = SINA_URL + ",".join(sym for _, sym in chunk)
            try:
                resp = session.get(url, headers=_HEADERS, timeout=self.timeout)
                text = resp.content.decode("gbk", errors="replace")
            except Exception as e:
                logger.warning("新浪快照批次 %d 获取失败: %s", i // self.batch_size + 1, e)
                continue
            for line in text.splitlines():
                parsed = self._parse_snapshot_line(line)
                if parsed:
                    out[parsed[0]] = parsed[1]
        return out

    @staticmethod
    def _parse_snapshot_line(line: str) -> Optional[Tuple[str, Tuple]]:
        if '="' not in line:
            return None
        key, _, value = line.partition('="')
        sym = key.split("hq_str_")[-1].strip()
        if len(sym) < 3:
            return None
        fields = value.rstrip('";').split(",")
        # 0 名称 1 今开 2 昨收 3 最新价 4 最高 5 最低 ... 8 成交量(股) ... 30 日期
        if len(fields) < 32:
            return None
        date_str = fields[30]
        if len(date_str) != 10 or date_str.count("-") != 2:
            return None
        try:
            open_p = float(fields[1])
            close_p = float(fields[3])
            high_p = float(fields[4])
            low_p = float(fields[5])
            volume_shares = float(fields[8])
        except (TypeError, ValueError):
            return None
        if close_p <= 0 or open_p <= 0:
            return None
        # 库里 volume 单位是“手”，新浪给的是“股”
        return sym[2:], (date_str, open_p, high_p, low_p, close_p, int(round(volume_shares / 100.0)))

    # ---------------------------------------------------------- 腾讯补缺口
    def _fetch_tencent_days(self, code: str, count: int) -> Optional[List[Tuple]]:
        sym = to_sina_symbol(code)
        if not sym:
            return None
        params = {"param": f"{sym},day,,,{max(count, 5)},qfq"}
        try:
            resp = requests.get(
                TENCENT_KLINE_URL, params=params, headers={"User-Agent": _HEADERS["User-Agent"]},
                timeout=self.timeout,
            )
            payload = resp.json()
            node = (payload.get("data") or {}).get(sym) or {}
            rows = node.get("qfqday") or node.get("day") or []
        except Exception:
            return None
        out = []
        for r in rows:
            if len(r) < 6:
                continue
            try:
                out.append((
                    r[0], float(r[1]), float(r[3]), float(r[4]), float(r[2]),
                    int(round(float(r[5]))),   # 腾讯给的就是“手”
                ))
            except (TypeError, ValueError):
                continue
        return out

    def _backfill_gap(self, codes: List[str], start_date: str, end_date: str,
                      concurrency: int = 12) -> int:
        """补齐 (start_date, end_date] 区间缺失的K线。start_date=None 表示不限。"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        try:
            span_days = (datetime.strptime(end_date, "%Y-%m-%d")
                         - datetime.strptime(start_date, "%Y-%m-%d")).days
        except (ValueError, TypeError):
            span_days = 10
        count = min(max(span_days + 10, 15), 120)

        rows: List[Tuple] = []

        def _job(code: str):
            days = self._fetch_tencent_days(code, count)
            if not days:
                return []
            picked = []
            for d, o, h, l, c, v in days:
                if d <= start_date or d > end_date:
                    continue
                picked.append((code, d, o, h, l, c, v))
            return picked

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(_job, c) for c in codes]
            for fut in as_completed(futures):
                try:
                    rows.extend(fut.result())
                except Exception:
                    continue

        if rows:
            self._write_rows(rows)
        return len(rows)

    # ------------------------------------------------------------------ 写
    def _write_rows(self, rows: List[Tuple]) -> None:
        db_path = Path(getattr(self.db, "db_path", "data/stock_selection.db"))
        conn = sqlite3.connect(str(db_path), timeout=30)
        try:
            with conn:
                conn.executemany(_UPSERT_SQL, rows)
        finally:
            conn.close()

    # ------------------------------------------------------------------ 主入口
    def update_latest(self, target_date: Optional[str] = None) -> Dict:
        """把最近一个交易日（或 target_date）补齐到 stock_kline。

        返回 {'success', 'date', 'written', 'backfilled', 'message'}
        """
        codes = self.db.list_all_stocks()
        if not codes:
            return {"success": False, "date": None, "written": 0,
                    "message": "库内没有股票，跳过快照更新"}

        before_max = self.db_max_date()
        snap = self.fetch_snapshot(codes)
        if not snap:
            return {"success": False, "date": None, "written": 0, "backfilled": 0,
                    "message": "新浪快照未取到任何数据"}

        if target_date:
            snap = {c: v for c, v in snap.items() if v[0] == target_date}
            if not snap:
                return {"success": False, "date": target_date, "written": 0, "backfilled": 0,
                        "message": f"快照中没有 {target_date} 的数据"}
            latest = target_date
        else:
            # 取出现次数最多的日期（= 最新交易日，少数停牌股仍是旧日期）
            counts: Dict[str, int] = {}
            for v in snap.values():
                counts[v[0]] = counts.get(v[0], 0) + 1
            latest = max(counts.items(), key=lambda kv: kv[1])[0]
            snap = {c: v for c, v in snap.items() if v[0] == latest}

        rows = [(code, v[0], v[1], v[2], v[3], v[4], v[5]) for code, v in snap.items()]
        self._write_rows(rows)

        backfilled = 0
        if before_max and before_max < latest:
            missing = _weekdays_between(before_max, latest)
            if missing:
                logger.info("检测到中间缺 %d 个交易日(%s)，用腾讯接口补齐",
                            len(missing), ",".join(missing))
                backfilled = self._backfill_gap(codes, before_max, latest)

        msg = f"已写入 {latest} 共 {len(rows)} 只"
        if before_max:
            msg += f"（库内原最新 {before_max}）"
        if backfilled:
            msg += f"，补缺口 {backfilled} 条"
        logger.info("快照补K线完成: %s", msg)
        return {"success": True, "date": latest, "written": len(rows),
                "backfilled": backfilled, "message": msg}
