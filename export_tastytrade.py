#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export tastytrade broker-side data to CSV (patched for SDK variants):
- Orders, Transactions, Positions, Net Liq History (account-side)
- Market Data: OHLCV candles (DXLink stream/backfill), Option Chain

Compatible with SDKs where:
- Account.get(session) is used (instead of Account.get_accounts)
- Some resources are exposed as account *instance* methods; we fall back to
  class methods if needed.

Dependencies: tastytrade, python-dotenv
"""

import os
import sys
import csv
import time
import asyncio
import datetime as dt
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Official SDK
from tastytrade import Session, Account

# Optional modules (version-dependent)
try:
    from tastytrade import Transaction
except Exception:
    Transaction = None

try:
    from tastytrade import Order
except Exception:
    Order = None

try:
    from tastytrade import Position
except Exception:
    Position = None

try:
    from tastytrade import NetLiqHistory
except Exception:
    NetLiqHistory = None

try:
    from tastytrade import OptionChain
except Exception:
    OptionChain = None

try:
    from tastytrade import DXLinkStreamer
except Exception:
    DXLinkStreamer = None


# ------------------------------ Utils ------------------------------

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def iso_date(d: dt.date) -> str:
    return d.isoformat()


def parse_days_to_range(days: int):
    end = dt.date.today()
    start = end - dt.timedelta(days=days)
    return start, end


def load_config():
    # Load .env from .venv first (if present), then project root
    load_dotenv(dotenv_path=Path('.venv')/'.env', override=False)
    load_dotenv(override=False)
    cfg = {
        "username": os.getenv("TASTY_USERNAME") or "",
        "password": os.getenv("TASTY_PASSWORD") or "",
        "mfa": os.getenv("TASTY_MFA_CODE") or None,
        "account": os.getenv("ACCOUNT_NUMBER") or None,
        "outdir": os.getenv("OUTPUT_DIR") or "exports",
    }
    return cfg


def require_module(mod, name: str):
    if mod is None:
        print(f"[ERROR] Your installed tastytrade SDK does not expose '{name}'. "
              f"Please upgrade: pip install --upgrade tastytrade", file=sys.stderr)
        sys.exit(2)


# ------------------------------ Session/Login ------------------------------

def login(username: str, password: str, mfa: Optional[str] = None) -> Session:
    if not username or not password:
        print("[ERROR] Missing username/password. Provide via .env or CLI.", file=sys.stderr)
        sys.exit(2)
    try:
        return Session(username, password, mfa_code=mfa) if mfa else Session(username, password)
    except TypeError:
        return Session(username, password, mfa) if mfa else Session(username, password)


def pick_account(session: Session, preferred: Optional[str]) -> Account:
    # Patched: use Account.get(session)
    try:
        accounts = Account.get(session)
    except Exception as e:
        print(f"[ERROR] Unable to fetch accounts via Account.get(session): {e}", file=sys.stderr)
        sys.exit(2)

    if not accounts:
        print("[ERROR] No accounts returned for your login.", file=sys.stderr)
        sys.exit(2)

    if preferred:
        for a in accounts:
            if getattr(a, "account_number", None) == preferred:
                return a
        print(f"[WARN] Preferred account '{preferred}' not found. Using the first one.")

    return accounts[0]


# ------------------------------ Exporters ------------------------------

def export_transactions(session: Session, account: Account, account_number: str, outdir: Path,
                        start: dt.date, end: dt.date):
    print(f"[INFO] Fetching transactions {start} ~ {end} ...")
    txns = []
    # Prefer instance method if available
    try:
        txns = account.get_transaction_history(session, start_date=iso_date(start), end_date=iso_date(end))
    except Exception:
        if Transaction is None:
            print("[ERROR] SDK lacks Transaction and account.get_transaction_history().", file=sys.stderr)
            sys.exit(2)
        txns = Transaction.get_transactions(session, account_number, start_date=iso_date(start), end_date=iso_date(end))

    ensure_dir(outdir)
    out = outdir / f"transactions_{account_number}_{start}_{end}.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(['id','date','type','symbol','quantity','price','amount','description'])
        for t in txns:
            w.writerow([
                getattr(t, 'id', ''),
                getattr(t, 'transaction_date', getattr(t, 'date', '')),
                getattr(t, 'type', ''),
                (getattr(t, 'symbol', '') or '').upper(),
                getattr(t, 'quantity', ''),
                getattr(t, 'price', ''),
                getattr(t, 'amount', ''),
                getattr(t, 'description', ''),
            ])
    print(f"[OK] Wrote {out}")


def export_orders(session: Session, account: Account, account_number: str, outdir: Path, status: str = 'all'):
    print(f"[INFO] Fetching orders (status={status}) ...")
    orders = []
    # Try common instance methods first
    tried = []
    for meth_name, kwargs in [
        ('get_order_history', {'status': status}),
        ('get_live_orders', {}),
    ]:
        try:
            meth = getattr(account, meth_name)
            orders = meth(session, **kwargs)
            break
        except Exception as e:
            tried.append(f"{meth_name} -> {e}")
            continue
    if not orders:
        # Fallback to class method (older SDKs)
        if Order is None:
            print("[ERROR] SDK lacks Order and account order methods. Tried: " + ", ".join(tried), file=sys.stderr)
            sys.exit(2)
        try:
            orders = Order.get_orders(session, account_number, status=status)
        except Exception as e:
            print("[ERROR] Order.get_orders failed and no other methods worked:\n  " + "\n  ".join(tried) + f"\n  get_orders -> {e}", file=sys.stderr)
            sys.exit(2)

    ensure_dir(outdir)
    out = outdir / f"orders_{account_number}_{status}.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(['id','status','underlying_symbol','asset_type','price_effect','time_in_force','entered_time','legs','executions'])
        for o in orders:
            legs = getattr(o, 'legs', None)
            execs = getattr(o, 'executions', None)
            legs_s = ''
            execs_s = ''
            if legs:
                legs_s = '; '.join([
                    f"{getattr(l, 'instrument_type', '')} {getattr(l, 'symbol', '')} x{getattr(l, 'quantity', '')} @{getattr(l, 'price', '')}"
                    for l in legs
                ])
            if execs:
                execs_s = '; '.join([
                    f"{getattr(e, 'symbol', '')} x{getattr(e, 'quantity', '')} @{getattr(e, 'price', '')} on {getattr(e, 'executed_at', '')}"
                    for e in execs
                ])
            w.writerow([
                getattr(o, 'id', ''),
                getattr(o, 'status', ''),
                getattr(o, 'underlying_symbol', ''),
                getattr(o, 'asset_type', ''),
                getattr(o, 'price_effect', ''),
                getattr(o, 'time_in_force', ''),
                getattr(o, 'entered_time', ''),
                legs_s,
                execs_s,
            ])
    print(f"[OK] Wrote {out}")


def export_positions(session: Session, account: Account, account_number: str, outdir: Path):
    print("[INFO] Fetching positions ...")
    try:
        positions = account.get_positions(session)
    except Exception:
        if Position is None:
            print("[ERROR] SDK lacks Position and account.get_positions().", file=sys.stderr)
            sys.exit(2)
        positions = Position.get_positions(session, account_number)

    ensure_dir(outdir)
    out = outdir / f"positions_{account_number}.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(['symbol','instrument_type','quantity','average_price','mark','pnl_open','pnl_day'])
        for p in positions:
            w.writerow([
                getattr(p, 'symbol', ''),
                getattr(p, 'instrument_type', ''),
                getattr(p, 'quantity', ''),
                getattr(p, 'average_price', ''),
                getattr(p, 'mark', ''),
                getattr(p, 'pnl_open', ''),
                getattr(p, 'pnl_day', ''),
            ])
    print(f"[OK] Wrote {out}")


def export_netliq(session: Session, account: Account, account_number: str, outdir: Path,
                  start: dt.date, end: dt.date):
    print(f"[INFO] Fetching net liq history {start} ~ {end} ...")
    data = []
    # Some SDKs expose as account method; otherwise use NetLiqHistory class
    try:
        data = account.get_net_liq_history(session, start_date=iso_date(start), end_date=iso_date(end))
    except Exception:
        if NetLiqHistory is None:
            print("[ERROR] SDK lacks NetLiqHistory and account.get_net_liq_history().", file=sys.stderr)
            sys.exit(2)
        data = NetLiqHistory.get_history(session, account_number, start_date=iso_date(start), end_date=iso_date(end))

    ensure_dir(outdir)
    out = outdir / f"netliq_{account_number}_{start}_{end}.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(['date','net_liquidating_value'])
        for d in data:
            w.writerow([
                getattr(d, 'date', ''),
                getattr(d, 'net_liquidating_value', getattr(d, 'net_liq', '')),
            ])
    print(f"[OK] Wrote {out}")


def export_option_chain(session: Session, symbol: str, outdir: Path):
    if OptionChain is None:
        print("[WARN] SDK missing OptionChain; skip option-chain.")
        return
    print(f"[INFO] Fetching option chain for {symbol} ...")
    try:
        chain = OptionChain.get_chain(session, symbol)
    except Exception as e:
        print(f"[WARN] Option chain fetch failed for {symbol}: {e}")
        return

    ensure_dir(outdir)
    out = outdir / f"option_chain_{symbol}.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(['expiration','strike','right','bid','ask','last','implied_vol','delta','gamma','theta','vega'])
        expirations = getattr(chain, 'expirations', None) or getattr(chain, 'items', [])
        for exp in expirations:
            exp_date = getattr(exp, 'expiration_date', getattr(exp, 'expiration', ''))
            options = getattr(exp, 'options', getattr(exp, 'items', []))
            for opt in options:
                w.writerow([
                    exp_date,
                    getattr(opt, 'strike_price', getattr(opt, 'strike', '')),
                    getattr(opt, 'option_type', getattr(opt, 'right', '')),
                    getattr(opt, 'bid', ''),
                    getattr(opt, 'ask', ''),
                    getattr(opt, 'last', ''),
                    getattr(opt, 'implied_volatility', getattr(opt, 'iv', '')),
                    getattr(opt, 'delta', ''),
                    getattr(opt, 'gamma', ''),
                    getattr(opt, 'theta', ''),
                    getattr(opt, 'vega', ''),
                ])
    print(f"[OK] Wrote {out}")


async def export_candles_stream(session: Session, symbol: str, interval: str, max_bars: int, outdir: Path):
    if DXLinkStreamer is None:
        print("[WARN] SDK missing DXLinkStreamer; skip candles.")
        return
    ensure_dir(outdir)
    out = outdir / f"candles_{symbol}_{interval}.csv"
    print(f"[INFO] Subscribing candles for {symbol} @ {interval} (max {max_bars} bars) ...")

    count = 0
    try:
        async with DXLinkStreamer(session) as s:
            await s.connect()
            stream = None
            if hasattr(s, 'subscribe_candles'):
                stream = await s.subscribe_candles(symbol, interval)
            elif hasattr(s, 'subscribe'):
                stream = await s.subscribe('candles', {'symbol': symbol, 'interval': interval})
            else:
                print("[WARN] No candles subscription method found; skip.")
                return

            with out.open('w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['end_time','open','high','low','close','volume'])
                async for c in stream:
                    end_ms = c.get('endTime') or c.get('end_time') or c.get('t')
                    ts = end_ms
                    try:
                        if end_ms is not None:
                            ts = dt.datetime.utcfromtimestamp(int(end_ms)/1000).isoformat()
                    except Exception:
                        pass
                    w.writerow([
                        ts,
                        c.get('open') or c.get('o'),
                        c.get('high') or c.get('h'),
                        c.get('low') or c.get('l'),
                        c.get('close') or c.get('c'),
                        c.get('volume') or c.get('v','')
                    ])
                    count += 1
                    if count >= max_bars:
                        break
        print(f"[OK] Wrote {out} ({count} bars)")
    except Exception as e:
        print(f"[WARN] Candles fetch failed for {symbol}: {e}")


# ------------------------------ CLI ------------------------------

import argparse

def main():
    cfg = load_config()

    parser = argparse.ArgumentParser(description="Export tastytrade broker-side & market data to CSV (SDK-compatible)")
    parser.add_argument("--username", default=cfg["username"], help="tastytrade login email")
    parser.add_argument("--password", default=cfg["password"], help="tastytrade password")
    parser.add_argument("--mfa", default=cfg["mfa"], help="one-time MFA code if required")
    parser.add_argument("--account", default=cfg["account"], help="account number (optional)")
    parser.add_argument("--outdir", default=cfg["outdir"], help="output dir (default: exports)")

    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("whoami", help="Show your accounts and default pick")

    p_tx = sub.add_parser("transactions", help="Export transactions")
    p_tx.add_argument("--days", type=int, default=90, help="range length in days (default: 90)")

    p_ord = sub.add_parser("orders", help="Export orders")
    p_ord.add_argument("--status", default="all", choices=["all","open","filled","cancelled"],
                       help="order status filter (default: all)")

    sub.add_parser("positions", help="Export positions")

    p_nl = sub.add_parser("netliq", help="Export net liq history")
    p_nl.add_argument("--days", type=int, default=365, help="range length in days (default: 365)")

    p_oc = sub.add_parser("option-chain", help="Export option chain for a symbol")
    p_oc.add_argument("--symbol", required=True)

    p_cd = sub.add_parser("candles", help="Export OHLCV candles via DXLink stream/backfill")
    p_cd.add_argument("--symbol", required=True)
    p_cd.add_argument("--interval", default="1m", help="e.g., 1m/5m/15m/1h/1d")
    p_cd.add_argument("--max-bars", type=int, default=1000, help="max number of bars to collect (default: 1000)")

    args = parser.parse_args()

    outdir = Path(args.outdir)
    ensure_dir(outdir)

    # login
    session = login(args.username, args.password, args.mfa)

    if args.cmd == "whoami":
        try:
            accounts = Account.get(session)
        except Exception as e:
            print(f"[ERROR] Account.get(session) failed: {e}", file=sys.stderr)
            sys.exit(2)
        print("Accounts:")
        for a in accounts:
            print(f"  - {a.account_number}  ({getattr(a, 'account_type_name', '')})")
        picked = pick_account(session, args.account)
        print(f"Default pick: {picked.account_number}")
        return

    # choose account
    account = pick_account(session, args.account)
    acct_num = account.account_number

    if args.cmd == "transactions":
        start, end = parse_days_to_range(args.days)
        export_transactions(session, account, acct_num, outdir, start, end)
    elif args.cmd == "orders":
        export_orders(session, account, acct_num, outdir, args.status)
    elif args.cmd == "positions":
        export_positions(session, account, acct_num, outdir)
    elif args.cmd == "netliq":
        start, end = parse_days_to_range(args.days)
        export_netliq(session, account, acct_num, outdir, start, end)
    elif args.cmd == "option-chain":
        export_option_chain(session, args.symbol, outdir)
    elif args.cmd == "candles":
        asyncio.run(export_candles_stream(session, args.symbol, args.interval, args.max_bars, outdir))
    else:
        print("[ERROR] Unknown command.", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
