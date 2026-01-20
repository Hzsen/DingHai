#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch export tastytrade data for symbols listed in a CSV (SDK-compatible):
- Market data: OHLCV candles (DXLink stream/backfill up to N bars), Option Chain
- Account-side (optional): Transactions & Orders filtered by those symbols

Patches for SDK variants:
- Use Account.get(session) to list accounts (instead of Account.get_accounts)
- Prefer account instance methods (get_transaction_history / get_order_history / get_positions / get_net_liq_history),
  and gracefully fall back to class methods if needed.

Dependencies: tastytrade, python-dotenv
"""

import os
import sys
import csv
import time
import asyncio
import datetime as dt
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from tastytrade import Session, Account

# Optional / version-dependent symbols
try:
    from tastytrade import OptionChain
except Exception:
    OptionChain = None

try:
    from tastytrade import DXLinkStreamer
except Exception:
    DXLinkStreamer = None

try:
    from tastytrade import Transaction
except Exception:
    Transaction = None

try:
    from tastytrade import Order
except Exception:
    Order = None


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def load_config():
    # Read secrets from .venv/.env first (if present), then project root .env
    load_dotenv(dotenv_path=Path('.venv')/'.env', override=False)
    load_dotenv(override=False)
    return {
        'username': os.getenv('TASTY_USERNAME') or 'zhe409',
        'password': os.getenv('TASTY_PASSWORD') or 'Bitedust@8964',
        'mfa': os.getenv('TASTY_MFA_CODE') or None,
        'account': os.getenv('ACCOUNT_NUMBER') or None,
        'outdir': os.getenv('OUTPUT_DIR') or 'exports_by_csv',
    }


def login(username: str, password: str, mfa: Optional[str]):
    if not username or not password:
        print('[ERROR] Missing username/password (use .env or CLI).', file=sys.stderr)
        sys.exit(2)
    try:
        return Session(username, password, mfa_code=mfa) if mfa else Session(username, password)
    except TypeError:
        return Session(username, password, mfa) if mfa else Session(username, password)


def pick_account(session: Session, preferred: Optional[str]) -> Account:
    try:
        accounts = Account.get(session)
    except Exception as e:
        print(f"[ERROR] Account.get(session) failed: {e}", file=sys.stderr)
        sys.exit(2)
    if not accounts:
        print('[ERROR] No accounts for this login.', file=sys.stderr)
        sys.exit(2)
    if preferred:
        for a in accounts:
            if getattr(a, 'account_number', None) == preferred:
                return a
        print(f"[WARN] Preferred account '{preferred}' not found; using first.")
    return accounts[0]


def read_symbols(csv_path: Path, col: str, no_header: bool = False, col_index: int = 0) -> List[str]:
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    syms: List[str] = []
    with csv_path.open("r", newline="") as f:
        sample = f.read(2048)
        f.seek(0)
        has_header = False if no_header else csv.Sniffer().has_header(sample)

        if has_header and not no_header:
            rdr = csv.DictReader(f)
            fields = rdr.fieldnames or []
            if col not in fields:
                if len(fields) == 1:
                    col = fields[0]
                    print(f"[INFO] Using single column '{col}' from header.")
                else:
                    print(f"[ERROR] Column '{col}' not in CSV. Columns: {fields}", file=sys.stderr)
                    sys.exit(2)
            for row in rdr:
                s = (row.get(col) or "").strip().upper()
                if s:
                    s = s.replace(".US","").replace(" US","")
                    syms.append(s)
        else:
            # no header: use csv.reader, take column by index
            rdr = csv.reader(f)
            for row in rdr:
                if not row: 
                    continue
                if col_index < 0 or col_index >= len(row):
                    continue
                s = (row[col_index] or "").strip().upper()
                if s:
                    s = s.replace(".US","").replace(" US","")
                    syms.append(s)

    syms = sorted(set(syms))
    print(f"[INFO] Loaded {len(syms)} symbols from {csv_path}")
    return syms



def export_option_chain(session: Session, symbol: str, outdir: Path):
    if OptionChain is None:
        print('[WARN] SDK missing OptionChain; skip option-chain.')
        return
    try:
        chain = OptionChain.get_chain(session, symbol)
    except Exception as e:
        print(f"[WARN] Option chain fetch failed for {symbol}: {e}")
        return
    ensure_dir(outdir)
    out = outdir / f"option_chain_{symbol}.csv"
    with out.open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['expiration','strike','right','bid','ask','last','iv','delta','gamma','theta','vega'])
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
    print(f"[OK] option_chain_{symbol}.csv")


async def export_candles_once(session: Session, symbol: str, interval: str, max_bars: int, outdir: Path):
    if DXLinkStreamer is None:
        print('[WARN] SDK missing DXLinkStreamer; skip candles.')
        return
    ensure_dir(outdir)
    out = outdir / f"candles_{symbol}_{interval}.csv"
    count = 0
    try:
        async with DXLinkStreamer(session) as s:
            await s.connect()
            if hasattr(s, 'subscribe_candles'):
                stream = await s.subscribe_candles(symbol, interval)
            elif hasattr(s, 'subscribe'):
                stream = await s.subscribe('candles', {'symbol': symbol, 'interval': interval})
            else:
                print('[WARN] No candles subscription method; skip.')
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
        print(f"[OK] candles_{symbol}_{interval}.csv ({count} bars)")
    except Exception as e:
        print(f"[WARN] Candles fetch failed for {symbol}: {e}")


def export_account_filtered(session: Session, account: Account, account_number: str, symbols: List[str], outdir: Path,
                            want_orders: bool, want_transactions: bool):
    symset = set(symbols)

    if want_transactions:
        txns = []
        try:
            end = dt.date.today()
            start = end - dt.timedelta(days=90)
            txns = account.get_transaction_history(session, start_date=start.isoformat(), end_date=end.isoformat())
        except Exception:
            if Transaction is not None:
                txns = Transaction.get_transactions(session, account_number, start_date=start.isoformat(), end_date=end.isoformat())
            else:
                print('[WARN] SDK missing Transaction and account.get_transaction_history(); skip transactions.')
                txns = []
        out = outdir / f"transactions_filtered_{account_number}.csv"
        ensure_dir(outdir)
        with out.open('w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['id','date','type','symbol','quantity','price','amount','description'])
            for t in txns:
                sym = (getattr(t, 'symbol', '') or '').upper()
                if not sym or sym in symset:
                    w.writerow([
                        getattr(t, 'id', ''),
                        getattr(t, 'transaction_date', getattr(t, 'date', '')),
                        getattr(t, 'type', ''),
                        sym,
                        getattr(t, 'quantity', ''),
                        getattr(t, 'price', ''),
                        getattr(t, 'amount', ''),
                        getattr(t, 'description', ''),
                    ])
        print(f"[OK] transactions_filtered_{account_number}.csv")

    if want_orders:
        orders = []
        tried = []
        for meth_name, kwargs in [
            ('get_order_history', {'status': 'all'}),
            ('get_live_orders', {}),
        ]:
            try:
                orders = getattr(account, meth_name)(session, **kwargs)
                break
            except Exception as e:
                tried.append(f"{meth_name} -> {e}")
        if not orders:
            if Order is not None:
                try:
                    orders = Order.get_orders(session, account_number, status='all')
                except Exception as e:
                    tried.append(f"Order.get_orders -> {e}")
            if not orders:
                print('[WARN] No order API path worked; skip orders. Tried: ' + ' | '.join(tried))
                orders = []
        out = outdir / f"orders_filtered_{account_number}.csv"
        ensure_dir(outdir)
        with out.open('w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['id','status','underlying_symbol','asset_type','price_effect','time_in_force','entered_time'])
            for o in orders:
                sym = (getattr(o, 'underlying_symbol', '') or '').upper()
                if not sym or sym in symset:
                    w.writerow([
                        getattr(o, 'id', ''),
                        getattr(o, 'status', ''),
                        sym,
                        getattr(o, 'asset_type', ''),
                        getattr(o, 'price_effect', ''),
                        getattr(o, 'time_in_force', ''),
                        getattr(o, 'entered_time', ''),
                    ])
        print(f"[OK] orders_filtered_{account_number}.csv")


# ------------------------------ CLI ------------------------------
import argparse

def main():
    cfg = load_config()

    ap = argparse.ArgumentParser(description='Batch export by symbols from CSV (SDK-compatible)')
    ap.add_argument('--csv', required=True, help='path to CSV that contains symbols')
    ap.add_argument('--col', default='symbol', help='column name for tickers (default: symbol)')
    ap.add_argument('--outdir', default=cfg['outdir'], help='output directory (default from .env or exports_by_csv)')

    ap.add_argument('--username', default=cfg['username'])
    ap.add_argument('--password', default=cfg['password'])
    ap.add_argument('--mfa', default=cfg['mfa'])
    ap.add_argument('--account', default=cfg['account'])

    ap.add_argument('--what', default='all', choices=['all','candles','options','account'],
                    help='what to export: all / candles / options / account')
    ap.add_argument('--interval', default='1m', help='candle interval: 1m/5m/15m/1h/1d (for candles)')
    ap.add_argument('--max-bars', type=int, default=500, help='max bars per symbol (for candles)')
    ap.add_argument('--sleep', type=float, default=0.5, help='throttle seconds between symbols')
    ap.add_argument('--include-orders', action='store_true', help='when --what account, include orders')
    ap.add_argument('--include-transactions', action='store_true', help='when --what account, include transactions')
    ap.add_argument("--no-header", action="store_true",
                help="CSV has no header; read by column index (default: False)")
    ap.add_argument("--col-index", type=int, default=0,
                help="Column index to read when no header (default: 0)")

    args = ap.parse_args()

    session = login(args.username, args.password, args.mfa)
    account = pick_account(session, args.account)

    outdir = Path(args.outdir); ensure_dir(outdir)

    symbols = read_symbols(Path(args.csv), args.col, args.no_header, args.col_index)

    if args.what in ('candles','all'):
        for sym in symbols:
            asyncio.run(export_candles_once(session, sym, args.interval, args.max_bars, outdir))
            time.sleep(args.sleep)

    if args.what in ('options','all'):
        for sym in symbols:
            export_option_chain(session, sym, outdir)
            time.sleep(args.sleep)

    if args.what in ('account','all'):
        export_account_filtered(session, account, account.account_number, symbols, outdir,
                                want_orders=args.include_orders, want_transactions=args.include_transactions)


if __name__ == '__main__':
    main()
