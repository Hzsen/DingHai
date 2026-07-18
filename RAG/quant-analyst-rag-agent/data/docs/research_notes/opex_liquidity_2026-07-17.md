# 2026-07-17 OPEX Liquidity Research

> Research state: `PRE_US_CASH_OPEN`  
> Point-in-time cutoff: `2026-07-17T10:10:00Z`  
> Scope: U.S. monthly options expiration, system liquidity, cross-asset absorption and AI/semiconductor positioning  
> This is a research-state update, not investment advice or a price forecast.

## Conclusion

The evidence does not support calling 17 July a system-wide liquidity shortage. It supports a more specific state:

```text
SYSTEM_LIQUIDITY_EXPANDING
+ FUNDING_COST_STILL_RESTRICTIVE
+ SELECTIVE_AI_SEMICONDUCTOR_DELEVERAGING
+ OPEX_POSITIONING_TRANSITION
```

Dollar liquidity supplied by the Fed/TGA/RRP complex has expanded over the last 20 days, credit remains contained and the VIX curve remains in contango. However, that liquidity is being absorbed unevenly: regional banks/credit are strong, broad U.S. large caps are mixed, while AI/semiconductors are rejecting liquidity. The global chip selloff was already active before the U.S. cash session, so OPEX is more plausibly a timing, pinning and hedge-unwind mechanism than the fundamental cause of the selloff.

The highest-probability microstructure interpretation is conditional. If the expiring monthly book is net positive gamma, dealer hedging can dampen moves or pin spot during parts of the session. When that gamma expires, the stabilizing inventory can disappear, leaving the post-OPEX market more sensitive to the underlying AI-valuation and funding-cost shock. This says something about potential volatility transmission, not the direction of the next move.

## Deterministic Macro Evidence

The local macro pipeline refreshed 26,967 point-in-time observations from FRED, Cboe and AkShare/Sina with zero source errors. Values below use information available by `2026-07-17T10:10:00Z`; U.S. cash-market price observations are therefore through the 16 July close.

### System liquidity sources

| Component | 20D contribution | Latest observation | Interpretation |
|---|---:|---:|---|
| Fed balance sheet | +7.4bn USD | 2026-07-15 | modest injection |
| Treasury General Account | +162.5bn USD | 2026-07-15 | dominant injection through TGA drawdown |
| Overnight reverse repo | +6.3bn USD | 2026-07-16 | modest release |
| Net source impulse | **+176.2bn USD** | point-in-time model | strongly expanding |

This is a plumbing measure, not proof that every asset receives inflows.

### Funding price and risk transmission

| Series | Latest | 5D change | 20D change | Reading |
|---|---:|---:|---:|---|
| 10Y real yield | 2.32% | 0bp | +14bp | long-duration discount rate remains high |
| 10Y nominal yield | 4.55% | -1bp | +17bp | funding price still restrictive |
| 30Y nominal yield | 5.08% | +2bp | +21bp | long-end duration pressure persists |
| Corporate spread proxy | 0.79% | +2bp | +2bp | contained; no broad credit accident |
| VIX | 16.73 | +1.70 | -1.68 | risk demand increased during the week but is not crisis-level |
| VIX3M | 19.50 | +0.93 | -0.63 | VIX/VIX3M = 0.858, still contango |

The joint signal is `liquidity available but duration capital expensive`, not `cash unavailable`.

### Where liquidity is being absorbed

| Target proxy | 5D return | 20D return | Absorption score | Model state |
|---|---:|---:|---:|---|
| KRE / banks-credit | +3.87% | +3.66% | +50.6 | ABSORBING |
| SPY / U.S. large cap | -0.56% | +2.98% | +16.7 | MIXED |
| IWM / small cap | -0.14% | -1.41% | +1.0 | MIXED |
| QQQ / AI-duration proxy | -2.70% | -0.08% | -16.9 | MIXED, weakening |
| SOXX / semiconductors | -8.75% | -10.08% | relative confirmation negative | REJECTING within AI complex |
| TLT / long Treasury | -0.31% | -3.61% | -41.4 | REJECTING |
| GLD / gold | -3.20% | -2.32% | -31.2 | REJECTING |

The large KRE-versus-SOXX divergence argues for rotation and selective deleveraging. It does not look like an indiscriminate liquidation in which banks, credit, broad equities and volatility all break together.

## What OPEX Can and Cannot Explain

Cboe's 2026 calendar identifies 17 July as the standard monthly expiration. The session has two relevant clocks: standard AM-settled index contracts are tied to the opening settlement process, while equity/ETF and PM-settled contracts continue to create expiry and closing-flow effects later in the day.

OPEX can affect liquidity through:

1. `gamma hedging`: dealer delta adjustments as spot moves;
2. `strike pinning`: long-gamma hedging sells rallies and buys dips near concentrated strikes;
3. `short-gamma amplification`: hedging follows the market move when dealers are short gamma;
4. `expiry unwind`: hedges attached to expired inventory are removed or rolled;
5. `closing concentration`: equity/ETF option exercise, assignment and benchmark-related flows can raise close volume without proving net buying or selling.

OPEX cannot, by itself, establish dealer gamma sign or predict market direction. Cboe explicitly notes that gross option volume is not equal to net risk because customer buys and sells can offset. Exact dealer inventory requires trade-direction and opening/closing classification that public observers generally do not possess.

## Positioning Proxies and Reliability

Two external model pages estimated positive aggregate/monthly SPX gamma before expiration, but their magnitudes are not directly comparable and neither is audited dealer inventory:

- Options Analysis Suite reported positive modeled SPX net gamma as of 14 July under a standard dealer-position convention.
- Modigin's expiry breakdown showed a large positive modeled monthly 17 July bucket and a negative same-day weekly bucket before the open.

These observations support a **possible positive-gamma pin followed by gamma removal** hypothesis, but reliability is `UNVERIFIED/MODEL_DERIVED`. They must not be promoted to measured fund flow. The mixed monthly-versus-weekly signs also imply that intraday behavior may change between the opening settlement, the cash session and the close.

## Premarket Event Context

Contemporaneous market reporting showed Nasdaq-100 futures down about 2%, S&P 500 futures down about 1% and a semiconductor ETF proxy down roughly 4.4% before the U.S. cash open as the global chip selloff deepened. Because the move was already visible across Asia and U.S. futures, OPEX cannot be the sole causal explanation.

The causal ordering supported by current evidence is:

```text
AI valuation / Capex return concern
+ expensive long-duration funding
+ crowded semiconductor positioning unwind
→ global chip selling
→ OPEX gamma and expiry flows alter intraday transmission
```

## Validation State Machine

### OPEX_PINNING_CONFIRMED

Require most of:

- spot repeatedly reverts toward high-open-interest strikes;
- realized range compresses despite heavy option/closing volume;
- VIX/VIX3M remains below 1;
- credit spread and KRE remain stable;
- SOXX relative weakness stops worsening into the close.

Interpretation: positive-gamma inventory is temporarily supplying market-making liquidity.

### OPEX_AMPLIFICATION_CONFIRMED

Require most of:

- directional spot move accelerates rather than mean-reverts;
- VIX rises faster than VIX3M or the curve moves toward backwardation;
- SOXX/QQQ underperformance broadens into SPY, IWM and credit;
- downside breadth and closing sell imbalance expand together.

Interpretation: short-gamma or hedge-unwind flows are amplifying an existing shock.

### POST_OPEX_GAMMA_REMOVAL

Test on the next session rather than infer on Friday:

- Friday is pinned/compressed but Monday realized volatility expands;
- major expiry strikes lose their magnet effect;
- the move follows the pre-existing AI/funding-cost signal after expiry inventory disappears.

Interpretation: OPEX had masked or delayed the underlying move; it did not create it.

### OPEX_NARRATIVE_INVALIDATED

- no abnormal closing concentration;
- VIX term structure, breadth and relative returns do not change around expiry;
- post-OPEX behavior is indistinguishable from the prior sector trend.

Interpretation: the calendar event added little explanatory value.

## Research Status and Next Update

Current research status:

```text
SYSTEMIC_LIQUIDITY_STRESS: NOT_CONFIRMED
SELECTIVE_AI_SEMICONDUCTOR_LIQUIDITY_WITHDRAWAL: CONFIRMED
OPEX_POSITIVE_GAMMA_PIN: PLAUSIBLE, NOT_CONFIRMED
OPEX_DIRECTIONAL_SIGNAL: UNKNOWN
POST_OPEX_VOLATILITY_RELEASE: WATCH
```

Required post-close evidence:

- actual SPX/QQQ/SOXX session return and intraday range;
- total volume versus 20-day average and last-hour share;
- advance/decline breadth and semiconductor relative return;
- closing auction imbalance;
- VIX/VIX3M close;
- option volume and open-interest changes by expiry/strike;
- 20 July follow-through after the expiring gamma inventory is gone.

## Sources

- Local point-in-time macro database: `data/processed/phase1_research.db`, refreshed 2026-07-17T10:10:00Z.
- Cboe 2026 Options Expiration Calendar: https://cdn.cboe.com/resources/options/Cboe2026OPTIONSCalendar.pdf
- Cboe, Volatility Insights: Much Ado About 0DTEs: https://www.cboe.com/insights/posts/volatility-insights-evaluating-the-market-impact-of-spx-0-dte-options
- Cboe, State of the Options Industry Q1 2026: https://www.cboe.com/insights/posts/the-state-of-the-options-industry-q-1-2026
- Cboe VIX products, 16 July 2026 close: https://www.cboe.com/tradable-products/vix
- SpotGamma July catalyst calendar: https://spotgamma.com/julys-setup-rotation-and-repositioning/
- Options Analysis Suite modeled SPX GEX snapshot: https://www.optionsanalysissuite.com/index/spx/gamma-exposure
- Modigin modeled SPX expiry GEX: https://modigin.com/gex/spx
- Bloomberg market report syndicated by Swissinfo, 17 July 2026: https://www.swissinfo.ch/eng/nasdaq-100-futures-tumble-2%25-as-chip-rout-deepens%3A-markets-wrap/91759205
