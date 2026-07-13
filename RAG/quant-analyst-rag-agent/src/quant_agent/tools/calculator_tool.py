from __future__ import annotations

def spread(left: float, right: float) -> dict[str, float]: return {"left": left, "right": right, "difference": left - right}
def percent(value: float) -> str: return f"{value * 100:.1f}%"
