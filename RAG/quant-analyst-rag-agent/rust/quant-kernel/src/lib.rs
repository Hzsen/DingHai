//! 第一阶段：用量化问题学习 Rust 的核心语法。
//!
//! 这还是一个纯 Rust crate，暂时不接 Python，也不追求极致性能。

use std::error::Error;
use std::fmt;

/// `struct` 把一组有业务含义的数据组织在一起。
///
/// `String` 和 `Vec<f64>` 由 `PriceSeries` 拥有；当它离开作用域时，
/// Rust 会自动释放它们，不需要垃圾回收器。
#[derive(Debug, Clone, PartialEq)]
pub struct PriceSeries {
    ticker: String,
    closes: Vec<f64>,
}

/// `enum` 表示“只能是这些状态之一”。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    RiskOn,
    Neutral,
    RiskOff,
}

/// `Result<T, QuantError>` 中失败一侧的明确错误类型。
#[derive(Debug, Clone, PartialEq)]
pub enum QuantError {
    EmptySeries,
    InvalidWindow { window: usize, observations: usize },
    InvalidPrice { index: usize, value: f64 },
}

impl fmt::Display for QuantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySeries => write!(formatter, "price series cannot be empty"),
            Self::InvalidWindow {
                window,
                observations,
            } => write!(
                formatter,
                "window {window} exceeds the {observations} available observations"
            ),
            Self::InvalidPrice { index, value } => {
                write!(formatter, "price at index {index} is invalid: {value}")
            }
        }
    }
}

impl Error for QuantError {}

impl PriceSeries {
    /// 构造函数取得 `closes` 的所有权，并在入口处保证数据合法。
    pub fn new(ticker: impl Into<String>, closes: Vec<f64>) -> Result<Self, QuantError> {
        validate_prices(&closes)?;
        Ok(Self {
            ticker: ticker.into(),
            closes,
        })
    }

    /// `&self` 是只读借用：调用者仍然拥有这个对象。
    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    /// 返回切片而不是复制整个 Vec。
    pub fn closes(&self) -> &[f64] {
        &self.closes
    }

    pub fn simple_returns(&self) -> Vec<Option<f64>> {
        simple_returns(&self.closes)
    }

    pub fn moving_average(&self, window: usize) -> Result<Vec<Option<f64>>, QuantError> {
        moving_average(&self.closes, window)
    }

    pub fn total_return(&self) -> f64 {
        let first = self.closes[0];
        let last = self.closes[self.closes.len() - 1];
        last / first - 1.0
    }
}

fn validate_prices(prices: &[f64]) -> Result<(), QuantError> {
    if prices.is_empty() {
        return Err(QuantError::EmptySeries);
    }
    for (index, value) in prices.iter().copied().enumerate() {
        if !value.is_finite() || value <= 0.0 {
            return Err(QuantError::InvalidPrice { index, value });
        }
    }
    Ok(())
}

/// `&[f64]` 表示借用任意连续浮点数据，不要求调用者交出所有权。
///
/// 第一个位置没有前一日价格，所以用 `Option::None` 表达缺失；
/// 其余位置用 `Option::Some(value)`，不使用一个含义模糊的魔法数。
pub fn simple_returns(prices: &[f64]) -> Vec<Option<f64>> {
    let mut output = Vec::with_capacity(prices.len());
    if prices.is_empty() {
        return output;
    }
    output.push(None);
    output.extend(prices.windows(2).map(|pair| Some(pair[1] / pair[0] - 1.0)));
    output
}

/// O(n) 滚动均线。
///
/// 窗口尚未形成时返回 `None`。形成后维护滚动和，避免每一行重新求和。
pub fn moving_average(values: &[f64], window: usize) -> Result<Vec<Option<f64>>, QuantError> {
    if window == 0 || window > values.len() {
        return Err(QuantError::InvalidWindow {
            window,
            observations: values.len(),
        });
    }

    let mut output = vec![None; values.len()];
    let mut rolling_sum = 0.0;

    for (index, value) in values.iter().copied().enumerate() {
        rolling_sum += value;
        if index >= window {
            rolling_sum -= values[index - window];
        }
        if index + 1 >= window {
            output[index] = Some(rolling_sum / window as f64);
        }
    }
    Ok(output)
}

pub fn classify_return(total_return: f64) -> MarketRegime {
    if total_return >= 0.05 {
        MarketRegime::RiskOn
    } else if total_return <= -0.05 {
        MarketRegime::RiskOff
    } else {
        MarketRegime::Neutral
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-12;

    fn assert_close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < EPSILON);
    }

    #[test]
    fn price_series_owns_data_but_methods_only_borrow_it() {
        let closes = vec![100.0, 105.0, 110.0];
        let series = PriceSeries::new("000300.SH", closes).unwrap();

        assert_eq!(series.ticker(), "000300.SH");
        assert_eq!(series.closes(), &[100.0, 105.0, 110.0]);
        assert_close(series.total_return(), 0.10);
    }

    #[test]
    fn calculates_simple_returns_with_explicit_missing_value() {
        let returns = simple_returns(&[100.0, 105.0, 99.75]);

        assert_eq!(returns[0], None);
        assert_close(returns[1].unwrap(), 0.05);
        assert_close(returns[2].unwrap(), -0.05);
    }

    #[test]
    fn calculates_rolling_average() {
        let averages = moving_average(&[10.0, 20.0, 30.0, 40.0], 3).unwrap();

        assert_eq!(averages[0], None);
        assert_eq!(averages[1], None);
        assert_eq!(averages[2], Some(20.0));
        assert_eq!(averages[3], Some(30.0));
    }

    #[test]
    fn rejects_invalid_inputs_instead_of_silently_calculating() {
        let invalid_price = PriceSeries::new("BAD", vec![10.0, f64::NAN]).unwrap_err();
        let invalid_window = moving_average(&[10.0, 11.0], 3).unwrap_err();

        assert!(matches!(
            invalid_price,
            QuantError::InvalidPrice { index: 1, .. }
        ));
        assert_eq!(
            invalid_window,
            QuantError::InvalidWindow {
                window: 3,
                observations: 2
            }
        );
    }

    #[test]
    fn classifies_all_regime_variants() {
        assert_eq!(classify_return(0.08), MarketRegime::RiskOn);
        assert_eq!(classify_return(0.01), MarketRegime::Neutral);
        assert_eq!(classify_return(-0.08), MarketRegime::RiskOff);
    }
}
