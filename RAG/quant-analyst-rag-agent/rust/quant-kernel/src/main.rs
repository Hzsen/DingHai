use quant_kernel::{PriceSeries, classify_return};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let closes = vec![100.0, 102.0, 101.0, 106.0, 110.0];

    // `closes` 的所有权在这里移动进 `PriceSeries`。
    let series = PriceSeries::new("DEMO", closes)?;
    // 第一项练习：取消下一行注释，再运行 `cargo check`。
     println!("moved closes: {closes:?}");

    // 这些方法只借用 `series`，所以可以连续调用。
    println!("ticker: {}", series.ticker());
    println!("closes: {:?}", series.closes());
    println!("returns: {:?}", series.simple_returns());
    println!("ma3: {:?}", series.moving_average(3)?);
    println!("regime: {:?}", classify_return(series.total_return()));

    Ok(())
}
