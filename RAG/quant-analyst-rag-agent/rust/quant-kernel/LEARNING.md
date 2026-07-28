# Rust 第一阶段速成

这个 crate 用当前量化项目的领域问题练习 Rust，不接 Python，不做性能优化。

## 只需要记住的心智模型

1. 每个值都有一个所有者。
2. 一个值同一时刻可以有多个只读借用，或者一个可写借用。
3. 所有者离开作用域，值自动释放。
4. `Option<T>` 表示值可能缺失。
5. `Result<T, E>` 表示操作可能失败。
6. `struct` 组合数据，`enum` 穷举状态，`impl` 定义行为。

## 运行

```bash
cd rust/quant-kernel
cargo run
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

第一次 `cargo run` 会执行 `src/main.rs`。核心实现和测试都在 `src/lib.rs`。

## 读代码的顺序

1. `PriceSeries`：`struct`、所有权、`Vec`。
2. `PriceSeries::new`：构造函数、`Result` 和 `?`。
3. `ticker`、`closes`：借用和切片。
4. `simple_returns`：迭代器、闭包、`Option`。
5. `moving_average`：可变变量、循环、错误处理。
6. `MarketRegime`：`enum` 和模式匹配。
7. `tests`：Rust 原生测试。

## 三个必须亲手完成的练习

### 练习 1：观察所有权

取消 `src/main.rs` 中下面这行的注释：

```rust
println!("{closes:?}");
```

把它放在 `PriceSeries::new("DEMO", closes)?` 之后。运行 `cargo check`，阅读编译器错误。
然后把传参改成 `closes.clone()`，再次运行，理解显式复制的成本。

### 练习 2：实现滚动最高价

在 `src/lib.rs` 中增加：

```rust
pub fn rolling_max(values: &[f64], window: usize)
    -> Result<Vec<Option<f64>>, QuantError>
```

先用容易理解的窗口切片实现，不要求最优性能。

输入：

```text
values = [10, 12, 11, 15], window = 3
```

预期：

```text
[None, None, Some(12), Some(15)]
```

### 练习 3：用 enum 消灭字符串状态

增加：

```rust
pub enum Trend {
    Up,
    Flat,
    Down,
}
```

然后实现：

```rust
pub fn classify_trend(prices: &[f64]) -> Result<Trend, QuantError>
```

不要返回 `"up"`、`"flat"`、`"down"` 字符串。

## 第一阶段完成标准

- 能解释“移动”和“借用”的区别；
- 能读懂 `&[f64]`、`Vec<f64>`、`Option<f64>` 和 `Result<T, E>`；
- 能为一个滚动计算编写函数和测试；
- 能通过 `cargo test`、`cargo clippy` 和 `cargo fmt --check`；
- 暂时不需要理解生命周期标注、宏、异步、trait object 和 `unsafe`。
