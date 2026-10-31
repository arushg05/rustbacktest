use backtest_engine_adapter::{BacktestResult, Fill, Order, OrderSide};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug)]
struct Digest(u64);

pub fn result_digest(result: &BacktestResult) -> String {
    let mut digest = Digest(FNV_OFFSET);
    digest.write_u64(result.orders.len() as u64);
    for order in &result.orders {
        write_order(&mut digest, order);
    }
    digest.write_u64(result.fills.len() as u64);
    for fill in &result.fills {
        write_fill(&mut digest, fill);
    }
    digest.write_f64(result.final_equity);
    format!("{:016x}", digest.0)
}

fn write_order(digest: &mut Digest, order: &Order) {
    digest.write_u64(order.id);
    digest.write_str(order.instrument_id.as_str());
    digest.write_u64(order.ts_init.as_u64());
    digest.write_side(order.side);
    digest.write_f64(order.quantity);
    digest.write_f64(order.target_position);
}

fn write_fill(digest: &mut Digest, fill: &Fill) {
    digest.write_u64(fill.order_id);
    digest.write_str(fill.instrument_id.as_str());
    digest.write_u64(fill.ts_event.as_u64());
    digest.write_u64(fill.ts_init.as_u64());
    digest.write_side(fill.side);
    digest.write_f64(fill.quantity);
    digest.write_f64(fill.price);
    digest.write_f64(fill.position_after);
    digest.write_f64(fill.cash_after);
}

impl Digest {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }

    fn write_f64(&mut self, value: f64) {
        self.write_u64(value.to_bits());
    }

    fn write_str(&mut self, value: &str) {
        self.write_u64(value.len() as u64);
        self.write(value.as_bytes());
    }

    fn write_side(&mut self, side: OrderSide) {
        self.write(&[match side {
            OrderSide::Buy => 1,
            OrderSide::Sell => 2,
        }]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backtest_engine_adapter::{BacktestResult, Fill, Order};
    use backtest_signal::{InstrumentId, UnixNanos};

    fn result() -> BacktestResult {
        BacktestResult {
            orders: vec![Order {
                id: 1,
                instrument_id: id("AAPL.XNAS"),
                ts_init: UnixNanos::new(10),
                side: OrderSide::Buy,
                quantity: 2.0,
                target_position: 2.0,
            }],
            fills: vec![Fill {
                order_id: 1,
                instrument_id: id("AAPL.XNAS"),
                ts_event: UnixNanos::new(11),
                ts_init: UnixNanos::new(11),
                side: OrderSide::Buy,
                quantity: 2.0,
                price: 101.0,
                position_after: 2.0,
                cash_after: 798.0,
            }],
            final_equity: 1_000.0,
        }
    }

    fn id(value: &str) -> InstrumentId {
        InstrumentId::new(value).expect("valid instrument")
    }

    #[test]
    fn identical_results_have_same_digest() {
        assert_eq!(result_digest(&result()), result_digest(&result()));
    }

    #[test]
    fn changed_fill_price_changes_digest() {
        let mut changed = result();
        changed.fills[0].price = 102.0;

        assert_ne!(result_digest(&result()), result_digest(&changed));
    }

    #[test]
    fn changed_order_sequence_changes_digest() {
        let mut changed = result();
        changed.orders.push(Order {
            id: 2,
            instrument_id: id("MSFT.XNAS"),
            ts_init: UnixNanos::new(12),
            side: OrderSide::Sell,
            quantity: 1.0,
            target_position: -1.0,
        });
        changed.orders.swap(0, 1);

        assert_ne!(result_digest(&result()), result_digest(&changed));
    }

    #[test]
    fn changed_final_equity_changes_digest() {
        let mut changed = result();
        changed.final_equity = 1_001.0;

        assert_ne!(result_digest(&result()), result_digest(&changed));
    }
}
