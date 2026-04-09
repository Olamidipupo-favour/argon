use dashmap::DashMap;

pub struct Deduper {
    seen: DashMap<String, u64>,
    window_ms: u64,
}

impl Deduper {
    pub fn new(window_ms: u64) -> Self {
        Self {
            seen: DashMap::new(),
            window_ms,
        }
    }

    pub fn check_and_update(&self, key: &str, now_ms: u64) -> bool {
        if let Some(entry) = self.seen.get(key) {
            if now_ms.saturating_sub(*entry) <= self.window_ms {
                return true;
            }
        }

        self.seen.insert(key.to_string(), now_ms);
        false
    }
}
