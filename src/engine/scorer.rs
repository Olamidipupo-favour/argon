pub fn compute_confidence(
    sources_count: usize,
    size_usd: f64,
    multiple_addresses: bool,
    onchain_confirmed: bool,
    only_rest: bool,
    lag_detected: bool,
) -> f64 {
    let mut score = 0.5;

    if sources_count >= 2 {
        score += 0.2;
    }
    if size_usd > 500.0 {
        score += 0.15;
    }
    if multiple_addresses {
        score += 0.1;
    }
    if lag_detected {
        score += 0.05;
    }
    if onchain_confirmed {
        score += 0.05;
    }
    if only_rest {
        score -= 0.2;
    }

    score.clamp(0.0, 1.0)
}
