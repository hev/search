//! The engine's FTS analyzer (RFC 0001): alyze `word_v4`.
//!
//! One analyzer, owned here, runs on **both** sides of the FTS index —
//! the write path derives the reserved `text_tok` column from `text`,
//! and the query path analyzes the query string before it is handed to
//! LanceDB. The load-bearing invariant is that the two sides stay
//! byte-for-byte identical: LanceDB is reduced to a passthrough
//! whitespace splitter over `text_tok` (all built-in filters off), so
//! any drift between write- and query-side analysis silently tanks
//! recall. Keep every configuration knob in this module.
//!
//! The configuration is the `word_v4` profile — UAX #29 word
//! segmentation + lowercasing, no stemming, no stop-word removal, no
//! ASCII folding — matching Turbopuffer's own BM25 analyzer, which is
//! the dialect Layer fronts this engine with (the parity thesis of
//! RFC 0001). The 39-byte token bound mirrors the gateway's query
//! tokenizer policy (`layer-gateway/src/routes/hybrid_text.rs`).

use std::cell::RefCell;

use alyze::analyze::{AnalysisOptions, Analyzer, ReusableBuffer, TokenizerOptions};

/// Maximum token length in bytes; longer tokens are dropped by the
/// analyzer on both paths. Mirrors the gateway's `word_v4` policy.
const MAX_TOKEN_LENGTH_BYTES: usize = 39;

/// Identifier for the analyzer configuration, written into the table's
/// schema metadata when the FTS index is built (RFC 0001's manifest
/// capture) so a rebuild can't silently desync from writes. Changing
/// the analyzer configuration or the pinned alyze version must change
/// this string, and changing this string means existing `text_tok`
/// columns are stale until reindexed.
pub const ANALYZER_ID: &str = "alyze=0.1.5:word_v4";

thread_local! {
    // `ReusableBuffer` preallocates a 32k-capacity stemming cache;
    // reusing it per worker thread avoids paying that per call.
    static ANALYZE_BUFFER: RefCell<ReusableBuffer> = RefCell::new(ReusableBuffer::new());
}

fn word_v4() -> Analyzer {
    Analyzer::new(AnalysisOptions {
        tokenizer: TokenizerOptions::UAX29Word(Default::default()),
        maximum_token_length: Some(MAX_TOKEN_LENGTH_BYTES),
        case_sensitive: false,
        stopword_removal: None,
        stemming: None,
        ascii_folding: false,
    })
}

/// Analyze a string into its `word_v4` token stream, in input order,
/// duplicates retained (term frequency matters for BM25).
pub fn analyze(input: &str) -> Vec<String> {
    let analyzer = word_v4();
    let mut tokens = Vec::new();
    ANALYZE_BUFFER.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        buffer.reset_keep_stemming_cache();
        analyzer.analyze(input, &mut buffer, |token| {
            tokens.push(token.text.to_string());
            true
        });
    });
    tokens
}

/// Analyze a string and space-join the tokens — the indexed surface of
/// the `text_tok` column, and the query string handed to LanceDB's
/// passthrough whitespace tokenizer. Returns `None` when analysis
/// yields no tokens (empty, whitespace, or punctuation-only input) so
/// callers keep `text_tok` null instead of storing an empty string.
pub fn tokenized(input: &str) -> Option<String> {
    let tokens = analyze(input);
    if tokens.is_empty() {
        None
    } else {
        Some(tokens.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed-corpus reference outputs for the `word_v4` profile. If
    /// this test breaks after an alyze bump, the analyzer config — and
    /// therefore the index format — changed: bump [`ANALYZER_ID`] and
    /// plan a reindex, don't just update the expectations.
    #[test]
    fn word_v4_reference_outputs() {
        assert_eq!(analyze("Hello, world!"), vec!["hello", "world"]);
        // No stemming: surface forms are preserved.
        assert_eq!(analyze("running runs ran"), vec!["running", "runs", "ran"]);
        // No stop-word removal.
        assert_eq!(analyze("To the Moon"), vec!["to", "the", "moon"]);
        // No ASCII folding: diacritics are preserved (lowercased).
        assert_eq!(analyze("Crème Brûlée"), vec!["crème", "brûlée"]);
        // UAX #29 keeps interior punctuation like apostrophes/periods
        // inside word tokens where the spec says so.
        assert_eq!(analyze("don't"), vec!["don't"]);
        // Numbers are word-like; pure punctuation is dropped.
        assert_eq!(analyze("k8s v1.2 --- !!!"), vec!["k8s", "v1.2"]);
        // Duplicates retained: BM25 term frequency depends on them.
        assert_eq!(analyze("the cat the hat"), vec!["the", "cat", "the", "hat"]);
        // Token-length bound: a >39-byte token is dropped.
        let long = "x".repeat(40);
        assert_eq!(analyze(&format!("short {long}")), vec!["short"]);
    }

    #[test]
    fn tokenized_joins_and_nulls() {
        assert_eq!(
            tokenized("The Lord of the Rings").as_deref(),
            Some("the lord of the rings")
        );
        assert_eq!(tokenized(""), None);
        assert_eq!(tokenized("   ,,, !!"), None);
    }

    /// Write↔query lockstep: both paths go through the same function,
    /// so analyzing an already-analyzed string must be a fixed point —
    /// the property that keeps the passthrough index consistent.
    #[test]
    fn analysis_is_idempotent_on_token_surface() {
        for input in [
            "The Lord of the Rings",
            "Harry Potter and the Philosopher's Stone",
            "Kubernetes networking deep-dive (2nd ed.)",
            "crème brûlée à la mode",
        ] {
            let once = tokenized(input).unwrap();
            let twice = tokenized(&once).unwrap();
            assert_eq!(once, twice, "analyzer not idempotent for {input:?}");
        }
    }
}
