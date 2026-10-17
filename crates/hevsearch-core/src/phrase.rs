//! Contiguous phrase predicates with Unicode simple lowercase and whitespace
//! normalization. Punctuation and accents remain significant; this is separate
//! from the full-text tokenizer and never joins text from different rows.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::DataType;
use datafusion::{
    common::ScalarValue,
    logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility},
};

/// Collapse Unicode whitespace, trim it, and apply context-free simple lowercase.
/// Using the first per-character lowercase mapping avoids contextual final sigma
/// and the multi-character dotted-I expansion of full string case conversion.
pub fn normalize_phrase(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    let mut space = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            space = !normalized.is_empty();
            continue;
        }
        if space {
            normalized.push(' ');
            space = false;
        }
        normalized.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    normalized
}

/// Test a contiguous normalized substring. Empty needles never match.
pub fn phrase_matches(text: &str, phrase: &str) -> bool {
    let needle = normalize_phrase(phrase);
    !needle.is_empty() && normalize_phrase(text).contains(&needle)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "phrase_matches",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|v| matches!(v, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let strings = |index: usize| {
                arrays[index]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "phrase_matches requires UTF-8 arguments".into(),
                        )
                    })
            };
            let texts = strings(0)?;
            let phrases = strings(1)?;
            let values: BooleanArray = (0..texts.len())
                .map(|row| {
                    Some(
                        !texts.is_null(row)
                            && !phrases.is_null(row)
                            && phrase_matches(texts.value(row), phrases.value(row)),
                    )
                })
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                    values.value(0),
                ))))
            } else {
                Ok(ColumnarValue::Array(Arc::new(values)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_substring_preserves_punctuation_accents_and_simple_case() {
        for (text, needle, expected) in [
            ("A\u{a0}\tB\nC", " a b ", true),
            ("alpha middle beta", "alpha beta", false),
            ("foobar", "foo", true),
            ("Café, finish", "CAFÉ,", true),
            ("café", "cafe", false),
            ("a,b", "a b", false),
            ("İSTANBUL", "istanbul", true),
            ("ΟΣ", "οσ", true),
            ("ΟΣ", "ος", false),
            ("anything", " \n", false),
        ] {
            assert_eq!(
                phrase_matches(text, needle),
                expected,
                "{text:?} / {needle:?}"
            );
        }
    }
}
