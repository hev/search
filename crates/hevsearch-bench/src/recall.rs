//! Recall harness primitives (RFC 0011).
//!
//! - `.fvecs` / `.ivecs` / `.bvecs` readers for the SIFT/GIST/Deep
//!   on-disk format: little-endian `[dim: i32][dim elements]` records.
//! - Exact brute-force L2 nearest neighbours, used both for the tiny
//!   self-test fixture and for synthetic-dataset ground truth.
//! - `recall@k` and `ndcg@k` scored against exact-NN ground truth
//!   (binary relevance: a hit is relevant iff it is one of the true
//!   k nearest neighbours).
//! - A seeded deterministic vector generator so a synthetic run is
//!   reproducible: same seed → same dataset → same ground truth.

use std::io::Read;
use std::path::Path;

use anyhow::{bail, Context};

/// Read an `.fvecs` file: repeated `[dim: i32 LE][dim f32 LE]`.
/// `limit` caps the number of vectors read (`None` = all).
pub fn read_fvecs(path: &Path, limit: Option<usize>) -> anyhow::Result<Vec<Vec<f32>>> {
    let data = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    parse_fvecs(&data, limit).with_context(|| format!("parsing {}", path.display()))
}

/// Read an `.ivecs` file (same framing, i32 payload — the
/// ground-truth neighbour-id format).
pub fn read_ivecs(path: &Path, limit: Option<usize>) -> anyhow::Result<Vec<Vec<u32>>> {
    let data = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    parse_ivecs(&data, limit).with_context(|| format!("parsing {}", path.display()))
}

/// Read a `.bvecs` file (`[dim: i32 LE][dim u8]`), widening to f32.
pub fn read_bvecs(path: &Path, limit: Option<usize>) -> anyhow::Result<Vec<Vec<f32>>> {
    let data = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut cursor = std::io::Cursor::new(&data);
    let mut out = Vec::new();
    loop {
        if let Some(limit) = limit {
            if out.len() >= limit {
                break;
            }
        }
        let mut dim_buf = [0u8; 4];
        match cursor.read_exact(&mut dim_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let dim = i32::from_le_bytes(dim_buf);
        if dim <= 0 {
            bail!("non-positive dimension {dim} in bvecs record {}", out.len());
        }
        let mut payload = vec![0u8; dim as usize];
        cursor.read_exact(&mut payload)?;
        out.push(payload.into_iter().map(f32::from).collect());
    }
    Ok(out)
}

fn parse_fvecs(data: &[u8], limit: Option<usize>) -> anyhow::Result<Vec<Vec<f32>>> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 4 <= data.len() {
        if let Some(limit) = limit {
            if out.len() >= limit {
                break;
            }
        }
        let dim = i32::from_le_bytes(data[off..off + 4].try_into().unwrap());
        if dim <= 0 {
            bail!("non-positive dimension {dim} in record {}", out.len());
        }
        let dim = dim as usize;
        off += 4;
        let bytes = dim * 4;
        if off + bytes > data.len() {
            bail!("truncated record {} (dim={dim})", out.len());
        }
        let v: Vec<f32> = data[off..off + bytes]
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        off += bytes;
        out.push(v);
    }
    Ok(out)
}

fn parse_ivecs(data: &[u8], limit: Option<usize>) -> anyhow::Result<Vec<Vec<u32>>> {
    // ivecs shares the fvecs framing with an i32 payload; reuse the
    // parser through a bit-preserving reinterpretation.
    let floats = parse_fvecs(data, limit)?;
    Ok(floats
        .into_iter()
        .map(|v| v.into_iter().map(|f| f.to_bits()).collect())
        .collect())
}

/// Squared L2 distance (monotonic with L2; enough for ranking).
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Exact k nearest neighbours of `query` among `base` under L2,
/// returned as base-array indices, nearest first. O(N·dim) per query
/// — the ground-truth oracle, not a search path.
pub fn brute_force_knn(base: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u32> {
    let mut scored: Vec<(f32, u32)> = base
        .iter()
        .enumerate()
        .map(|(i, v)| (l2_squared(v, query), i as u32))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, i)| i).collect()
}

/// `recall@k`: |retrieved@k ∩ true-NN@k| / k. Ground truth must hold
/// at least `k` ids.
pub fn recall_at_k(ground_truth: &[u32], retrieved: &[u64], k: usize) -> f64 {
    assert!(
        ground_truth.len() >= k,
        "ground truth holds {} ids, need {k}",
        ground_truth.len()
    );
    let truth: std::collections::HashSet<u64> =
        ground_truth[..k].iter().map(|&i| i as u64).collect();
    let hits = retrieved
        .iter()
        .take(k)
        .filter(|id| truth.contains(id))
        .count();
    hits as f64 / k as f64
}

/// `ndcg@k` with binary relevance against the true k nearest
/// neighbours: gain 1 iff the retrieved id is in the exact top-k.
pub fn ndcg_at_k(ground_truth: &[u32], retrieved: &[u64], k: usize) -> f64 {
    assert!(
        ground_truth.len() >= k,
        "ground truth holds {} ids, need {k}",
        ground_truth.len()
    );
    let truth: std::collections::HashSet<u64> =
        ground_truth[..k].iter().map(|&i| i as u64).collect();
    let dcg: f64 = retrieved
        .iter()
        .take(k)
        .enumerate()
        .filter(|(_, id)| truth.contains(id))
        .map(|(rank, _)| 1.0 / ((rank as f64 + 2.0).log2()))
        .sum();
    let ideal: f64 = (0..k.min(truth.len()))
        .map(|rank| 1.0 / ((rank as f64 + 2.0).log2()))
        .sum();
    if ideal == 0.0 {
        0.0
    } else {
        dcg / ideal
    }
}

/// Aggregate mean of a per-query metric.
pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Seeded deterministic vector generator (splitmix64-driven) for the
/// synthetic dataset mode: same (seed, n, dim) → identical vectors on
/// every run and platform, so synthetic ground truth is reproducible.
pub fn synthetic_vectors(seed: u64, n: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut state = seed;
    let mut next = move || {
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    };
    (0..n)
        .map(|_| {
            (0..dim)
                // map to [-1, 1) with 24 bits of mantissa
                .map(|_| ((next() >> 40) as f32) / (1u64 << 23) as f32 - 1.0)
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fvecs_round_trip() {
        // two records: dim=2 [1.0, 2.0], dim=2 [3.0, 4.0]
        let mut data = Vec::new();
        for rec in [[1.0f32, 2.0], [3.0, 4.0]] {
            data.extend_from_slice(&2i32.to_le_bytes());
            for f in rec {
                data.extend_from_slice(&f.to_le_bytes());
            }
        }
        let vecs = parse_fvecs(&data, None).unwrap();
        assert_eq!(vecs, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
        assert_eq!(parse_fvecs(&data, Some(1)).unwrap().len(), 1);
    }

    #[test]
    fn ivecs_round_trip() {
        let mut data = Vec::new();
        data.extend_from_slice(&3i32.to_le_bytes());
        for id in [7i32, 0, 42] {
            data.extend_from_slice(&id.to_le_bytes());
        }
        let ids = parse_ivecs(&data, None).unwrap();
        assert_eq!(ids, vec![vec![7, 0, 42]]);
    }

    #[test]
    fn truncated_fvecs_rejected() {
        let mut data = Vec::new();
        data.extend_from_slice(&4i32.to_le_bytes());
        data.extend_from_slice(&1.0f32.to_le_bytes()); // 1 of 4 floats
        assert!(parse_fvecs(&data, None).is_err());
    }

    /// The scorer self-test the RFC requires: on a tiny fixture with
    /// inline brute-force ground truth, a perfect retrieval scores
    /// recall@k == 1.0 and a deliberately lossy one scores < 1.0.
    #[test]
    fn recall_perfect_and_lossy_on_fixture() {
        let base = synthetic_vectors(42, 200, 8);
        let queries = synthetic_vectors(7, 10, 8);
        for q in &queries {
            let truth = brute_force_knn(&base, q, 10);
            let perfect: Vec<u64> = truth.iter().map(|&i| i as u64).collect();
            assert_eq!(recall_at_k(&truth, &perfect, 10), 1.0);
            assert_eq!(ndcg_at_k(&truth, &perfect, 10), 1.0);
            // lossy: drop the top hit, shift everything up, pad junk
            let mut lossy = perfect[1..].to_vec();
            lossy.push(u64::MAX);
            assert!(recall_at_k(&truth, &lossy, 10) < 1.0);
            assert!(ndcg_at_k(&truth, &lossy, 10) < 1.0);
        }
    }

    #[test]
    fn ndcg_rewards_rank_order() {
        let truth: Vec<u32> = (0..10).collect();
        let in_order: Vec<u64> = (0..10).collect();
        let reversed: Vec<u64> = (0..10).rev().collect();
        // both are recall 1.0, ndcg identical (binary relevance,
        // full overlap) — but a partial overlap at the top beats the
        // same overlap at the bottom.
        assert_eq!(recall_at_k(&truth, &reversed, 10), 1.0);
        assert_eq!(ndcg_at_k(&truth, &in_order, 10), 1.0);
        let top_hit: Vec<u64> = vec![0, 99, 98, 97, 96, 95, 94, 93, 92, 91];
        let bottom_hit: Vec<u64> = vec![99, 98, 97, 96, 95, 94, 93, 92, 91, 0];
        assert!(ndcg_at_k(&truth, &top_hit, 10) > ndcg_at_k(&truth, &bottom_hit, 10));
    }

    #[test]
    fn synthetic_generator_is_deterministic() {
        let a = synthetic_vectors(1234, 50, 16);
        let b = synthetic_vectors(1234, 50, 16);
        assert_eq!(a, b);
        let c = synthetic_vectors(1235, 50, 16);
        assert_ne!(a, c);
        // and ground truth derived from it is stable too
        assert_eq!(brute_force_knn(&a, &a[0], 5), brute_force_knn(&b, &b[0], 5));
    }
}
