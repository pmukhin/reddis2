use crate::stored_value::StoredValue;
use anyhow::bail;
use bytes::Bytes;
use std::collections::HashMap;

pub trait HMapSortedSetOps {
    fn zset_add(&mut self, key: &[u8], members: &[(i64, &[u8])]) -> anyhow::Result<usize>;
    fn zset_range(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
        withscores: bool,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>>;
    fn zset_revrange(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
        withscores: bool,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>>;
    fn zset_rank(&self, key: &[u8], member: &[u8]) -> anyhow::Result<Option<usize>>;
    fn zset_revrank(&self, key: &[u8], member: &[u8]) -> anyhow::Result<Option<usize>>;
    fn zset_score(&self, key: &[u8], member: &[u8]) -> anyhow::Result<Option<i64>>;
    fn zset_range_by_score(
        &self,
        key: &[u8],
        min: i64,
        max: i64,
        withscores: bool,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>>;
    fn zset_incr_by(&mut self, key: &[u8], incr: i64, member: &[u8]) -> anyhow::Result<i64>;
}

fn normalize_range(len: usize, start: isize, stop: isize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let real_start = if start < 0 {
        (len as isize + start).max(0) as usize
    } else {
        (start as usize).min(len - 1)
    };
    let real_stop = if stop < 0 {
        (len as isize + stop).max(0) as usize
    } else {
        (stop as usize).min(len - 1)
    };
    if real_start > real_stop {
        return None;
    }
    Some((real_start, real_stop))
}

fn collect_with_scores(
    iter: impl Iterator<Item = (i64, Bytes)>,
    withscores: bool,
) -> (Vec<Bytes>, usize) {
    let mut result = Vec::new();
    for (score, member) in iter {
        result.push(member);
        if withscores {
            result.push(Bytes::from(score.to_string()));
        }
    }
    let len = result.len();
    (result, len)
}

impl HMapSortedSetOps for HashMap<Bytes, StoredValue> {
    fn zset_add(&mut self, key: &[u8], members: &[(i64, &[u8])]) -> anyhow::Result<usize> {
        let stored_value =
            self.entry(Bytes::copy_from_slice(key))
                .or_insert(StoredValue::SortedSet(
                    Default::default(),
                    Default::default(),
                ));
        match stored_value {
            StoredValue::SortedSet(tree, scores) => {
                let mut added = 0;
                for (score, member) in members {
                    let member_bytes = Bytes::copy_from_slice(member);
                    if let Some(old_score) = scores.get(&member_bytes) {
                        tree.remove(&(*old_score, member_bytes.clone()));
                    } else {
                        added += 1;
                    }
                    tree.insert((*score, member_bytes.clone()));
                    scores.insert(member_bytes, *score);
                }
                Ok(added)
            }
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_range(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
        withscores: bool,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::SortedSet(tree, _)) => {
                let Some((real_start, real_stop)) = normalize_range(tree.len(), start, stop) else {
                    return Ok(Some((vec![], 0)));
                };
                let iter = tree
                    .iter()
                    .skip(real_start)
                    .take(real_stop - real_start + 1)
                    .map(|(s, m)| (*s, m.clone()));
                Ok(Some(collect_with_scores(iter, withscores)))
            }
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_revrange(
        &self,
        key: &[u8],
        start: isize,
        stop: isize,
        withscores: bool,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::SortedSet(tree, _)) => {
                let Some((real_start, real_stop)) = normalize_range(tree.len(), start, stop) else {
                    return Ok(Some((vec![], 0)));
                };
                let iter = tree
                    .iter()
                    .rev()
                    .skip(real_start)
                    .take(real_stop - real_start + 1)
                    .map(|(s, m)| (*s, m.clone()));
                Ok(Some(collect_with_scores(iter, withscores)))
            }
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_rank(&self, key: &[u8], member: &[u8]) -> anyhow::Result<Option<usize>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::SortedSet(tree, scores)) => match scores.get(member) {
                None => Ok(None),
                Some(score) => {
                    let target = (*score, Bytes::copy_from_slice(member));
                    Ok(Some(tree.range(..&target).count()))
                }
            },
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_revrank(&self, key: &[u8], member: &[u8]) -> anyhow::Result<Option<usize>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::SortedSet(tree, scores)) => match scores.get(member) {
                None => Ok(None),
                Some(score) => {
                    let target = (*score, Bytes::copy_from_slice(member));
                    let rank = tree.range(..&target).count();
                    Ok(Some(tree.len() - 1 - rank))
                }
            },
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_score(&self, key: &[u8], member: &[u8]) -> anyhow::Result<Option<i64>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::SortedSet(_, scores)) => Ok(scores.get(member).copied()),
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_range_by_score(
        &self,
        key: &[u8],
        min: i64,
        max: i64,
        withscores: bool,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::SortedSet(tree, _)) => {
                let iter = tree
                    .iter()
                    .skip_while(|(s, _)| *s < min)
                    .take_while(|(s, _)| *s <= max)
                    .map(|(s, m)| (*s, m.clone()));
                Ok(Some(collect_with_scores(iter, withscores)))
            }
            _ => bail!("stored value isn't a sorted set"),
        }
    }

    fn zset_incr_by(&mut self, key: &[u8], incr: i64, member: &[u8]) -> anyhow::Result<i64> {
        let stored_value =
            self.entry(Bytes::copy_from_slice(key))
                .or_insert(StoredValue::SortedSet(
                    Default::default(),
                    Default::default(),
                ));
        match stored_value {
            StoredValue::SortedSet(tree, scores) => {
                let member_bytes = Bytes::copy_from_slice(member);
                let old_score = scores.get(&member_bytes).copied().unwrap_or(0);
                let new_score = old_score + incr;
                if scores.contains_key(&member_bytes) {
                    tree.remove(&(old_score, member_bytes.clone()));
                }
                tree.insert((new_score, member_bytes.clone()));
                scores.insert(member_bytes, new_score);
                Ok(new_score)
            }
            _ => bail!("stored value isn't a sorted set"),
        }
    }
}
