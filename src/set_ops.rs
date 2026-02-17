use crate::stored_value::StoredValue;
use anyhow::bail;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

pub trait HMapSetOps {
    fn set_add(&mut self, key: &[u8], members: Vec<&[u8]>) -> anyhow::Result<usize>;
    fn set_is_member(&self, key: &[u8], member: &[u8]) -> anyhow::Result<bool>;
    fn set_inter(&self, keys: &[&[u8]]) -> anyhow::Result<(Vec<&Bytes>, usize)>;
    fn set_union(&self, keys: &[&[u8]]) -> anyhow::Result<(Vec<&Bytes>, usize)>;
    fn set_diff(&self, keys: &[&[u8]]) -> anyhow::Result<(Vec<&Bytes>, usize)>;
    fn set_card(&self, key: &[u8]) -> anyhow::Result<Option<usize>>;
    fn set_members(&self, key: &[u8]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>>;
}

impl HMapSetOps for HashMap<Bytes, StoredValue> {
    fn set_add(&mut self, key: &[u8], members: Vec<&[u8]>) -> anyhow::Result<usize> {
        let stored_value = self
            .entry(Bytes::copy_from_slice(key))
            .or_insert(StoredValue::Set(Default::default()));
        match stored_value {
            StoredValue::Set(set) => {
                let mut added = 0;
                for member in members {
                    if set.insert(Bytes::copy_from_slice(member)) {
                        added += 1;
                    }
                }
                Ok(added)
            }
            _ => bail!("stored value isn't a set"),
        }
    }

    fn set_is_member(&self, key: &[u8], member: &[u8]) -> anyhow::Result<bool> {
        match self.get(key) {
            None => Ok(false),
            Some(StoredValue::Set(set)) => Ok(set.contains(member)),
            _ => bail!("stored value isn't a set"),
        }
    }

    fn set_inter(&self, keys: &[&[u8]]) -> anyhow::Result<(Vec<&Bytes>, usize)> {
        let mut iter = keys.iter();
        let first_key = match iter.next() {
            None => return Ok((vec![], 0)),
            Some(key) => key,
        };

        let mut result: HashSet<&Bytes> = match self.get(*first_key) {
            None => return Ok((vec![], 0)),
            Some(StoredValue::Set(set)) => set.iter().collect(),
            _ => bail!("stored value isn't a set"),
        };

        for key in iter {
            match self.get(*key) {
                None => return Ok((vec![], 0)),
                Some(StoredValue::Set(set)) => {
                    result.retain(|member| set.contains(*member));
                }
                _ => bail!("stored value isn't a set"),
            }
        }

        let len = result.len();
        Ok((result.into_iter().collect(), len))
    }

    fn set_union(&self, keys: &[&[u8]]) -> anyhow::Result<(Vec<&Bytes>, usize)> {
        let mut result: HashSet<&Bytes> = HashSet::new();

        for key in keys {
            match self.get(*key) {
                None => {}
                Some(StoredValue::Set(set)) => {
                    result.extend(set.iter());
                }
                _ => bail!("stored value isn't a set"),
            }
        }

        let len = result.len();
        Ok((result.into_iter().collect(), len))
    }

    fn set_diff(&self, keys: &[&[u8]]) -> anyhow::Result<(Vec<&Bytes>, usize)> {
        let mut iter = keys.iter();
        let first_key = match iter.next() {
            None => return Ok((vec![], 0)),
            Some(key) => key,
        };

        let mut result: HashSet<&Bytes> = match self.get(*first_key) {
            None => return Ok((vec![], 0)),
            Some(StoredValue::Set(set)) => set.iter().collect(),
            _ => bail!("stored value isn't a set"),
        };

        for key in iter {
            match self.get(*key) {
                None => {}
                Some(StoredValue::Set(set)) => {
                    result.retain(|member| !set.contains(*member));
                }
                _ => bail!("stored value isn't a set"),
            }
        }

        let len = result.len();
        Ok((result.into_iter().collect(), len))
    }

    fn set_card(&self, key: &[u8]) -> anyhow::Result<Option<usize>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Set(set)) => Ok(Some(set.len())),
            _ => bail!("stored value isn't a set"),
        }
    }

    fn set_members(&self, key: &[u8]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Set(set)) => {
                let members: Vec<&Bytes> = set.iter().collect();
                let len = members.len();
                Ok(Some((members, len)))
            }
            _ => bail!("stored value isn't a set"),
        }
    }
}
