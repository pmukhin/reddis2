use crate::stored_value::StoredValue;
use anyhow::bail;
use bytes::Bytes;
use std::collections::HashMap;

pub trait HMapDictOps {
    fn dict_get(&self, key: &[u8], field: &[u8]) -> anyhow::Result<Option<&Bytes>>;
    fn dict_set(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> anyhow::Result<()>;
    fn dict_mget(&self, key: &[u8], fields: &[&[u8]]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>>;
    fn dict_mset(&mut self, key: &[u8], fields_and_values: &[&[u8]]) -> anyhow::Result<()>;
    fn dict_get_all(&self, key: &[u8]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>>;
    fn dict_incr_by(&mut self, key: &[u8], field: &[u8], incr_by: i64) -> anyhow::Result<Bytes>;
    fn dict_exists(&self, key: &[u8], field: &[u8]) -> anyhow::Result<bool>;
    fn dict_keys(&self, key: &[u8]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>>;
}

impl HMapDictOps for HashMap<Bytes, StoredValue> {
    fn dict_get(&self, key: &[u8], field: &[u8]) -> anyhow::Result<Option<&Bytes>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Dict(dict)) => Ok(dict.get(field)),
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_set(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let stored_value = self
            .entry(Bytes::copy_from_slice(key))
            .or_insert(StoredValue::Dict(Default::default()));
        match stored_value {
            StoredValue::Dict(dict) => {
                dict.insert(
                    Bytes::copy_from_slice(field),
                    Bytes::copy_from_slice(value),
                );
                Ok(())
            }
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_mget(&self, key: &[u8], fields: &[&[u8]]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Dict(dict)) => {
                let values = fields.iter().filter_map(|f| dict.get(*f)).collect();
                Ok(Some((values, dict.keys().len())))
            }
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_mset(&mut self, key: &[u8], fields_and_values: &[&[u8]]) -> anyhow::Result<()> {
        let stored_value = self
            .entry(Bytes::copy_from_slice(key))
            .or_insert(StoredValue::Dict(Default::default()));
        match stored_value {
            StoredValue::Dict(dict) => {
                for chunk in fields_and_values.chunks(2) {
                    dict.insert(
                        Bytes::copy_from_slice(chunk[0]),
                        Bytes::copy_from_slice(chunk[1]),
                    );
                }
                Ok(())
            }
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_get_all(&self, key: &[u8]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Dict(dict)) => {
                let values = dict.iter().flat_map(|(k, v)| [k, v]).collect();
                Ok(Some((values, dict.keys().len())))
            }
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_incr_by(&mut self, key: &[u8], field: &[u8], incr_by: i64) -> anyhow::Result<Bytes> {
        let stored_value = self
            .entry(Bytes::copy_from_slice(key))
            .or_insert(StoredValue::Dict(Default::default()));
        match stored_value {
            StoredValue::Dict(dict) => {
                let field_key = Bytes::copy_from_slice(field);
                let current = match dict.get(&field_key) {
                    None => 0,
                    Some(bytes) => {
                        let s = String::from_utf8_lossy(bytes);
                        s.parse::<i64>()
                            .map_err(|_| anyhow::anyhow!("hash value is not an integer"))?
                    }
                };
                let new_value = (current + incr_by).to_string();
                let value_bytes = Bytes::copy_from_slice(new_value.as_bytes());
                dict.insert(field_key, value_bytes.clone());
                Ok(value_bytes)
            }
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_exists(&self, key: &[u8], field: &[u8]) -> anyhow::Result<bool> {
        match self.get(key) {
            None => Ok(false),
            Some(StoredValue::Dict(dict)) => Ok(dict.contains_key(field)),
            _ => bail!("stored value isn't a dict"),
        }
    }

    fn dict_keys(&self, key: &[u8]) -> anyhow::Result<Option<(Vec<&Bytes>, usize)>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Dict(dict)) => {
                let keys: Vec<&Bytes> = dict.keys().collect();
                let len = keys.len();
                Ok(Some((keys, len)))
            }
            _ => bail!("stored value isn't a dict"),
        }
    }
}
