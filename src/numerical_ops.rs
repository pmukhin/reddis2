use crate::stored_value::StoredValue;
use anyhow::bail;
use bytes::Bytes;
use std::collections::HashMap;

pub trait HMapNumericalOps {
    fn incr_by(&mut self, key: &[u8], incr_by: i64) -> anyhow::Result<Option<Bytes>>;
}

impl HMapNumericalOps for HashMap<Bytes, StoredValue> {
    fn incr_by(&mut self, key: &[u8], incr_by: i64) -> anyhow::Result<Option<Bytes>> {
        match self.get(key) {
            None => Ok(None),
            Some(StoredValue::Plain(bytes)) => {
                let str = String::from_utf8_lossy(bytes);

                match str::parse::<i64>(&str) {
                    Ok(num) => {
                        let new_value = (num + incr_by).to_string();
                        let value_as_bytes = Bytes::copy_from_slice(new_value.as_bytes());
                        self.insert(
                            Bytes::copy_from_slice(key),
                            StoredValue::Plain(value_as_bytes.clone()),
                        );
                        Ok(Some(value_as_bytes))
                    }
                    Err(_) => bail!("stored value isn't a 64 bit integer"),
                }
            }
            _ => bail!("stored value isn't a 64 bit integer"),
        }
    }
}
