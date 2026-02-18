use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttributeKey(pub String);

#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, Default)]
pub struct Attributes {
    inner: HashMap<AttributeKey, AttributeValue>,
}

impl Attributes {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn insert<K: Into<String>>(
        &mut self,
        key: K,
        value: AttributeValue,
    ) -> Option<AttributeValue> {
        let key = AttributeKey(key.into());
        self.inner.insert(key, value)
    }

    pub fn get<K: AsRef<str>>(&self, key: K) -> Option<&AttributeValue> {
        let key = AttributeKey(key.as_ref().to_owned());
        self.inner.get(&key)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
