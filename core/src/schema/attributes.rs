use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AttributeKey(pub String);

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AttributeValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
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

    pub fn iter(&self) -> impl Iterator<Item = (&AttributeKey, &AttributeValue)> {
        self.inner.iter()
    }

    pub fn get_text<K: AsRef<str>>(&self, key: K) -> Option<String> {
        match self.inner.get(&AttributeKey(key.as_ref().to_owned())) {
            Some(AttributeValue::Text(value)) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn get_float<K: AsRef<str>>(&self, key: K) -> Option<f64> {
        match self.inner.get(&AttributeKey(key.as_ref().to_owned())) {
            Some(AttributeValue::Float(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn get_int<K: AsRef<str>>(&self, key: K) -> Option<i64> {
        match self.inner.get(&AttributeKey(key.as_ref().to_owned())) {
            Some(AttributeValue::Integer(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn get_bool<K: AsRef<str>>(&self, key: K) -> Option<bool> {
        match self.inner.get(&AttributeKey(key.as_ref().to_owned())) {
            Some(AttributeValue::Boolean(value)) => Some(*value),
            _ => None,
        }
    }
}
