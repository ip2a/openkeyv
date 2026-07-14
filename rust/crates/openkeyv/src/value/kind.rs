#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Binary,
    Utf8,
    Integer,
    UnsignedInteger,
    Float,
    Bool,
    Null,
    Structured,
}

impl ValueKind {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Binary => 0,
            Self::Utf8 => 1,
            Self::Integer => 2,
            Self::Float => 3,
            Self::Bool => 4,
            Self::Null => 5,
            Self::Structured => 6,
            Self::UnsignedInteger => 7,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Binary),
            1 => Some(Self::Utf8),
            2 => Some(Self::Integer),
            3 => Some(Self::Float),
            4 => Some(Self::Bool),
            5 => Some(Self::Null),
            6 => Some(Self::Structured),
            7 => Some(Self::UnsignedInteger),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_kind_tags_are_stable() {
        let kinds = [
            (ValueKind::Binary, 0),
            (ValueKind::Utf8, 1),
            (ValueKind::Integer, 2),
            (ValueKind::Float, 3),
            (ValueKind::Bool, 4),
            (ValueKind::Null, 5),
            (ValueKind::Structured, 6),
            (ValueKind::UnsignedInteger, 7),
        ];

        for (kind, tag) in kinds {
            assert_eq!(kind.tag(), tag);
            assert_eq!(ValueKind::from_tag(tag), Some(kind));
        }
        assert_eq!(ValueKind::from_tag(8), None);
    }
}
