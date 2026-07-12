#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Binary,
    Utf8,
    Integer,
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
            _ => None,
        }
    }
}
