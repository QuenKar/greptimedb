use crate::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
};
use crate::vectors::{PrimitiveVector, PrimitiveVectorBuilder};

pub type DurationSecondVector = PrimitiveVector<DurationSecondType>;
pub type DurationSecondVectorBuilder = PrimitiveVectorBuilder<DurationSecondType>;

pub type DurationMillisecondVector = PrimitiveVector<DurationMillisecondType>;
pub type DurationMillisecondVectorBuilder = PrimitiveVectorBuilder<DurationMillisecondType>;

pub type DurationMicrosecondVector = PrimitiveVector<DurationMicrosecondType>;
pub type DurationMicrosecondVectorBuilder = PrimitiveVectorBuilder<DurationMicrosecondType>;

pub type DurationNanosecondVector = PrimitiveVector<DurationNanosecondType>;
pub type DurationNanosecondVectorBuilder = PrimitiveVectorBuilder<DurationNanosecondType>;
