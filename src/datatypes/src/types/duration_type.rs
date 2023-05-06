use arrow::datatypes::{
    DataType as ArrowDataType, DurationMicrosecondType as ArrowDurationMicrosecondType,
    DurationMillisecondType as ArrowDurationMillisecondType,
    DurationNanosecondType as ArrowDurationNanosecondType,
    DurationSecondType as ArrowDurationSecondType, TimeUnit as ArrowTimeUnit,
};
use common_time::duration::Duration;
use common_time::timestamp::TimeUnit;
use enum_dispatch::enum_dispatch;
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use super::LogicalPrimitiveType;
use crate::data_type::DataType;
use crate::duration::{
    DurationMicrosecond, DurationMillisecond, DurationNanosecond, DurationSecond,
};
use crate::error;
use crate::error::InvalidDurationPrecisionSnafu;
use crate::prelude::{
    ConcreteDataType, LogicalTypeId, MutableVector, ScalarVectorBuilder, Value, ValueRef, Vector,
};
use crate::vectors::{
    DurationMicrosecondVector, DurationMicrosecondVectorBuilder, DurationMillisecondVector,
    DurationMillisecondVectorBuilder, DurationNanosecondVector, DurationNanosecondVectorBuilder,
    DurationSecondVector, DurationSecondVectorBuilder, PrimitiveVector,
};

const SECOND_VARIATION: u64 = 0;
const MILLISECOND_VARIATION: u64 = 3;
const MICROSECOND_VARIATION: u64 = 6;
const NANOSECOND_VARIATION: u64 = 9;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum DurationType {
    Second(DurationSecondType),
    Millisecond(DurationMillisecondType),
    Microsecond(DurationMicrosecondType),
    Nanosecond(DurationNanosecondType),
}

impl DurationType {
    /// Returns the [`TimeUnit`] of this type.
    pub fn unit(&self) -> TimeUnit {
        match self {
            DurationType::Second(_) => TimeUnit::Second,
            DurationType::Millisecond(_) => TimeUnit::Millisecond,
            DurationType::Microsecond(_) => TimeUnit::Microsecond,
            DurationType::Nanosecond(_) => TimeUnit::Nanosecond,
        }
    }

    pub fn precision(&self) -> u64 {
        match self {
            DurationType::Second(_) => SECOND_VARIATION,
            DurationType::Millisecond(_) => MILLISECOND_VARIATION,
            DurationType::Microsecond(_) => MICROSECOND_VARIATION,
            DurationType::Nanosecond(_) => NANOSECOND_VARIATION,
        }
    }
}

impl TryFrom<u64> for DurationType {
    type Error = error::Error;

    /// Convert fractional duration precision to duration types. Supported precisions are:
    /// - 0: second
    /// - 3: millisecond
    /// - 6: microsecond
    /// - 9: nanosecond
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            SECOND_VARIATION => Ok(DurationType::Second(DurationSecondType::default())),
            MILLISECOND_VARIATION => {
                Ok(DurationType::Millisecond(DurationMillisecondType::default()))
            }
            MICROSECOND_VARIATION => {
                Ok(DurationType::Microsecond(DurationMicrosecondType::default()))
            }
            NANOSECOND_VARIATION => Ok(DurationType::Nanosecond(DurationNanosecondType::default())),
            _ => InvalidDurationPrecisionSnafu { precision: value }.fail(),
        }
    }
}

macro_rules! impl_data_type_for_duration {
    ($unit: ident) => {
        paste! {
            #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
            pub struct [<Duration $unit Type>];

            impl DataType for [<Duration $unit Type>] {
                fn name(&self) -> &str {
                    stringify!([<Duration $unit>])
                }

                fn logical_type_id(&self) -> LogicalTypeId {
                    LogicalTypeId::[<Duration $unit>]
                }

                fn default_value(&self) -> Value {
                    Value::Duration(Duration::new(0, TimeUnit::$unit))
                }

                fn as_arrow_type(&self) -> ArrowDataType {
                    ArrowDataType::Duration(ArrowTimeUnit::$unit)
                }

                fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                    Box::new([<Duration $unit Vector Builder>]::with_capacity(capacity))
                }

                fn is_timestamp_compatible(&self) -> bool {
                    // does duration is timestamp compatible?
                    true
                }
            }

            impl LogicalPrimitiveType for [<Duration $unit Type>] {
                type ArrowPrimitive = [<Arrow Duration $unit Type>];
                type Native = i64;
                type Wrapper = [<Duration $unit>];
                type LargestType = Self;

                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::Duration(DurationType::$unit(
                        [<Duration $unit Type>]::default(),
                    ))
                }

                fn type_name() -> &'static str {
                    stringify!([<Duration $unit Type>])
                }

                fn cast_vector(vector: &dyn Vector) -> crate::Result<&PrimitiveVector<Self>> {
                    vector
                        .as_any()
                        .downcast_ref::<[<Duration $unit Vector>]>()
                        .with_context(|| error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast {} to {}",
                                vector.vector_type_name(), stringify!([<Duration $unit Vector>])
                            ),
                        })
                }

                fn cast_value_ref(value: ValueRef) -> crate::Result<Option<Self::Wrapper>> {
                    match value {
                        ValueRef::Null => Ok(None),
                        ValueRef::Int64(v) =>{
                            Ok(Some([<Duration $unit>]::from(v)))
                        }
                        ValueRef::Duration(t) => match t.unit() {
                            TimeUnit::$unit => Ok(Some([<Duration $unit>](t))),
                            other => error::CastTypeSnafu {
                                msg: format!(
                                    "Failed to cast Duration value with different unit {:?} to {}",
                                    other, stringify!([<Duration $unit>])
                                ),
                            }
                            .fail(),
                        },
                        other => error::CastTypeSnafu {
                            msg: format!("Failed to cast value {:?} to {}", other, stringify!([<Duration $unit>])),
                        }
                        .fail(),
                    }
                }
            }
        }
    }
}

impl_data_type_for_duration!(Nanosecond);
impl_data_type_for_duration!(Second);
impl_data_type_for_duration!(Millisecond);
impl_data_type_for_duration!(Microsecond);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_type_unit() {
        assert_eq!(
            TimeUnit::Second,
            DurationType::Second(DurationSecondType).unit()
        );

        assert_eq!(
            TimeUnit::Millisecond,
            DurationType::Millisecond(DurationMillisecondType).unit()
        );

        assert_eq!(
            TimeUnit::Microsecond,
            DurationType::Microsecond(DurationMicrosecondType).unit()
        );

        assert_eq!(
            TimeUnit::Nanosecond,
            DurationType::Nanosecond(DurationNanosecondType).unit()
        );
    }
}
