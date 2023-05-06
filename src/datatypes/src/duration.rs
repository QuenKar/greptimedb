use common_time::duration::Duration;
use common_time::timestamp::TimeUnit;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::prelude::{Scalar, Value, ValueRef};
use crate::scalars::ScalarRef;
use crate::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    WrapperType,
};
use crate::vectors::{
    DurationMicrosecondVector, DurationMillisecondVector, DurationNanosecondVector,
    DurationSecondVector,
};

macro_rules! define_duration_with_unit {
    ($unit: ident) => {
        paste! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
            pub struct [<Duration $unit>](pub Duration);

            impl [<Duration $unit>] {
                pub fn new(val: i64) -> Self {
                    Self(Duration::new(val, TimeUnit::$unit))
                }
            }

            impl Default for [<Duration $unit>] {
                fn default() -> Self {
                    Self::new(0)
                }
            }

            impl From<[<Duration $unit>]> for Value {
                fn from(t: [<Duration $unit>]) -> Value {
                    Value::Duration(t.0)
                }
            }

            impl From<[<Duration $unit>]> for serde_json::Value {
                fn from(t: [<Duration $unit>]) -> Self {
                    t.0.into()
                }
            }

            impl From<[<Duration $unit>]> for ValueRef<'static> {
                fn from(t: [<Duration $unit>]) -> Self {
                    ValueRef::Duration(t.0)
                }
            }

            impl Scalar for [<Duration $unit>] {
                type VectorType = [<Duration $unit Vector>];
                type RefType<'a> = [<Duration $unit>];

                fn as_scalar_ref(&self) -> Self::RefType<'_> {
                    *self
                }

                fn upcast_gat<'short, 'long: 'short>(
                    long: Self::RefType<'long>,
                ) -> Self::RefType<'short> {
                    long
                }
            }

            impl<'a> ScalarRef<'a> for [<Duration $unit>] {
                type ScalarType = [<Duration $unit>];

                fn to_owned_scalar(&self) -> Self::ScalarType {
                    *self
                }
            }

            impl WrapperType for [<Duration $unit>] {
                type LogicalType = [<Duration $unit Type>];
                type Native = i64;

                fn from_native(value: Self::Native) -> Self {
                    Self::new(value)
                }

                fn into_native(self) -> Self::Native {
                    self.0.into()
                }
            }

            impl From<i64> for [<Duration $unit>] {
                fn from(val: i64) -> Self {
                    [<Duration $unit>]::from_native(val)
                }
            }

            impl From<[<Duration $unit>]> for i64{
                fn from(val: [<Duration $unit>]) -> Self {
                    val.0.value()
                }
            }
        }
    };
}

define_duration_with_unit!(Second);
define_duration_with_unit!(Millisecond);
define_duration_with_unit!(Microsecond);
define_duration_with_unit!(Nanosecond);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_scalar() {
        let d = DurationSecond::new(456);
        assert_eq!(d, d.as_scalar_ref());
        assert_eq!(d, d.to_owned_scalar());
        let d = DurationMillisecond::new(456);
        assert_eq!(d, d.as_scalar_ref());
        assert_eq!(d, d.to_owned_scalar());
        let d = DurationMicrosecond::new(456);
        assert_eq!(d, d.as_scalar_ref());
        assert_eq!(d, d.to_owned_scalar());
        let d = DurationNanosecond::new(456);
        assert_eq!(d, d.as_scalar_ref());
        assert_eq!(d, d.to_owned_scalar());
    }
}
