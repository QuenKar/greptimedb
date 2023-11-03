// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

use rust_decimal::Decimal as RustDecimal;
use serde::{Deserialize, Serialize};

/// The maximum precision for [Decimal128] values
pub const DECIMAL128_MAX_PRECISION: u8 = 38;

/// The maximum scale for [Decimal128] values
pub const DECIMAL128_MAX_SCALE: i8 = 38;

/// The default scale for [Decimal128] values
pub const DECIMAL128_DEFAULT_SCALE: i8 = 10;

/// 128bit decimal, using the i128 to represent the decimal.
///
/// **precision**: the total number of digits in the number, it's range is \[1, 38\].
///
/// **scale**: the number of digits to the right of the decimal point, it's range is \[0, precision\].
///
/// Decimal value, it's range is \[-10^precision + 1 / 10^scale, 10^precision - 1 / 10^scale\].
///
/// For example, if precision is 10 and scale is 2,
/// decimal range will be \[-9999999.99, 9999999.99\]
#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct Decimal128 {
    value: i128,
    precision: u8,
    scale: i8,
}

impl Decimal128 {
    pub fn new(value: i128, precision: u8, scale: i8) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }

    pub fn val(&self) -> i128 {
        self.value
    }

    /// Returns the precision of this decimal.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the scale of this decimal.
    pub fn scale(&self) -> i8 {
        self.scale
    }

    /// Returns the decimal zero
    pub fn zero() -> Self {
        Self {
            value: 0,
            precision: 0,
            scale: 0,
        }
    }

    /// Convert to ScalarValue representation
    pub fn to_scalar_value(&self) -> (Option<i128>, u8, i8) {
        (Some(self.value), self.precision, self.scale)
    }

    pub fn to_pb_value(&self) -> (i64, i64, i32) {
        (
            (self.value >> 64) as i64,
            self.value as i64,
            ((self.precision as i32) << 8) | (self.scale as i32),
        )
    }
}

impl PartialEq for Decimal128 {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Eq for Decimal128 {}

impl PartialOrd for Decimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal128 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl FromStr for Decimal128 {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rd = RustDecimal::from_str(s)?;
        Ok(Self::from(rd))
    }
}

impl Display for Decimal128 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            format_decimal_str(&self.value.to_string(), self.precision as usize, self.scale)
        )
    }
}

impl Hash for Decimal128 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_i128(self.value);
        state.write_u8(self.precision);
        state.write_i8(self.scale);
    }
}

impl From<Decimal128> for serde_json::Value {
    fn from(decimal: Decimal128) -> Self {
        serde_json::Value::String(decimal.to_string())
    }
}

impl From<Decimal128> for i128 {
    fn from(decimal: Decimal128) -> Self {
        decimal.val()
    }
}

impl From<i128> for Decimal128 {
    fn from(value: i128) -> Self {
        Self::new(value, DECIMAL128_MAX_PRECISION, DECIMAL128_DEFAULT_SCALE)
    }
}

/// Convert from RustDecimal to Decimal128
/// RustDecimal can represent the range is smaller than Decimal128,
/// so it is safe to convert RustDecimal to Decimal128
impl From<RustDecimal> for Decimal128 {
    fn from(rd: RustDecimal) -> Self {
        let s = rd.to_string();
        let precision = (s.len() - s.matches(&['.', '-'][..]).count()) as u8;
        Self {
            value: rd.mantissa(),
            precision,
            scale: rd.scale() as i8,
        }
    }
}

/// Port from arrow-rs,
/// see https://github.com/Apache/arrow-rs/blob/master/arrow-array/src/types.rs#L1323-L1344
fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
    let (sign, rest) = match value_str.strip_prefix('-') {
        Some(stripped) => ("-", stripped),
        None => ("", value_str),
    };

    let bound = precision.min(rest.len()) + sign.len();
    let value_str = &value_str[0..bound];

    if scale == 0 {
        value_str.to_string()
    } else if scale < 0 {
        let padding = value_str.len() + scale.unsigned_abs() as usize;
        format!("{value_str:0<padding$}")
    } else if rest.len() > scale as usize {
        // Decimal separator is in the middle of the string
        let (whole, decimal) = value_str.split_at(value_str.len() - scale as usize);
        format!("{whole}.{decimal}")
    } else {
        // String has to be padded
        format!("{}0.{:0>width$}", sign, rest, width = scale as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_decimal() {
        let decimal = Decimal128::new(123456789, 9, 0);
        assert_eq!(decimal.to_string(), "123456789");

        let decimal = Decimal128::new(123456789, 9, 2);
        assert_eq!(decimal.to_string(), "1234567.89");

        let decimal = Decimal128::new(123, 3, -2);
        assert_eq!(decimal.to_string(), "12300");
    }

    #[test]
    fn test_from_rust_decimal() {
        let rd = RustDecimal::new(123456789, 9);
        let decimal = Decimal128::from(rd);
        assert_eq!(decimal.to_string(), "0.123456789");

        // Max RustDecimal
        let rd = RustDecimal::MAX;
        let decimal = Decimal128::from(rd);
        assert_eq!(decimal.to_string(), "79228162514264337593543950335");

        // Min RustDecimal
        let rd = RustDecimal::MIN;
        let decimal = Decimal128::from(rd);
        assert_eq!(decimal.to_string(), "-79228162514264337593543950335");
    }
}
