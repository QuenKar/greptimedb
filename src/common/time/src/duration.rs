use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, ParseDurationSnafu};
use crate::timestamp::TimeUnit;

/// Duration is a type that represents a time duration.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Duration {
    value: i64,
    unit: TimeUnit,
}

impl Duration {
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }

    pub fn new_second(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Second,
        }
    }

    pub fn new_millisecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    pub fn new_microsecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Microsecond,
        }
    }

    pub fn new_nanosecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Nanosecond,
        }
    }

    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    /// Format `Duration` to string,
    /// such as 1s, 10ms, 100us, 10000ns.
    pub fn to_string(&self) -> String {
        format!("{}{}", self.value, self.unit.short_name())
    }

    /// Split a [Duration] into seconds part and nanoseconds part.
    /// Notice the seconds part of split result is always rounded down to floor.
    fn split(&self) -> (i64, u32) {
        let sec_mul = (TimeUnit::Second.factor() / self.unit.factor()) as i64;
        let nsec_mul = (self.unit.factor() / TimeUnit::Nanosecond.factor()) as i64;

        let sec_div = self.value.div_euclid(sec_mul);
        let sec_mod = self.value.rem_euclid(sec_mul);
        // safety:  the max possible value of `sec_mod` is 999,999,999
        let nsec = u32::try_from(sec_mod * nsec_mul).unwrap();
        (sec_div, nsec)
    }

    /// Convert current Duration to different TimeUnit
    fn convert_to(&self, unit: TimeUnit) -> Self {
        let (sec, nsec) = self.split();
        let value = match unit {
            TimeUnit::Second => sec,
            TimeUnit::Millisecond => {
                sec * TimeUnit::Second.factor() as i64 + i64::from(nsec) / 1_000_000
            }
            TimeUnit::Microsecond => {
                sec * TimeUnit::Second.factor() as i64 + i64::from(nsec) / 1_000
            }
            TimeUnit::Nanosecond => sec * TimeUnit::Second.factor() as i64 + i64::from(nsec),
        };
        Self::new(value, unit)
    }
}

// convert "123s", "1ms", "1000us" , "1200000ns", "-123ms" String into Duration Type.
impl FromStr for Duration {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut num_chars = String::new();
        let mut duration_unit = String::new();

        // tell is_negative
        let mut is_negative = false;
        let mut s = s;
        if s.starts_with('-') {
            is_negative = true;
            s = &s[1..];
        }

        //get number and unit
        for c in s.chars() {
            if c.is_digit(10) {
                num_chars.push(c);
            } else {
                duration_unit.push(c);
            }
        }

        // parse number
        let num = match num_chars.parse::<i64>() {
            Ok(n) => {
                if is_negative {
                    Ok(-n)
                } else {
                    Ok(n)
                }
            }
            Err(_) => ParseDurationSnafu { raw: s }.fail(),
        };

        // convert to Duration
        let duration = match duration_unit.as_str() {
            "s" => Ok(Duration {
                value: num.unwrap(),
                unit: TimeUnit::Second,
            }),
            "ms" => Ok(Duration {
                value: num.unwrap(),
                unit: TimeUnit::Millisecond,
            }),
            "us" => Ok(Duration {
                value: num.unwrap(),
                unit: TimeUnit::Microsecond,
            }),
            "ns" => Ok(Duration {
                value: num.unwrap(),
                unit: TimeUnit::Nanosecond,
            }),
            _ => ParseDurationSnafu { raw: s }.fail(),
        };

        duration
    }
}

// from i64 into Duration Type.
// Default TimeUnit is Millisecond.
impl From<i64> for Duration {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

// i64: Default TimeUnit is Millisecond.
impl From<Duration> for i64 {
    fn from(d: Duration) -> Self {
        d.convert_to(TimeUnit::Millisecond).value
    }
}

impl From<Duration> for serde_json::Value {
    fn from(d: Duration) -> Self {
        serde_json::Value::String(d.to_string())
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Duration is ordable.
impl Ord for Duration {
    fn cmp(&self, other: &Self) -> Ordering {
        // fast path: most comparisons use the same unit.
        if self.unit == other.unit {
            return self.value.cmp(&other.value);
        }

        let (s_sec, s_nsec) = self.split();
        let (o_sec, o_nsec) = other.split();
        match s_sec.cmp(&o_sec) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => s_nsec.cmp(&o_nsec),
        }
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, self.unit.short_name())
    }
}

impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Duration {}

impl Hash for Duration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (sec, nsec) = self.split();
        state.write_i64(sec);
        state.write_u32(nsec);
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::timestamp::TimeUnit;
    use crate::Duration;

    #[test]
    pub fn test_cmp_duration() {
        let t = Duration::new(1, TimeUnit::Millisecond);
        assert_eq!(TimeUnit::Millisecond, t.unit());
        assert_eq!(1, t.value());
        assert_eq!(Duration::new(1000, TimeUnit::Microsecond), t);
        assert!(t > Duration::new(999, TimeUnit::Microsecond));
        assert!(t < Duration::new(1, TimeUnit::Second))
    }

    #[test]
    pub fn test_str_to_duration() {
        let t = Duration::from_str("1ms").unwrap();
        assert_eq!(TimeUnit::Millisecond, t.unit());
        assert_eq!(1, t.value());

        let t = Duration::from_str("123s").unwrap();
        assert_eq!(TimeUnit::Second, t.unit());
        assert_eq!(123, t.value());

        let t = Duration::from_str("1000us").unwrap();
        assert_eq!(TimeUnit::Microsecond, t.unit());
        assert_eq!(1000, t.value());

        let t = Duration::from_str("1200000ns").unwrap();
        assert_eq!(TimeUnit::Nanosecond, t.unit());
        assert_eq!(1200000, t.value());
    }

    #[test]
    pub fn test_from_i64() {
        let t = Duration::from(1);
        assert_eq!(TimeUnit::Millisecond, t.unit());
        assert_eq!(1, t.value());
    }

    #[test]
    pub fn test_duration_to_i64() {
        let t = Duration::from(1);
        assert_eq!(1, i64::from(t));
    }

    #[test]
    pub fn test_hash() {
        let t = Duration::from(1);
        let t2 = Duration::from(1);
        let mut map = std::collections::HashMap::new();
        map.insert(t, 1);
        assert_eq!(1, *map.get(&t2).unwrap());
    }

    #[test]
    pub fn test_negative_str() {
        let t = Duration::from_str("-1ns").unwrap();
        assert_eq!(TimeUnit::Nanosecond, t.unit());
        assert_eq!(-1, t.value());
    }
}
