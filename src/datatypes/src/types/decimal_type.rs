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

use arrow_array::types::Decimal128Type as ArrowDecimal128Type;
use arrow_schema::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use super::LogicalPrimitiveType;
use crate::data_type::ConcreteDataType;
use crate::decimal::{Decimal128, DECIMAL128_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION};
use crate::error;
use crate::prelude::{DataType, ScalarVectorBuilder};
use crate::type_id::LogicalTypeId;
use crate::value::{Value, ValueRef};
use crate::vectors::{
    Decimal128Vector, Decimal128VectorBuilder, MutableVector, PrimitiveVector, Vector,
};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Decimal128Type {
    precision: u8,
    scale: i8,
}

impl Decimal128Type {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self { precision, scale }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}

impl DataType for Decimal128Type {
    fn name(&self) -> &str {
        "decimal128"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Decimal128
    }

    fn default_value(&self) -> Value {
        Value::Decimal128(Decimal128::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Decimal128(self.precision, self.scale)
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(Decimal128VectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, _: Value) -> Option<Value> {
        todo!()
    }
}

impl LogicalPrimitiveType for Decimal128Type {
    type ArrowPrimitive = ArrowDecimal128Type;

    type Native = i128;

    type Wrapper = Decimal128;

    type LargestType = Self;

    fn build_data_type() -> ConcreteDataType {
        ConcreteDataType::decimal128_datatype(DECIMAL128_MAX_PRECISION, DECIMAL128_DEFAULT_SCALE)
    }

    fn type_name() -> &'static str {
        "Decimal128"
    }

    fn cast_vector(vector: &dyn Vector) -> crate::Result<&PrimitiveVector<Self>> {
        vector
            .as_any()
            .downcast_ref::<Decimal128Vector>()
            .with_context(|| error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast {} to Decimal128Vector",
                    vector.vector_type_name()
                ),
            })
    }

    fn cast_value_ref(value: ValueRef) -> crate::Result<Option<Self::Wrapper>> {
        match value {
            ValueRef::Null => Ok(None),
            ValueRef::Decimal128(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value {other:?} to Decimal128"),
            }
            .fail(),
        }
    }
}
