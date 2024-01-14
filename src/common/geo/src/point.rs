use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Point(geo::Point<f64>);

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self(geo::Point::new(x, y))
    }

    pub fn x(&self) -> f64 {
        self.0.x()
    }

    pub fn y(&self) -> f64 {
        self.0.y()
    }
}

impl Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (x, y) = (self.x(), self.y());
        write!(f, "Point({}, {})", x, y)
    }
}
