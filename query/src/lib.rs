use std::marker::PhantomData;

pub trait BinaryExpression<T> {
    fn evaluate(&self, t: &T) -> bool;
    fn evaluate_bin(&self, start: &T, end: &T) -> bool;
}

pub trait UnaryExpression<T> {
    fn evaluate(&self, t: &T) -> T;
}

/**
 * boolean expressions
 */

pub enum BooleanOp {
    And,
    Or,
}

pub struct BooleanExpression<T> {
    operands: Vec<Box<BinaryExpression<T>>>,
    boolean_op: BooleanOp,
}

impl<T> BooleanExpression<T> {
    pub fn new(operands: Vec<Box<BinaryExpression<T>>>,
            boolean_op: BooleanOp) -> BooleanExpression<T> {
        BooleanExpression {
            operands: operands,
            boolean_op: boolean_op,
        }
    }

    pub fn operand_count(&self) -> usize {
        self.operands.len()
    }
}

impl<T> BinaryExpression<T> for BooleanExpression<T> {
    fn evaluate(&self, t: &T) -> bool {
        match &self.boolean_op {
            BooleanOp::And => {
                for operand in self.operands.iter() {
                    if !operand.evaluate(t) {
                        return false;
                    }
                }

                true
            },
            BooleanOp::Or =>  {
                for operand in self.operands.iter() {
                    if operand.evaluate(t) {
                        return true;
                    }
                }

                false
            },
        }
    }

    fn evaluate_bin(&self, start: &T, end: &T) -> bool {
        match &self.boolean_op {
            BooleanOp::And => {
                for operand in self.operands.iter() {
                    if !operand.evaluate_bin(start, end) {
                        return false;
                    }
                }

                true
            },
            BooleanOp::Or =>  {
                for operand in self.operands.iter() {
                    if operand.evaluate_bin(start, end) {
                        return true;
                    }
                }

                false
            },
        }
    }
}

/**
 * compare expressions
 */

pub enum CompareOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
}

pub struct CompareExpression<T: PartialEq + PartialOrd> {
    left_op: Box<UnaryExpression<T>>,
    right_op: Box<UnaryExpression<T>>,
    compare_op: CompareOp,
}

impl<T: PartialEq + PartialOrd> CompareExpression<T> {
    pub fn new(left_op: Box<UnaryExpression<T>>,
            right_op: Box<UnaryExpression<T>>,
            compare_op: CompareOp) -> CompareExpression<T> {
        CompareExpression {
            left_op: left_op,
            right_op: right_op,
            compare_op: compare_op,
        }
    }
}

impl<T: PartialEq + PartialOrd> BinaryExpression<T>
        for CompareExpression<T> {
    fn evaluate(&self, t: &T) -> bool {
        match &self.compare_op {
            CompareOp::Equal => 
                self.left_op.evaluate(t) == self.right_op.evaluate(t),
            CompareOp::NotEqual => 
                self.left_op.evaluate(t) != self.right_op.evaluate(t),
            CompareOp::GreaterThan => 
                self.left_op.evaluate(t) > self.right_op.evaluate(t),
            CompareOp::GreaterThanOrEqualTo => 
                self.left_op.evaluate(t) >= self.right_op.evaluate(t),
            CompareOp::LessThan => 
                self.left_op.evaluate(t) < self.right_op.evaluate(t),
            CompareOp::LessThanOrEqualTo => 
                self.left_op.evaluate(t) <= self.right_op.evaluate(t),
        }
    }

    fn evaluate_bin(&self, start: &T, end: &T) -> bool {
        match &self.compare_op {
            CompareOp::Equal | CompareOp::NotEqual =>  unimplemented!(),
            CompareOp::GreaterThan 
                    | CompareOp::GreaterThanOrEqualTo
                    | CompareOp::LessThan 
                    | CompareOp::LessThanOrEqualTo => 
                self.evaluate(start) || self.evaluate(end),
        }
    }
}

/**
 * constant expression
 */

pub struct ConstantExpression<T> {
    t: T,
}

impl<T> ConstantExpression<T> {
    pub fn new(t: T) -> ConstantExpression<T> {
        ConstantExpression {
            t: t,
        }
    }
}

impl<T: PartialEq + PartialOrd + Clone> UnaryExpression<T>
        for ConstantExpression<T> {
    fn evaluate(&self, _t: &T) -> T {
        self.t.clone()
    }
}

/**
 * evaluate expression
 */

pub struct EvaluateExpression<T> {
    phantom: PhantomData<T>,
}

impl<T> EvaluateExpression<T> {
    pub fn new() -> EvaluateExpression<T> {
        EvaluateExpression {
            phantom: PhantomData,
        }
    }
}

impl<T: PartialEq + PartialOrd + Clone> UnaryExpression<T>
        for EvaluateExpression<T> {
    fn evaluate(&self, t: &T) -> T {
        t.clone()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn unary_expression() {
        use super::{ConstantExpression, EvaluateExpression,
            UnaryExpression};

        let constant = ConstantExpression::<u64>::new(0u64);
        assert_eq!(constant.evaluate(&1u64), 0u64);

        let evaluate = EvaluateExpression::<u64>::new();
        assert_eq!(evaluate.evaluate(&1u64), 1u64);
    }

    #[test]
    fn compare_expression() {
        use super::{BinaryExpression, CompareExpression, CompareOp,
            ConstantExpression, EvaluateExpression};

        let constant = ConstantExpression::<u64>::new(0u64);
        let evaluate = EvaluateExpression::<u64>::new();

        let compare = CompareExpression::<u64>::new(Box::new(constant),
            Box::new(evaluate), CompareOp::Equal);

        assert_eq!(compare.evaluate(&0u64), true);
        assert_eq!(compare.evaluate(&1u64), false);
    }

    #[test]
    fn boolean_expression() {
        use super::{BinaryExpression, BooleanExpression, BooleanOp,
            CompareExpression, CompareOp, ConstantExpression,
            EvaluateExpression};

        let constant_less = ConstantExpression::<u64>::new(10u64);
        let evaluate_less = EvaluateExpression::<u64>::new();
        let compare_less = CompareExpression::<u64>::new(
            Box::new(evaluate_less), Box::new(constant_less),
            CompareOp::LessThan);

        let constant_greater = ConstantExpression::<u64>::new(0u64);
        let evaluate_greater = EvaluateExpression::<u64>::new();
        let compare_greater = CompareExpression::<u64>::new(
            Box::new(evaluate_greater), Box::new(constant_greater),
            CompareOp::GreaterThan);

        let boolean = BooleanExpression::new(
            vec!(Box::new(compare_less), Box::new(compare_greater)),
            BooleanOp::And);

        assert_eq!(boolean.evaluate(&0u64), false);
        assert_eq!(boolean.evaluate(&10u64), false);
        assert_eq!(boolean.evaluate(&5u64), true);
        assert_eq!(boolean.evaluate_bin(&5u64, &12u64), true);
        assert_eq!(boolean.evaluate_bin(&11u64, &12u64), false);
    }
}
