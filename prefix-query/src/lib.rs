#[derive(Debug, PartialEq)]
pub enum BooleanOperation {
    And,
    Or,
}

#[derive(Debug)]
pub struct BooleanExpression {
    expressions: Vec<PrefixExpression>,
    operation: BooleanOperation,
}

impl BooleanExpression {
    pub fn new(expressions: Vec<PrefixExpression>,
            operation: BooleanOperation) -> BooleanExpression {
        BooleanExpression {
            expressions: expressions,
            operation: operation,
        }
    }

    pub fn evaluate(&self, prefix: &str) -> bool {
        match self.operation {
            BooleanOperation::And => {
                for expression in self.expressions.iter() {
                    if !expression.evaluate(prefix) {
                        return false;
                    }
                }

                return true;
            },
            BooleanOperation::Or => {
                for expression in self.expressions.iter() {
                    if expression.evaluate(prefix) {
                        return true;
                    }
                }

                return false;
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum PrefixOperation {
    Equal,
    NotEqual,
}

#[derive(Debug)]
pub struct PrefixExpression {
    prefix: String,
    operation: PrefixOperation,
}

impl PrefixExpression {
    pub fn new(prefix: String, operation: PrefixOperation)
            -> PrefixExpression {
        PrefixExpression {
            prefix: prefix,
            operation: operation,
        }
    }

    pub fn evaluate(&self, prefix: &str) -> bool {
        let prefix_len = std::cmp::min(prefix.len(), self.prefix.len());
        let (a, b) = (&prefix[..prefix_len],
            &self.prefix[..prefix_len]);

        match self.operation {
            PrefixOperation::Equal => a == b,
            PrefixOperation::NotEqual => prefix.len() < self.prefix.len() || a != b,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn prefix_equal_expression() {
        use super::{PrefixExpression, PrefixOperation};
        let equal_expression = PrefixExpression::new("8bce".to_string(),
            PrefixOperation::Equal);

        assert_eq!(equal_expression.evaluate("8"), true);
        assert_eq!(equal_expression.evaluate("8bce"), true);
        assert_eq!(equal_expression.evaluate("8bcef4a"), true);

        assert_eq!(equal_expression.evaluate("a"), false);
        assert_eq!(equal_expression.evaluate("8bca"), false);
        assert_eq!(equal_expression.evaluate("8bcaf4a"), false);
    }

    #[test]
    fn prefix_not_equal_expression() {
        use super::{PrefixExpression, PrefixOperation};
        let equal_expression = PrefixExpression::new("8bcc".to_string(),
            PrefixOperation::NotEqual);

        assert_eq!(equal_expression.evaluate("8"), true);
        assert_eq!(equal_expression.evaluate("8bcc"), false);
        assert_eq!(equal_expression.evaluate("8bccf4a"), false);

        assert_eq!(equal_expression.evaluate("a"), true);
        assert_eq!(equal_expression.evaluate("8bca"), true);
        assert_eq!(equal_expression.evaluate("8bcaf4a"), true);
    }

    #[test]
    fn boolean_expression() {
        use super::{BooleanExpression, BooleanOperation,
            PrefixExpression, PrefixOperation};

        let equal_expression = PrefixExpression::new("8bc".to_string(),
            PrefixOperation::Equal);
        let not_equal_expression =
            PrefixExpression::new("8bcc".to_string(),
                PrefixOperation::NotEqual);
        let boolean_expression = BooleanExpression::new(
            vec!(equal_expression, not_equal_expression),
            BooleanOperation::And);

        assert_eq!(boolean_expression.evaluate("8"), true);
        assert_eq!(boolean_expression.evaluate("8bce"), true);
        assert_eq!(boolean_expression.evaluate("8bcef4a"), true);

        assert_eq!(boolean_expression.evaluate("a"), false);
        assert_eq!(boolean_expression.evaluate("8bcc"), false);
        assert_eq!(boolean_expression.evaluate("8bccf4a"), false);
    }
}
