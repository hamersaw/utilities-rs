use crate::RadixTrie;

pub enum BooleanOperation {
    And,
    Or,
}

pub struct RadixQuery {
    expressions: Vec<PrefixExpression>,
    operation: BooleanOperation,
}

impl RadixQuery {
    pub fn new(expressions: Vec<PrefixExpression>,
            operation: BooleanOperation) -> RadixQuery {
        RadixQuery {
            expressions: expressions,
            operation: operation,
        }
    }

    pub fn evaluate(&self, trie: &RadixTrie<usize>) {
        for child in trie.children.iter() {
            self.evaluate_recursive(child, 0, &vec![true; self.expressions.len()], 0);
        }
    }

    fn evaluate_recursive(&self, trie: &RadixTrie<usize>,
            index: usize, expression_mask: &[bool], depth: u8) {
        // compute (valid, include) for each expression on node
        let mut results = Vec::new();
        for (i, expression) in self.expressions.iter().enumerate() {
            if expression_mask[i] {
                results.push(expression.evaluate(&trie.key, index));
            } else {
                results.push((false, false));
            }
        }

        /*for _ in 0..depth {
            print!("  ");
        }
        println!("evaluating on '{}' : {:?} : {:?}", String::from_utf8_lossy(&trie.key), &results, &expression_mask);*/

        // process expression results
        let mut valid;
        let mut include = false;
        match &self.operation {
            BooleanOperation::And => {
                valid = true;
                for result in results.iter() {
                    valid &= result.0;
                    include |= result.0 & result.1;
                }
            },
            BooleanOperation::Or => {
                valid = false;
                for result in results.iter() {
                    valid |= result.0;
                    include |= result.0 & result.1;
                }
            },
        }

        // check valdity and include value for this node
        if !valid {
            return;
        } else if include {
            // TODO - include value
            println!("INCLUDE {:?}:{:?}", String::from_utf8_lossy(&trie.key), &trie.value);
        }

        // compute expression mask for children
        let mut children_expression_mask =
            Vec::with_capacity(self.expressions.len());
        for (i, result) in results.iter().enumerate() {
            children_expression_mask.push(
                expression_mask[i] & result.0);
        }

        // execute on children
        for child in trie.children.iter() {
            self.evaluate_recursive(child, index + trie.key.len(),
                &children_expression_mask, depth + 1);
        }
    }
}

#[derive(Debug)]
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

    pub fn evaluate(&self, prefix: &[u8], index: usize)
            -> (bool, bool) {
        let include = self.prefix.len() <= prefix.len() + index;
        if index > self.prefix.len() {
            return (true, include);
        }

        // convert prefixes to equal length substrings
        let mod_prefix = &self.prefix.as_bytes()[index..];
        let (a, b) = if mod_prefix.len() > prefix.len() {
            (&mod_prefix[..prefix.len()], prefix)
        } else if mod_prefix.len() < prefix.len() {
            (&mod_prefix[..], &prefix[..mod_prefix.len()])
        } else {
            (&mod_prefix[..], prefix)
        };
  
        // compare slices
        match self.operation { 
            PrefixOperation::Equal => (a == b, include),
            PrefixOperation::NotEqual => (a != b 
                || prefix.len() + index < self.prefix.len(), include),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn insert() {
        use super::{BooleanOperation, PrefixExpression,
            PrefixOperation, RadixQuery};

        let mut trie = crate::RadixTrie::<usize>::new();

        let vec = vec!["danny", "dan", "daniel", "danerys", "david", "danerya", "everet", "emmett"];
        for (i, value) in vec.iter().enumerate() {
            trie.insert(value.as_bytes(), i);
        }

        //println!("find 'dani': {:?}", trie.get(&"dani".as_bytes()));
        //println!("find 'danny': {:?}", trie.get(&"danny".as_bytes()));

        let mut expressions = Vec::new();
        expressions.push(PrefixExpression::new("danery".to_string(),
            PrefixOperation::NotEqual));
        expressions.push(PrefixExpression::new("dan".to_string(),
            PrefixOperation::Equal));
        let query = RadixQuery::new(expressions, BooleanOperation::And);

        query.evaluate(&trie);
    }
}
