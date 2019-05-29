use crate::{RadixError, RadixTrie};

#[derive(Debug, PartialEq)]
pub enum BooleanOperation {
    And,
    Or,
}

#[derive(Debug)]
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

    pub fn evaluate<T, U, F>(&self, trie: &RadixTrie<T>, token: &mut U,
            f: &mut F) where F: FnMut(&Vec<u8>, &T, &mut U) {
        let mut key = Vec::new();
        for child in trie.children.iter() {
            self.evaluate_recursive(child, token, f, 0, &mut key,
                &vec![true; self.expressions.len()]);
        }
    }

    fn evaluate_recursive<T, U, F>(&self, trie: &RadixTrie<T>,
            token: &mut U, f: &mut F, index: usize, key: &mut Vec<u8>,
            expression_mask: &[bool]) where F: FnMut(&Vec<u8>, &T, &mut U) {
        // append trie.key to key
        for value in trie.key.iter() {
            key.push(*value);
        }

        // compute (valid, include) for each expression on node
        let mut results = Vec::new();
        for (i, expression) in self.expressions.iter().enumerate() {
            if expression_mask[i] {
                results.push(expression.evaluate(&trie.key, index));
            } else {
                results.push((false, false));
            }
        }

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

        // check validity for this node
        if valid {
            // if include -> process this node
            if include {
                if let Some(value) = &trie.value {
                    f(&key, value, token);
                }
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
                self.evaluate_recursive(child, token, f,
                    index + trie.key.len(),
                    key, &children_expression_mask);
            }
        }

        // remove trie.key from key
        for _ in 0..trie.key.len() {
            let _ = key.pop();
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

pub fn parse_query(query_string: &str)
        -> Result<RadixQuery, RadixError> {
    // find indices of split characters (ex. '&' or '|')
    let characters: Vec<char> = query_string.chars().collect();
    let (mut key, mut value, mut on_key)
        = (String::new(), String::new(), true);

    let mut boolean_operation = None;
    let mut prefix_operation = PrefixOperation::Equal;
    let mut expressions = Vec::new();
    for i in 0..characters.len() {
        match characters[i] {
            x if x == '&' || x == '|' => {
                // set boolean operation
                let op = match x {
                    '&' => BooleanOperation::And,
                    '|' => BooleanOperation::Or,
                    _ => unreachable!(),
                };

                match &boolean_operation {
                    Some(current_op) => {
                        if current_op != &op {
                            return Err(RadixError::from("only one boolean operation type allowed in each query"));
                        }
                    },
                    None => boolean_operation = Some(op),
                }

                // process key and value
                match &key[..] {
                    "prefix" | "p"  => {
                        expressions.push(
                            PrefixExpression::new(value.clone(),
                                prefix_operation.clone()));
                    },
                    _ => return Err(RadixError::from("unknown key")),
                }

                // reset iteration variables
                key.clear();
                value.clear();
                on_key = true;
            },
            x if x == '=' || x == '!' => {
                // set prefix operation
                match x {
                    '=' => prefix_operation = PrefixOperation::Equal,
                    '!' => prefix_operation = PrefixOperation::NotEqual,
                    _ => unreachable!(),
                }

                // no longer on key
                on_key = false;
            },
            x => {
                match on_key {
                    true => key.push(x),
                    false => value.push(x),
                }
            },
        }
    }

    // process last key and value
    match &key[..] {
        "prefix" | "p"  => {
            expressions.push(
                PrefixExpression::new(value.clone(),
                    prefix_operation.clone()));
        },
        _ => return Err(RadixError::from("unknown key")),
    }

    Ok(RadixQuery::new(expressions,
        boolean_operation.unwrap_or(BooleanOperation::And)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn query() {
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

        //query.evaluate(&trie);
    }

    #[test]
    fn parse_query() {
        let mut trie = crate::RadixTrie::<usize>::new();
        let vec = vec!["danny", "dan", "daniel", "danerys", "david", "danerya", "everet", "emmett"];
        //let vec = vec!["8bcc", "8bce"];
        for (i, value) in vec.iter().enumerate() {
            trie.insert(value.as_bytes(), i);
        }

        //print(&trie, 0);

        let query = super::parse_query("prefix=dan&prefix!dane")
            .expect("radix query parsing");
        query.evaluate(&trie, &mut (), &mut print_process);
    }

    fn print_process(key: &Vec<u8>, value: &usize, token: &mut ()) {
        println!("{:?} : {:?}", String::from_utf8_lossy(key), value);
    }

    fn print(trie: &super::RadixTrie<usize>, depth: u8) {
        for _ in 0..depth {
            print!("    ");
        }
        println!("{} -> {:?}", String::from_utf8_lossy(&trie.key), trie.value);

        for child in trie.children.iter() {
            print(child, depth + 1);
        }
    }
}
