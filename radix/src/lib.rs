mod error;
pub mod query;

pub use error::RadixError;
pub use query::{parse_query, BooleanOperation, PrefixExpression, PrefixOperation, RadixProcessor, RadixQuery};

pub struct RadixTrie<T> {
    key: Vec<u8>,
    value: Option<T>,
    children: Vec<RadixTrie<T>>,
}

impl<T> RadixTrie<T> {
    pub fn new() -> RadixTrie<T> {
        RadixTrie {
            key: Vec::new(),
            value: None,
            children: Vec::new(),
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if key.len() == 0 {
            return self.value.is_some();
        }

        // get longest key match
        let (index, match_length) = self.get_longest_match(key);
        if index == self.children.len() {
            false
        } else {
            self.children[index].contains(&key[match_length..])
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&T> {
        if key.len() == 0 {
            return self.value.as_ref();
        }

        // get longest key match
        let (index, match_length) = self.get_longest_match(key);
        if index == self.children.len() {
            None
        } else {
            self.children[index].get(&key[match_length..])
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        if key.len() == 0 {
            return self.value.as_mut();
        }

        // get longest key match
        let (index, match_length) = self.get_longest_match(key);
        if index == self.children.len() {
            None
        } else {
            self.children[index].get_mut(&key[match_length..])
        }
    }

    fn get_longest_match(&self, key: &[u8]) -> (usize, usize) {
        let mut match_length = 0;
        for (i, child) in self.children.iter().enumerate() {
            // compute prefix match lengths
            while match_length < child.key.len()
                    && match_length < key.len()
                    && child.key[match_length] == key[match_length] {
                match_length += 1;
            }

            // if there is a match -> insert using child
            if match_length != 0 {
                return (i, match_length);
            }
        }

        (self.children.len(), match_length)
    }

    pub fn insert(&mut self, key: &[u8], value: T)
            -> Result<(), RadixError> {
        if key.len() == 0 {
            return Err(RadixError::from("TODO - message?"));
        }

        // get longest key match
        let (index, match_length) = self.get_longest_match(key);

        // insert key -> value pair
        if index < self.children.len() {
            if match_length != self.children[index].key.len() {
                // if split -> split child and insert new node
                let mut child = self.children.remove(index);

                let mut split_key = Vec::new();
                for _ in 0..match_length {
                    split_key.push(child.key.remove(0));
                }

                // initialize split node
                let mut split_node = RadixTrie {
                    key: split_key,
                    value: None,
                    children: vec!(child),
                };

                // insert new key -> value
                if key.len() == match_length {
                    split_node.value = Some(value);
                } else {
                    let node = RadixTrie {
                        key: key[match_length..].to_vec(),
                        value: Some(value),
                        children: Vec::new(),
                    };
                    split_node.children.push(node);
                }

                self.children.push(split_node);
            } else {
                // if no split -> insert into child
                self.children[index].insert(&key[match_length..], value)?;
            }
        } else {
            // if no match -> create new child
            let node = RadixTrie {
                key: key.to_vec(),
                value: Some(value),
                children: Vec::new(),
            };

            self.children.push(node);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn insert() {
        let mut trie = super::RadixTrie::<String>::new();
        trie.insert(&"danny".as_bytes(), String::from("rammer"));
        trie.insert(&"dan".as_bytes(), String::from("the man"));
        trie.insert(&"danerys".as_bytes(), String::from("targeryn"));
        //print(&trie, 0);

        trie.insert(&"david".as_bytes(), String::from("&goliath"));
        //print(&trie, 0);

        trie.insert(&"danerya".as_bytes(), String::from("blartaryn"));
        //print(&trie, 0);

        /*println!("find 'dani': {:?}", trie.get(&"dani".as_bytes()));
        println!("find 'danny': {:?}", trie.get(&"danny".as_bytes()));
        println!("find 'davidson': {:?}", trie.get(&"davidson".as_bytes()));
        println!("find 'da': {:?}", trie.get(&"da".as_bytes()));
        println!("find 'danerys': {:?}", trie.get(&"danerys".as_bytes()));*/
    }

    fn print(node: &super::RadixTrie<String>, depth: u8) {
        for _ in 0..depth {
            print!("  ");
        }

        println!("{}: {:?}", String::from_utf8_lossy(&node.key), node.value); 

        for child in node.children.iter() {
            print(child, depth + 1);
        }
    }
}
