use std::collections::BTreeMap;

pub enum HkvResult<'a, T> {
    Value {value: &'a T},
    Node {node: &'a HkvNode<T>},
}

pub struct HkvStore<T> {
    root: HkvNode<T>,
}

impl<T> HkvStore<T> {
    pub fn new() -> HkvStore<T> {
        HkvStore {
            root: HkvNode::new(),
        }
    }

    pub fn get(&self, name: &Vec<&str>) -> Option<HkvResult<T>> {
        self.root.get(name)
    }

    pub fn set(&mut self, name: &Vec<&str>, value: T) -> Result<(), String> {
        self.root.set(name, value)
    }

    pub fn unset(&mut self, name: &Vec<&str>) {
        self.root.unset(name);
    }
}

pub struct HkvNode<T> {
    pub nodes: BTreeMap<String, HkvNode<T>>,
    pub values: BTreeMap<String, T>,
}

impl<T> HkvNode<T> {
    fn new() -> HkvNode<T> {
        HkvNode {
            nodes: BTreeMap::new(),
            values: BTreeMap::new(),
        }
    }

    fn get(&self, name: &[&str]) -> Option<HkvResult<T>> {
        if name.len() == 0 {
            Some(HkvResult::Node{node: self})
        } else if name.len() == 1 && self.values.contains_key(name[0]) {
            Some(HkvResult::Value{value: self.values.get(name[0]).unwrap()})
        } else {
            match self.nodes.get(name[0]) {
                Some(node) => node.get(&name[1..]),
                None => None,
            }
        }
    }
    
    fn set(&mut self, name: &[&str], value: T) -> Result<(), String> {
        match name.len() {
            1 => {
                if self.nodes.contains_key(name[0]) {
                    Err(format!("Value '{}' already exists as intermediate level", name[0]))
                } else {
                    self.values.insert(name[0].to_string(), value);
                    Ok(())
                }
            },
            _ => {
                if self.values.contains_key(name[0]) {
                    Err(format!("Value '{}' already exists as variable", name[0]))
                } else {
                    let node = self.nodes.entry(name[0].to_string())
                        .or_insert(HkvNode::new());
                    node.set(&name[1..], value)
                }
            },
        }
    }

    fn unset(&mut self, name: &[&str]) {
        match name.len() {
            1 => {
                self.nodes.remove(name[0]);
                self.values.remove(name[0]);
            },
            _ => {
                if let Some(node) = self.nodes.get_mut(name[0]) {
                    node.unset(&name[1..]);

                    // remove node if it no longer has information
                    if node.values.is_empty() && node.nodes.is_empty() {
                        self.nodes.remove(name[0]);
                    }
                }
            },
        }
    }
}
