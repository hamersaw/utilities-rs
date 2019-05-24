use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum RadixError {
    Radix(String),               
}       
        
impl Display for RadixError {    
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {          
            RadixError::Radix(ref err) => write!(f, "RadixError: {}", err),
        }
    }
}

impl<'a> From<&'a str> for RadixError {
    fn from(err: &'a str) -> RadixError {
        RadixError::Radix(String::from(err))
    }
}

impl From<String> for RadixError {
    fn from(err: String) -> RadixError {
        RadixError::Radix(err)
    }
}
