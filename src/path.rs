use std::{
    borrow::Borrow, convert::{AsRef, From}, sync::Arc,
    cmp::{PartialEq, PartialOrd, Eq, Ord},
    ops::Deref,
    str::FromStr,
    fmt
};

pub static ESC: char = '\\';
pub static SEP: char = '/';

/// A path in the namespace. Paths are immutable and reference
/// counted.  Path components are seperated by /, which may be escaped
/// with \. / and \ are the only special characters in path, any other
/// unicode character may be used. Path lengths are not limited on the
/// local machine, but may be restricted by maximum message size on
/// the wire.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Path(Arc<str>);

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str { &*self.0 }
}

impl Borrow<str> for Path {
    fn borrow(&self) -> &str { &*self.0 }
}

impl Deref for Path {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Path {
    fn from(s: String) -> Path {
        if is_canonical(&s) {
            Path(Arc::from(s.as_str()))
        } else {
            Path(Arc::from(canonize(&s).as_str()))
        }
    }
}

impl<'a> From<&'a str> for Path {
    fn from(s: &str) -> Path {
        if is_canonical(s) {
            Path(Arc::from(s))
        } else {
            Path(Arc::from(canonize(s).as_str()))
        }
    }
}

impl<'a> From<&'a String> for Path {
    fn from(s: &String) -> Path {        
        if is_canonical(s.as_str()) {
            Path(Arc::from(s.as_str()))
        } else {
            Path(Arc::from(canonize(s.as_str()).as_str()))
        }
    }
}

impl FromStr for Path {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Path::from(s))
    }
}

fn is_canonical(s: &str) -> bool {
    for _ in Path::parts(s).filter(|p| *p == "") {
        return false;
    }
    true
}

fn canonize(s: &str) -> String {
    let mut res = String::with_capacity(s.len());
    if s.len() > 0 {
        if s.starts_with(SEP) { res.push(SEP) }
        let mut first = true;
        for p in Path::parts(s).filter(|p| *p != "") {
            if first { first = false; }
            else { res.push(SEP) }
            res.push_str(p);
        }
    }
    res
}

fn is_escaped(s: &str, i: usize) -> bool {
    let b = s.as_bytes();
    !s.is_char_boundary(i) || (b[i] == (SEP as u8) && {
        let mut res = false;
        for j in (0..i).rev() {
            if s.is_char_boundary(j) && b[j] == (ESC as u8) { res = !res; }
            else { break }
        }
        res
    })
}

impl Path {
    /// returns /
    pub fn root() -> Path { Path::from("/") }

    /// returns true if the path starts with /, false otherwise
    pub fn is_absolute(&self) -> bool { self.as_ref().starts_with(SEP) }

    /// return a new path with the specified string appended as a new
    /// part separated by the pathsep char.
    ///
    /// # Examples
    /// ```
    /// use json_pubsub::path::Path;
    /// let p = Path::root().append("bar").append("baz");
    /// assert_eq!(&*p, "/bar/baz");
    ///
    /// let p = Path::root().append("/bar").append("//baz//////foo/");
    /// assert_eq!(&*p, "/bar/baz/foo");
    /// ```
    pub fn append<T: AsRef<str>>(&self, other: T) -> Self {
        let other = other.as_ref();
        if other.len() == 0 { self.clone() }
        else {
            let mut res = String::with_capacity(self.as_ref().len() + other.len());
            res.push_str(self.as_ref());
            res.push(SEP);
            res.push_str(other);
            Path::from(res)
        }
    }

    /// return an iterator over the parts of the path. The path
    /// separator may be escaped with \. and a literal \ may be
    /// represented as \\.
    ///
    /// # Examples
    /// ```
    /// use json_pubsub::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec!["foo", "bar", "baz"]);
    ///
    /// let p = Path::from(r"/foo\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec![r"foo\/bar", "baz"]);
    ///
    /// let p = Path::from(r"/foo\\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec![r"foo\\", "bar", "baz"]);
    ///
    /// let p = Path::from(r"/foo\\\/bar/baz");
    /// assert_eq!(Path::parts(&p).collect::<Vec<_>>(), vec![r"foo\\\/bar", "baz"]);
    /// ```
    pub fn parts(s: &str) -> impl Iterator<Item=&str> {
        let skip = if s == "/" {
            2
        } else if s.starts_with("/") {
            1
        } else {
            0
        };
        utils::split_escaped(s, ESC, SEP).skip(skip)
    }

    pub fn levels(s: &str) -> usize {
        let mut p = 0;
        for _ in Path::parts(s) {
            p += 1
        }
        p
    }

    /// return the path without the last part, or return None if the
    /// path is empty or /.
    ///
    /// # Examples
    /// ```
    /// use json_pubsub::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::dirname(&p), Some("/foo/bar"));
    ///
    /// let p = Path::root();
    /// assert_eq!(Path::dirname(&p), None);
    ///
    /// let p = Path::from("/foo");
    /// assert_eq!(Path::dirname(&p), None);
    /// ```
    pub fn dirname(s: &str) -> Option<&str> {
        Path::rfind_sep(s).and_then(|i| {
            if i == 0 { None } 
            else { Some(&s[0..i]) }
        })
    }

    /// return the last part of the path, or return None if the path
    /// is empty.
    ///
    /// # Examples
    /// ```
    /// use json_pubsub::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::basename(&p), Some("baz"));
    ///
    /// let p = Path::from("foo");
    /// assert_eq!(Path::basename(&p), Some("foo"));
    ///
    /// let p = Path::from("foo/bar");
    /// assert_eq!(Path::basename(&p), Some("bar"));
    ///
    /// let p = Path::from("");
    /// assert_eq!(Path::basename(&p), None);
    ///
    /// let p = Path::from("/");
    /// assert_eq!(Path::basename(&p), None);
    /// ```
    pub fn basename(s: &str) -> Option<&str> {
        match Path::rfind_sep(s) {
            None => if s.len() > 0 { Some(s) } else { None },
            Some(i) => {
                if s.len() <= 1 { None }
                else { Some(&s[i+1..s.len()]) }
            }
        }
    }

    /// return the position of the last path separator in the path, or
    /// None if there isn't one.
    ///
    /// # Examples
    /// ```
    /// use json_pubsub::path::Path;
    /// let p = Path::from("/foo/bar/baz");
    /// assert_eq!(Path::rfind_sep(&p), Some(8));
    ///
    /// let p = Path::from("");
    /// assert_eq!(Path::rfind_sep(&p), None);
    /// ```
    pub fn rfind_sep(mut s: &str) -> Option<usize> {
        if s.len() == 0 { None }
        else {
            loop {
                match s.rfind(SEP) {
                    Some(i) =>
                        if !is_escaped(s, i) { return Some(i) } else { s = &s[0..i] }
                    None => return None,
                }
            }
        }
    }
}
