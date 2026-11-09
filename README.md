# idx_file
## Features
This is a library for handling single-dimensional array data. It uses mmap and avltriee.

Basically, the data that can be handled must be fixed-length data, but we also have a trait for handling variable-length data.

Array data is a balanced tree algorithm that iterates from the minimum value to the maximum value, but the inserted value is always added to the end of the file and stays in the same position all the time.
In other words, sorting, searching, and obtaining values ​​by specifying rows can all be processed at high speed.
Also, since I'm using mmap, when I update the value it's automatically saved to the file.


This crate is forked from
https://crates.io/crates/idx_sized


## Usage

```rust
use std::num::NonZeroU32;

use idx_file::{AvltrieeSearch, AvltrieeUpdate, IdxFile};

let dir = std::env::temp_dir().join("idx_file-readme");
std::fs::create_dir_all(&dir).unwrap();
let path = dir.join("readme.i");
let _ = std::fs::remove_file(&path);

// The second argument is the number of rows to extend the file by at once.
let mut idx = IdxFile::<i64>::new(&path, 100);

// insert returns the row of the new value.
let row1 = idx.insert(&100);
let row2 = idx.insert(&300);
let row3 = idx.insert(&100);
let row4 = idx.insert(&150);

// update and delete by row.
idx.update(row2, &50);
idx.delete(row1);

assert_eq!(idx.value(row2), Some(&50));
assert_eq!(idx.value(row1), None);
assert_eq!(idx.value(NonZeroU32::new(99).unwrap()), None);

// iterate rows in value order.
for row in idx.iter() {
    println!("{} : {}", row, idx.value(row).unwrap());
}
assert_eq!(idx.iter_by(&100).collect::<Vec<_>>(), [row3]);
assert_eq!(idx.iter_range(&100, &200).collect::<Vec<_>>(), [row3, row4]);

// the data is saved to the file.
drop(idx);
let idx = IdxFile::<i64>::new(&path, 100);
assert_eq!(idx.value(row4), Some(&150));
```
