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
### init
```rust
use idx_file::IdxFile;

let mut idx=IdxFile::<i64>::new("hoge.idx").unwrap();
```

### insert
```rust
idx.insert(&100);
idx.insert(&300);
idx.insert(&100);
idx.insert(&150);
```

### update
```rust
idx.update(2, &50);
```

### delete
```rust
idx.delete(1);
```

### search
```rust
for row in idx.iter() {
    println!(" {} : {}", row, **unsafe { idx.get_unchecked(row) });
}

for row in idx.iter_by(&100) {
    println!(" {} : {}", row, **unsafe { idx.get_unchecked(row) });
}
for row in idx.iter_from(&100) {
    println!(" {} : {}", row, **unsafe { idx.get_unchecked(row) });
}
for row in idx.iter_to(&200) {
    println!(" {} : {}", row, **unsafe { idx.get_unchecked(row) });
}
for row in idx.iter_range(&100, &200) {
    println!(" {} : {}", row, **unsafe { idx.get_unchecked(row) });
}
```