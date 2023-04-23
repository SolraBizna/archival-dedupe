This is a utility that deduplicates files on a UNIX filesystem. It's suitable only for files that won't change anymore, e.g. multiple released versions of the same game.

It uses hardlinks to coalesce multiple copies of "the same" file together. Any deduplicated file will have write permissions revoked. The oldest copy of a file will be used as the "original", and therefore the modified time (etc.) will be that of the oldest file.

If files change out from under this utility while it's running, it will try to notice and stop what it's doing, but you might fool it into doing something silly, so be careful.

# Installation

`cargo install archival-dedupe`

# Legalese

`archival-dedupe` is copyright 2023, Solra Bizna, and licensed under either of:

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or
   <http://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the `archival-dedupe` crate by you, as defined
in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
