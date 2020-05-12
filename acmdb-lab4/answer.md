- In addition to nested-loops join, I also implemented hash equi-join, which is more efficient than nested-loops join but may scan the second table more than once.
  
- I didn't change any API.

- I filled in all the blanks as `README.md` requires.

- I spent about two days on the lab. At first, I thought that only those dirty pages that already exist in BufferPool should be updated in BufferPool and I failed  `BufferPoolWriteTest`. Then I realized that those dirty pages that don't exist in BufferPool before should also be put into BufferPool. Besides, as for `Aggregate`, I'm not sure when we should do the actual aggregation calculation. Should it be done in `Constructor` or `open` or the first time calling `fetchNext`? 