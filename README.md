# Sequel::SeekPagination

This gem provides support for "seek" or "keyset" pagination for Sequel and PostgreSQL. In seek pagination, you pass the pagination function the last data you saw, and it returns the next page in the set. Example:

```
# Activate the extension:
Sequel::Database.extension :seek_pagination
# or
DB.extension :seek_pagination
# or
ds = DB[:seek]
ds.extension(:seek_pagination)

# Use the new Dataset#seek_paginate method to get the first page.
DB[:seek].order(:id).seek_paginate(50) # SELECT * FROM "seek" ORDER BY "id" LIMIT 50

# Use the last data you saw to get the second page.
# (suppose the id of the last row you got was 1456)
DB[:seek].order(:id).seek_paginate(50, after: 1456) # SELECT * FROM "seek" WHERE ("id" > 1456) ORDER BY "id" LIMIT 50

# Also works when sorting by multiple columns.
DB[:seek].order(:col1, :col2).seek_paginate(50) # SELECT * FROM "seek" ORDER BY "col1", "col2" LIMIT 50
DB[:seek].order(:col1, :col2).seek_paginate(50, after: [12, 56]) # SELECT * FROM "seek" WHERE (("col1", "col2") > (12, 56)) ORDER BY "col1", "col2" LIMIT 50
```

### Why Seek Pagination?

Performance. The WHERE conditions generated above can use an index much more efficiently on deeper pages. For example, using traditional LIMIT/OFFSET pagination, retrieving the 9th page of 30 records requires that the database process at least 270 rows to get the ones you want. With seek pagination, getting the 100th page is just as efficient as getting the second.

Additionally, there's no slow count(*) of the entire table to get the number of pages that are available. This is especially valuable when you're querying on a complex join or the like.

### Why Not Seek Pagination?

The total number of pages isn't available (unless you do a count(*) yourself), and you can't jump to a specific page in the table. This makes seek pagination ideal for infinitely scrolling pages.

### Caveats

It's advisable for the column set you're sorting on to be unique, or else there's the risk that results will be duplicated if multiple rows have the same value across a page break. You may be able to get away with doing this on non-unique column sets if they have very high cardinality (for example, if one of the columns is a created_at timestamp that will rarely be repeated). If you need to enforce a unique column set to get a stable sort, you can always add a unique column to the end of the ordering.
