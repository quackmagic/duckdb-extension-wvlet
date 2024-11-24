<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB [wvlet](https://wvlet.org/) Community Extension
[Wvlet](https://wvlet.org/) a cross-SQL flow-style query language for functional data modeling and interactive data analysis. 

This extension adds support for executing **wvlet** scripts directly through DuckDB SQL.

### Example
```sql
D SELECT * FROM wvlet('select 1');
-- wvlet version=0.0.0+1-e9ceb08b+20241124-0132, src=01JDF4E4BK6RA89RB7RTN4V0NV.wv:1
select 1
┌───────┐
│   1   │
│ int32 │
├───────┤
│     1 │
└───────┘
D SELECT * FROM wvlet('select version()');
-- wvlet version=0.0.0+1-e9ceb08b+20241124-0132, src=01JDF4E6NJ94JG6D5K95REX3S2.wv:1
select version() 
┌─────────────┐
│ "version"() │
│   varchar   │
├─────────────┤
│ v1.1.3      │
└─────────────┘
D CREATE TABLE t1 AS SELECT 42 AS i, 84 AS j;
D SELECT * FROM wvlet('from t1');
-- wvlet version=0.0.0+1-e9ceb08b+20241124-0132, src=01JDF4ECWR417HJQNG1GSHCGH0.wv:1
select * from t1
┌───────┬───────┐
│   i   │   j   │
│ int32 │ int32 │
├───────┼───────┤
│    42 │    84 │
└───────┴───────┘

```


### Status

- Experimental + Unstable
- Depends on a custom [libwvlet](https://github.com/quackmagic/wvlet-lib/releases/tag/nightly)
- Tests welcome, no warranties!
  
