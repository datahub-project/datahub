It is pretty easy to create all metadata tables in mysql client.
```
cd WhereHows/data-model/DDL
mysql -hlocalhost -uwherehows -pwherehows -Dwherehows < create_all_tables_wrapper.sql
```

It is also fine to load each DDL files into a GUI client such as [DBeaver][DBV] or [Aqua Data Studio][ADS]

[DBV]: http://dbeaver.jkiss.org/
[ADS]: http://www.aquafold.com/
