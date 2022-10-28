view: fragment_derived_view 
{ derived_table: 
  { 
    sql: date DATE encode ZSTD, 
         platform VARCHAR(20) encode ZSTD AS aliased_platform, 
         country VARCHAR(20) encode ZSTD
         ;; 
  } 
}
