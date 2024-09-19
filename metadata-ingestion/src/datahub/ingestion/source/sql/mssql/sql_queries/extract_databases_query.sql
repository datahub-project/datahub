SELECT 
    name 
FROM master.sys.databases 
WHERE name NOT IN (  'master'
                   , 'model'
                   , 'msdb'
                   , 'tempdb'
                   , 'Resource'
                   , 'distribution'
                   , 'reportserver'
                   , 'reportservertempdb'
                   );