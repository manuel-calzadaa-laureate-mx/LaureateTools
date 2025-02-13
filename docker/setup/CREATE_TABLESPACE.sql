CREATE TABLESPACE DEVELOPMENT
  DATAFILE '/tmp/oracle_datafiles/development01.dbf'  -- Ephemeral datafile location
  SIZE 100M                                           -- Initial size of the datafile
  AUTOEXTEND ON                                       -- Enable auto-extend
  NEXT 10M                                            -- Size of the next extension
  MAXSIZE UNLIMITED                                   -- Maximum size of the datafile
  EXTENT MANAGEMENT LOCAL                             -- Use local extent management
  SEGMENT SPACE MANAGEMENT AUTO                       -- Use automatic segment space management
  LOGGING;                                            -- Enable logging for the tablespace

  PROMPT Tablespace created successfully