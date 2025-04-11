-- 1. Create DEVELOPMENT tablespace
CREATE TABLESPACE DEVELOPMENT
  DATAFILE '/opt/oracle/oradata/FREE/development01.dbf'
  SIZE 100M
  AUTOEXTEND ON NEXT 10M
  MAXSIZE 2G
  EXTENT MANAGEMENT LOCAL
  SEGMENT SPACE MANAGEMENT AUTO
  ONLINE
  NOLOGGING;

-- 2. Create TEMP tablespace (if needed)
CREATE TEMPORARY TABLESPACE TEMP_DEV
  TEMPFILE '/opt/oracle/oradata/FREE/temp_dev01.dbf'
  SIZE 50M
  AUTOEXTEND ON NEXT 5M
  MAXSIZE 1G;

-- 3. Create UNDO tablespace (optional, only if default is insufficient)
BEGIN
  EXECUTE IMMEDIATE 'CREATE UNDO TABLESPACE UNDO_DEV
    DATAFILE ''/opt/oracle/oradata/FREE/undo_dev01.dbf''
    SIZE 100M
    AUTOEXTEND ON NEXT 10M
    MAXSIZE 2G';
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Note: Could not create undo tablespace. Using default: ' || SQLERRM);
END;
/

-- 4. Verify tablespaces were created
SELECT tablespace_name, status, contents, logging
FROM dba_tablespaces
WHERE tablespace_name IN ('DEVELOPMENT', 'TEMP_DEV', 'UNDO_DEV');

PROMPT Tablespaces created successfully in /opt/oracle/oradata/FREE/;