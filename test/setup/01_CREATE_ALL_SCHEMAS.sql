SET SERVEROUTPUT ON SIZE UNLIMITED
PROMPT Starting schema creation process...

DECLARE
  v_count NUMBER;
BEGIN

  -- NLSUSER User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'NLSUSER';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing NLSUSER user...');
      EXECUTE IMMEDIATE 'DROP USER NLSUSER CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping NLSUSER: ' || SQLERRM);
  END;

  -- UVM User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'UVM';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing UVM user...');
      EXECUTE IMMEDIATE 'DROP USER UVM CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping UVM: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating UVM user...');
  EXECUTE IMMEDIATE 'CREATE USER UVM IDENTIFIED BY "UVM_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO UVM';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO UVM';
  EXECUTE IMMEDIATE 'ALTER USER UVM QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('UVM user created successfully with enhanced privileges');

  -- GENERAL User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'GENERAL';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing GENERAL user...');
      EXECUTE IMMEDIATE 'DROP USER GENERAL CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping GENERAL: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating GENERAL user...');
  EXECUTE IMMEDIATE 'CREATE USER GENERAL IDENTIFIED BY "GENERAL_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO GENERAL';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO GENERAL';
  EXECUTE IMMEDIATE 'ALTER USER GENERAL QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('GENERAL user created successfully with enhanced privileges');


  -- BANINST1 User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'BANINST1';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing BANINST1 user...');
      EXECUTE IMMEDIATE 'DROP USER BANINST1 CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping BANINST1: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating BANINST1 user...');
  EXECUTE IMMEDIATE 'CREATE USER BANINST1 IDENTIFIED BY "BANINST1_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO BANINST1';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO BANINST1';
  EXECUTE IMMEDIATE 'ALTER USER BANINST1 QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('BANINST1 user created successfully');

  -- SATURN User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'SATURN';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing SATURN user...');
      EXECUTE IMMEDIATE 'DROP USER SATURN CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping SATURN: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating SATURN user...');
  EXECUTE IMMEDIATE 'CREATE USER SATURN IDENTIFIED BY "SATURN_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO SATURN';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO SATURN';
  EXECUTE IMMEDIATE 'ALTER USER SATURN QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('SATURN user created successfully');

  -- TAISMGR User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'TAISMGR';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing TAISMGR user...');
      EXECUTE IMMEDIATE 'DROP USER TAISMGR CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping TAISMGR: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating TAISMGR user...');
  EXECUTE IMMEDIATE 'CREATE USER TAISMGR IDENTIFIED BY "TAISMGR_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO TAISMGR';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO TAISMGR';
  EXECUTE IMMEDIATE 'ALTER USER TAISMGR QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('TAISMGR user created successfully');

  -- BANSECR User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'BANSECR';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing BANSECR user...');
      EXECUTE IMMEDIATE 'DROP USER BANSECR CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping BANSECR: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating BANSECR user...');
  EXECUTE IMMEDIATE 'CREATE USER BANSECR IDENTIFIED BY "BANSECR_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO BANSECR';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO BANSECR';
  EXECUTE IMMEDIATE 'ALTER USER BANSECR QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('BANSECR user created successfully');

  -- TIBCO01 User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'TIBCO01';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing TIBCO01 user...');
      EXECUTE IMMEDIATE 'DROP USER TIBCO01 CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping TIBCO01: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating TIBCO01 user...');
  EXECUTE IMMEDIATE 'CREATE USER TIBCO01 IDENTIFIED BY "TIBCO01_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO TIBCO01';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO TIBCO01';
  EXECUTE IMMEDIATE 'ALTER USER TIBCO01 QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('TIBCO01 user created successfully');

  -- TIBCO02 User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'TIBCO02';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing TIBCO02 user...');
      EXECUTE IMMEDIATE 'DROP USER TIBCO02 CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping TIBCO02: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating TIBCO02 user...');
  EXECUTE IMMEDIATE 'CREATE USER TIBCO02 IDENTIFIED BY "TIBCO02_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO TIBCO02';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO TIBCO02';
  EXECUTE IMMEDIATE 'ALTER USER TIBCO02 QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('TIBCO02 user created successfully');

  -- TIBCO03 User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'TIBCO03';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing TIBCO03 user...');
      EXECUTE IMMEDIATE 'DROP USER TIBCO03 CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping TIBCO03: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating TIBCO03 user...');
  EXECUTE IMMEDIATE 'CREATE USER TIBCO03 IDENTIFIED BY "TIBCO03_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO TIBCO03';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO TIBCO03';
  EXECUTE IMMEDIATE 'ALTER USER TIBCO03 QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('TIBCO03 user created successfully');

  -- TIBCO04 User
  BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'TIBCO04';
    IF v_count > 0 THEN
      DBMS_OUTPUT.PUT_LINE('Dropping existing TIBCO04 user...');
      EXECUTE IMMEDIATE 'DROP USER TIBCO04 CASCADE';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking/dropping TIBCO04: ' || SQLERRM);
  END;

  DBMS_OUTPUT.PUT_LINE('Creating TIBCO04 user...');
  EXECUTE IMMEDIATE 'CREATE USER TIBCO04 IDENTIFIED BY "TIBCO04_password"';
  EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO TIBCO04';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO TIBCO04';
  EXECUTE IMMEDIATE 'ALTER USER TIBCO04 QUOTA UNLIMITED ON USERS';
  DBMS_OUTPUT.PUT_LINE('TIBCO04 user created successfully');

  DBMS_OUTPUT.PUT_LINE('All schema operations completed successfully');
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
    RAISE;
END;
/

COMMIT;
PROMPT Schema creation process completed. Check output for details.