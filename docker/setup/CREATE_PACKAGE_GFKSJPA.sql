-- Package Specification
CREATE OR REPLACE PACKAGE GFKSJPA AS
  -- Global variables
  ID NUMBER;
  VERSION NUMBER;

  -- Functions
  FUNCTION GETID RETURN NUMBER;
  FUNCTION GETVERSION RETURN NUMBER;

  -- Procedures
  PROCEDURE SETID(p_id IN NUMBER);
  PROCEDURE SETVERSION(p_version IN NUMBER);
END GFKSJPA;
/
SHOW ERRORS PACKAGE GFKSJPA;

-- Package Body
CREATE OR REPLACE PACKAGE BODY GFKSJPA AS
  FUNCTION GETID return number IS
  BEGIN
    RETURN 0;
  END;

  FUNCTION GETVERSION return number IS
  BEGIN
    RETURN 0;
  END;

  PROCEDURE SETID(p_id IN NUMBER) IS
  BEGIN
    ID := p_id;
  END;

  PROCEDURE SETVERSION(p_version IN NUMBER) IS
  BEGIN
    VERSION := p_version;
  END;

END GFKSJPA;
/
SHOW ERRORS PACKAGE BODY GFKSJPA;

CREATE OR REPLACE PUBLIC SYNONYM GFKSJPA FOR GFKSJPA;
GRANT EXECUTE ON GFKSJPA TO PUBLIC;

-- Print a success message
PROMPT Package GFKSJPA created successfully.
