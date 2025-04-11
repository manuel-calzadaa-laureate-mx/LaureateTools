-- Package Specification
CREATE OR REPLACE PACKAGE GFKSJPA AS
  -- Declare functions/procedures (but NOT variables here)
  FUNCTION GETID RETURN NUMBER;
  FUNCTION GETVERSION RETURN NUMBER;

  PROCEDURE SETID(p_id IN NUMBER);
  PROCEDURE SETVERSION(p_version IN NUMBER);
END GFKSJPA;
/
SHOW ERRORS PACKAGE GFKSJPA;

-- Package Body (with variables properly declared)
CREATE OR REPLACE PACKAGE BODY GFKSJPA AS
  -- Global variables MUST be declared in the body
  v_id NUMBER := 0;       -- Default value
  v_version NUMBER := 0;  -- Default value

  -- Implement functions
  FUNCTION GETID RETURN NUMBER IS
  BEGIN
    RETURN v_id;
  END GETID;

  FUNCTION GETVERSION RETURN NUMBER IS
  BEGIN
    RETURN v_version;
  END GETVERSION;

  -- Implement procedures
  PROCEDURE SETID(p_id IN NUMBER) IS
  BEGIN
    v_id := p_id;
  END SETID;

  PROCEDURE SETVERSION(p_version IN NUMBER) IS
  BEGIN
    v_version := p_version;
  END SETVERSION;

END GFKSJPA;
/
SHOW ERRORS PACKAGE BODY GFKSJPA;

-- Create a public synonym (optional but useful)
CREATE OR REPLACE PUBLIC SYNONYM GFKSJPA FOR GFKSJPA;
GRANT EXECUTE ON GFKSJPA TO PUBLIC;

PROMPT Package GFKSJPA created successfully. Ready for use.;