DROP TABLE CS_OBJECT;
CREATE TABLE CS_OBJECT (
    PK_OBJECT       INTEGER,
    OBJECT_TYPE     VARCHAR2(30),
    OBJECT_OWNER    VARCHAR2(30),
    OBJECT_NAME     VARCHAR2(30)
);
CREATE SEQUENCE SQT_CS_OBJECT;
CREATE UNIQUE INDEX IX_CS_OBJECT_001 ON CS_OBJECT (PK_OBJECT);

SELECT * FROM CS_OBJECT;
INSERT INTO CS_OBJECT (PK_OBJECT, OBJECT_OWNER, OBJECT_NAME, OBJECT_TYPE) VALUES (-1, 'DEVELOPER', 'CS_SESSION', 'TABLE');
INSERT INTO CS_OBJECT (PK_OBJECT, OBJECT_OWNER, OBJECT_NAME, OBJECT_TYPE) VALUES (-2, 'DEVELOPER', 'CS_TRACE',   'TABLE');
INSERT INTO CS_OBJECT (PK_OBJECT, OBJECT_OWNER, OBJECT_NAME, OBJECT_TYPE) VALUES (-3, 'DEVELOPER', 'CS_OBJECT',  'TABLE');
/
DROP TABLE CS_SOURCE;
CREATE TABLE CS_SOURCE (
    PK_SOURCE       INTEGER,
    FK_OBJECT       INTEGER,
    SOURCE_PLSQL    CLOB,
    CREATED_T       TIMESTAMP
);
CREATE SEQUENCE SQT_CS_SOURCE;
CREATE UNIQUE INDEX IX_CS_SOURCE_001 ON CS_SOURCE (PK_SOURCE);
CREATE INDEX IX_CS_SOURCE_002 ON CS_SOURCE (FK_OBJECT);
/
SELECT * FROM CS_SOURCE;
/
CREATE OR REPLACE PACKAGE MgrObject
IS
    FUNCTION getType(
        iType      IN VARCHAR2,
        iLength    IN PLS_INTEGER,
        iPrecision IN PLS_INTEGER,
        iScale     IN PLS_INTEGER,
        iNullable  IN VARCHAR2
    )
    RETURN VARCHAR2;
    
    FUNCTION getSource(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE,
        iType  IN CS_OBJECT.Object_Type%TYPE
    )
    RETURN CLOB;

    PROCEDURE SaveObject(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE,
        iType  IN CS_OBJECT.Object_Type%TYPE
    );
    
    PROCEDURE SaveUsert(
        iOwner IN CS_OBJECT.Object_Owner%TYPE
    );
END MgrObject;
/
CREATE OR REPLACE PACKAGE BODY MgrObject
IS
    FUNCTION getType(
        iType      IN VARCHAR2,
        iLength    IN PLS_INTEGER,
        iPrecision IN PLS_INTEGER,
        iScale     IN PLS_INTEGER,
        iNullable  IN VARCHAR2
    )
    RETURN VARCHAR2
    IS
        lValue VARCHAR2(30);
    BEGIN
        lValue := CASE
                WHEN iType = 'TIMESTAMP(6)' THEN 'TIMESTAMP'
                WHEN iType = 'VARCHAR2'     THEN 'VARCHAR2('||iLength||')'
                WHEN iType = 'NUMBER' AND COALESCE(iScale, 1) > 0 THEN 'NUMBER' || CASE WHEN iPrecision IS NOT NULL THEN '('||iPrecision||
                    CASE WHEN iScale > 0 THEN ', '||iScale END||')' ELSE NULL END
                WHEN iType = 'NUMBER' AND iPrecision IS NULL THEN 'INTEGER'
                WHEN iType = 'NUMBER' THEN 'NUMBER('||iPrecision||')'
                ELSE iType
            END||
            CASE iNullable
                WHEN 'N' THEN ' NOT NULL'
            END;
        RETURN lValue;
    END getType;
    
    FUNCTION getSourceTable(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE
    )
    RETURN CLOB
    IS
        CURSOR cuData IS
        WITH vwData AS (
            SELECT  OWNER,
                    TABLE_NAME,
                    COLUMN_ID,
                    COLUMN_NAME,
                    MgrObject.getType(DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE) DATA_TYPE
            FROM    ALL_TAB_COLUMNS x
            WHERE   OWNER = iOwner
                AND TABLE_NAME = iName
        )
        SELECT  'CREATE TABLE '||TABLE_NAME||' ('||CHR(13)||
                LISTAGG('    '||COLUMN_NAME||' '||DATA_TYPE, ','||CHR(13)) WITHIN GROUP (
                    ORDER BY COLUMN_ID
                )||CHR(13)||');'
        FROM    vwData
        GROUP BY OWNER, TABLE_NAME;
        
        lResult CLOB;
    BEGIN
        OPEN  cuData;
        FETCH cuData INTO lResult;
        CLOSE cuData;
        
        RETURN lResult;
    END getSourceTable;
    
    FUNCTION getSourceProc(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE,
        iType  IN CS_OBJECT.Object_Type%TYPE
    )
    RETURN CLOB
    IS
        CURSOR cuData IS
        SELECT  TEXT
        FROM    ALL_SOURCE
        WHERE   OWNER = iOwner
            AND NAME = iName
            AND TYPE = iType
        ORDER BY LINE ASC;
        
        lLine   CLOB;
        lSource CLOB;
    BEGIN
        OPEN  cuData;
        LOOP
            FETCH cuData INTO lLine;
            EXIT WHEN cuData%NOTFOUND;
            
            lSource := lSource||lLine;
        END LOOP;
        CLOSE cuData;
        
        RETURN lSource;
    END getSourceProc;
    
    FUNCTION getSourceSequence(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE
    )
    RETURN CLOB
    IS
        CURSOR cuData IS
        SELECT  'CREATE SEQUENCE '||SEQUENCE_NAME||' INCREMENT BY '||INCREMENT_BY||' MINVALUE '||MIN_VALUE||' MAXVALUE '||MAX_VALUE||';'
        FROM    ALL_SEQUENCES
        WHERE   SEQUENCE_OWNER = iOwner
            AND SEQUENCE_NAME  = iName;
        lResult CLOB;
    BEGIN
        OPEN  cuData;
        FETCH cuData INTO lResult;
        CLOSE cuData;
        
        RETURN lResult;
    END getSourceSequence;
    
    FUNCTION getIndexColumns(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE
    )
    RETURN CLOB
    IS
        CURSOR cuData IS
        SELECT  c.COLUMN_NAME,
                e.COLUMN_EXPRESSION
        FROM    ALL_IND_COLUMNS c
                LEFT JOIN
                ALL_IND_EXPRESSIONS e
            ON  e.INDEX_OWNER     = c.INDEX_OWNER
            AND e.INDEX_NAME      = c.INDEX_NAME
            AND e.COLUMN_POSITION = c.COLUMN_POSITION
        WHERE   c.INDEX_OWNER = iOwner
            AND c.INDEX_NAME  = iName
        ORDER BY c.COLUMN_POSITION ASC;
        
        lName   VARCHAR2(30);
        lMask   VARCHAR2(30);
        lResult CLOB;
    BEGIN
        OPEN  cuData;
        LOOP
            FETCH cuData INTO lName, lMask;
            EXIT WHEN cuData%NOTFOUND;
            
            IF lResult IS NULL THEN
                lResult := COALESCE(lMask, lName);
            ELSE
                lResult := lResult||', '||COALESCE(lMask, lName);
            END IF;
        END LOOP;
        CLOSE cuData;
        
        RETURN lResult;
    END getIndexColumns;
    
    FUNCTION getSourceIndex(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE
    )
    RETURN CLOB
    IS
        CURSOR cuData IS
        SELECT  'CREATE'||
                CASE
                    WHEN UNIQUENESS = 'UNIQUE' THEN ' UNIQUE'
                    WHEN INDEX_TYPE = 'FUNCTION-BASED BITMAP' THEN ' BITMAP'
                    WHEN INDEX_TYPE = 'BITMAP' THEN ' BITMAP'
                    ELSE ''
                END||
                ' INDEX '||INDEX_NAME||' ON '||TABLE_NAME||' (?);' Q
        FROM    ALL_INDEXES i
        WHERE   OWNER = iOwner
            AND INDEX_NAME = iName;
        lResult CLOB;
    BEGIN
        OPEN  cuData;
        FETCH cuData INTO lResult;
        CLOSE cuData;
        
        lResult := REPLACE(lResult, '?', getIndexColumns(iOwner, iName));
        
        RETURN lResult;
    END getSourceIndex;

    FUNCTION getSource(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE,
        iType  IN CS_OBJECT.Object_Type%TYPE
    )
    RETURN CLOB
    IS
    BEGIN
        IF iType IN ('TABLE') THEN
            RETURN getSourceTable(iOwner, iName);
        ELSIF iType IN ('PACKAGE', 'PACKAGE BODY', 'FUNCTION', 'PROCEDURE', 'TYPE') THEN
            RETURN getSourceProc(iOwner, iName, iType);
        ELSIF iType IN ('SEQUENCE') THEN
            RETURN getSourceSequence(iOwner, iName);
        ELSIF iType IN ('INDEX') THEN
            RETURN getSourceIndex(iOwner, iName);
        END IF;
        
        RETURN NULL;
    END getSource;

    FUNCTION getObject_Primary_Key(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE,
        iType  IN CS_OBJECT.Object_Type%TYPE
    )
    RETURN CS_OBJECT.PK_Object%TYPE
    IS
        lId CS_OBJECT.PK_Object%TYPE;
    BEGIN
        SELECT  PK_Object
        INTO    lId
        FROM    CS_OBJECT
        WHERE   Object_Owner = iOwner
            AND Object_Name  = iName
            AND Object_Type  = iType;
    
        RETURN lId;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END getObject_Primary_Key;
    
    FUNCTION getLastSource(
        iPK_Object IN CS_OBJECT.PK_Object%TYPE
    )
    RETURN CS_SOURCE.Source_Plsql%TYPE
    IS
        CURSOR cuData IS
        WITH vwData AS (
            SELECT  *
            FROM    CS_SOURCE
            WHERE   FK_Object = iPK_Object
            ORDER BY CREATED_T DESC
        )
        SELECT  SOURCE_Plsql
        FROM    vwData
        WHERE   ROWNUM = 1;
        
        lSource CS_SOURCE.Source_Plsql%TYPE;
    BEGIN
        OPEN  cuData;
        FETCH cuData INTO lSource;
        CLOSE cuData;
        
        RETURN lSource;
    END getLastSource;

    PROCEDURE SaveObject(
        iOwner IN CS_OBJECT.Object_Owner%TYPE,
        iName  IN CS_OBJECT.Object_Name%TYPE,
        iType  IN CS_OBJECT.Object_Type%TYPE
    )
    IS
        lObject CS_OBJECT%ROWTYPE;
        lPK     CS_OBJECT.PK_Object%TYPE;
        lSource     CS_SOURCE.Source_Plsql%TYPE;
        lLastSource CS_SOURCE.Source_plsql%TYPE;
    BEGIN
        lSource := getSource(iOwner, iName, iType);
        
        IF lSource IS NOT NULL THEN
            lObject.PK_Object    := getObject_Primary_Key(iOwner, iName, iType);
            lObject.Object_Owner := iOwner;
            lObject.Object_Name  := iName;
            lObject.Object_Type  := iType;
            
            IF lObject.PK_Object IS NULL THEN
                lObject.PK_Object := SQT_CS_OBJECT.NextVal;
                
                INSERT  --+ APPEND
                INTO    CS_OBJECT NOLOGGING
                VALUES  lObject;
            ELSE
                lLastSource := getLastSource(lObject.PK_Object);
            END IF;
            
            IF lLastSource IS NULL OR lLastSource <> lSource THEN
                INSERT  --+ APPEND
                INTO    CS_SOURCE (PK_Source, FK_Object, Source_Plsql, Created_T)
                VALUES (SQT_CS_SOURCE.NextVal, lObject.PK_Object, lSource, SYSTIMESTAMP);
            END IF;
        END IF;
    END SaveObject;
    
    PROCEDURE SaveUsert(
        iOwner IN CS_OBJECT.Object_Owner%TYPE
    )
    IS
        CURSOR cuData IS
        SELECT  OBJECT_NAME, OBJECT_TYPE
        FROM    ALL_OBJECTS
        WHERE   OWNER = 'DEVELOPER'
            AND OBJECT_TYPE NOT IN ('LOB');
    BEGIN
        OPEN  cuData;
        LOOP
            FETCH cuData INTO lName, lType;
            EXIT WHEN cuData%NOTFOUND;
        END LOOP;
        CLOSE cuData;
    END SaveUser;
END MgrObject;
/
SELECT  OBJECT_NAME, OBJECT_TYPE
FROM    ALL_OBJECTS
WHERE   OWNER = 'DEVELOPER'
    AND OBJECT_TYPE NOT IN ('LOB');
/
EXEC MgrObject.saveObject('DEVELOPER', 'CS_OBJECT', 'TABLE');
EXEC MgrObject.saveObject('DEVELOPER', 'CS_SOURCE', 'TABLE');
EXEC MgrObject.saveObject('DEVELOPER', 'MGROBJECT', 'PACKAGE');
EXEC MgrObject.saveObject('DEVELOPER', 'MGROBJECT', 'PACKAGE BODY');
--EXEC MgrObject.saveObject('DEVELOPER', '');

SELECT * FROM CS_OBJECT;
SELECT * FROM CS_SOURCE;

WITH vwData AS (
    SELECT  *
    FROM    CS_SOURCE
    WHERE   FK_Object = 5
    ORDER BY CREATED_T DESC
)
SELECT  * --SOURCE_Plsql
FROM    vwData
WHERE   ROWNUM = 1;
/
SELECT  MAX(CREATED_T)
FROM    CS_OBJECT,
        CS_SOURCE
WHERE   PK_OBJECT = FK_OBJECT
    AND OBJECT_OWNER = 'DEVELOPER'
    AND OBJECT_NAME  = 'CS_SESSION';

SELECT  OBJECT_TYPE, LAST_DDL_TIME
FROM    ALL_OBJECTS
WHERE   OWNER = 'DEVELOPER'
    AND OBJECT_NAME = 'MGROBJECT';

SELECT  CASE WHEN LINE > 1 THEN CHR(13) END||TEXT
FROM    ALL_SOURCE
WHERE   OWNER = 'DEVELOPER'
    AND NAME = 'MGROBJECT'
    AND TYPE = 'PACKAGE BODY';

SELECT  * --'CREATE SEQUENCE '||SEQUENCE_NAME||' INCREMENT BY '||INCREMENT_BY||' MINVALUE '||MIN_VALUE||' MAXVALUE '||MAX_VALUE||';'
FROM    ALL_SEQUENCES
WHERE   SEQUENCE_OWNER = 'DEVELOPER';

SELECT OBJECT_TYPE, COUNT(1) CNT FROM ALL_OBJECTS WHERE OWNER = 'DEVELOPER' GROUP BY OBJECT_TYPE;
SELECT * FROM ALL_OBJECTS WHERE OBJECT_TYPE NOT IN ('LOB', 'TABLE', 'PACKAGE', 'PACKAGE BODY', 'SEQUENCE');

SELECT MgrObject.getSource('DEVELOPER', 'CS_TRACE',         'TABLE')        FROM DUAL;
SELECT MgrObject.getSource('DEVELOPER', 'MGROBJECT',        'PACKAGE')      FROM DUAL;
SELECT MgrObject.getSource('DEVELOPER', 'MGROBJECT',        'PACKAGE BODY') FROM DUAL;
SELECT MgrObject.getSource('DEVELOPER', 'SQT_CS_SOURCE',    'SEQUENCE')     FROM DUAL;
SELECT MgrObject.getSource('DEVELOPER', 'IX_CS_OBJECT_001', 'INDEX')        FROM DUAL;

SELECT * FROM CS_OBJECT;
CREATE BITMAP INDEX IX_CS_OBJECT_002 ON CS_OBJECT (OBJECT_TYPE);
CREATE BITMAP INDEX IX_CS_OBJECT_003 ON CS_OBJECT (SUBSTR(OBJECT_NAME, 1, 2));
CREATE INDEX IX_CS_OBJECT_004 ON CS_OBJECT (SUBSTRB(OBJECT_NAME, 1, 2));
CREATE INDEX IX_CS_OBJECT_005 ON CS_OBJECT (OBJECT_NAME);
CREATE INDEX IX_CS_OBJECT_006 ON CS_OBJECT (OBJECT_TYPE, LENGTH(OBJECT_NAME));
CREATE INDEX IX_CS_OBJECT_007 ON CS_OBJECT (OBJECT_TYPE, OBJECT_OWNER);
CREATE INDEX IX_CS_OBJECT_008 ON CS_OBJECT (LENGTH(OBJECT_TYPE), LENGTH(OBJECT_OWNER));

DROP INDEX IX_CS_OBJECT_002;
DROP INDEX IX_CS_OBJECT_003;
DROP INDEX IX_CS_OBJECT_004;
DROP INDEX IX_CS_OBJECT_005;
DROP INDEX IX_CS_OBJECT_006;
DROP INDEX IX_CS_OBJECT_007;
DROP INDEX IX_CS_OBJECT_008;


SELECT  c.COLUMN_NAME,
        e.COLUMN_EXPRESSION
FROM    ALL_IND_COLUMNS c
        LEFT JOIN
        ALL_IND_EXPRESSIONS e
    ON  e.INDEX_OWNER     = c.INDEX_OWNER
    AND e.INDEX_NAME      = c.INDEX_NAME
    AND e.COLUMN_POSITION = c.COLUMN_POSITION
WHERE   c.INDEX_OWNER = 'DEVELOPER'
    AND c.INDEX_NAME = 'IX_CS_OBJECT_006';

SELECT  OWNER,
        INDEX_NAME,
        'CREATE'||
        CASE
            WHEN UNIQUENESS = 'UNIQUE' THEN ' UNIQUE'
            WHEN INDEX_TYPE = 'FUNCTION-BASED BITMAP' THEN ' BITMAP'
            WHEN INDEX_TYPE = 'BITMAP' THEN ' BITMAP'
            ELSE ''
        END||
        ' INDEX '||INDEX_NAME||' ON '||TABLE_NAME||' (?);' Q
FROM    ALL_INDEXES i
WHERE   OWNER = 'DEVELOPER';