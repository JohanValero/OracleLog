DROP TABLE CS_SESSION;
CREATE TABLE CS_SESSION (
    PK_SESSION  INTEGER,
    CREATED_T   TIMESTAMP,
    CLOSED_T    TIMESTAMP,
    SID         INTEGER,
    SESSIONID   INTEGER,
    OS_USER     VARCHAR2(30),
    HOST        VARCHAR2(30),
    IP_ADDRESS  VARCHAR2(30),
    TERMINAL    VARCHAR2(30),
    FG_JOB_ID   INTEGER,
    BG_JOB_ID   INTEGER
);
CREATE SEQUENCE SQT_CS_SESSION;
/
DROP TABLE CS_TRACE;
CREATE TABLE CS_TRACE (
    PK_TRACE        INTEGER,
    FK_SESSION      INTEGER,
    CREATED_T       TIMESTAMP,
    TRACE_MESSAGE   VARCHAR2(4000),
    WHO_CALL_ME     VARCHAR2(100),
    DEPTH_TRACE     INTEGER,
    CLIENT_INFO     VARCHAR2(64 BYTE),
    MODULE          VARCHAR2(32),
    ACTION          VARCHAR2(32),
    SESSION_USER    VARCHAR2(32)
);
CREATE SEQUENCE SQT_CS_TRACE;
/
CREATE OR REPLACE PACKAGE MgrSession
AS
    -- Obtiene únicamente el nombre del procedimiento que está siendo llamado.
    FUNCTION getWhoCallMe(iLevel IN PLS_INTEGER DEFAULT 2)
    RETURN CS_Trace.Who_Call_Me%Type;
    
    -- Obtiene todo el call stack.
    FUNCTION getCallStack
    RETURN VARCHAR2;
    
    -- Obtiene la profundidad actual de objetos llamados.
    FUNCTION getDepth(iLevel IN PLS_INTEGER DEFAULT 2)
    RETURN CS_Trace.Depth_Trace%Type;
    
    -- Obtiene el PK del registro para la sessión actual.
    FUNCTION getCurrentSession
    RETURN CS_Trace.PK_Trace%Type;
    
    -- Obtiene el ROWID del registro para la sessión actual.
    FUNCTION getCurrentSessionRowid
    RETURN ROWID;
    
    -- Inicializa la session en memoria.
    PROCEDURE InitSession;
    
    -- Cierra la fecha de cierre de la sessión.
    PROCEDURE CloseSession;
    
    -- Escribe un mensaje de traza.
    PROCEDURE WriteTrace(iMessage IN CS_Trace.Trace_Message%TYPE);
    
    -- Método dummy.
    PROCEDURE Dummy;
END MgrSession;
/
CREATE OR REPLACE PACKAGE BODY MgrSession
AS
    gSession      CS_Trace.PK_Trace%Type;
    gSessionRowid ROWID;
    
    FUNCTION getCurrentSessionRowid
    RETURN ROWID
    IS
    BEGIN
        RETURN gSessionRowid;
    END getCurrentSessionRowid;
    
    FUNCTION getCurrentSession
    RETURN CS_Trace.PK_Trace%Type
    IS
    BEGIN
        RETURN gSession;
    END getCurrentSession;
    
    FUNCTION getWhoCallMe(iLevel IN PLS_INTEGER DEFAULT 2)
    RETURN CS_Trace.Who_Call_Me%Type
    IS
        lName VARCHAR2(1000);
        lLine PLS_INTEGER;
    BEGIN
        lName := utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(iLevel));
        lLine := utl_call_stack.unit_line(iLevel);
        
        RETURN lName||' ('||lLine||')';
    END;
    
    FUNCTION getCallStack
    RETURN VARCHAR2
    IS
        lCallStack VARCHAR2(4000);
    BEGIN
        FOR i IN 2..UTL_CALL_STACK.DYNAMIC_DEPTH LOOP
            lCallStack := lCallStack||CASE WHEN lCallStack IS NOT NULL THEN CHR(13) END||
                utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(i))||'('||utl_call_stack.unit_line(i)||')';
        END LOOP;
        
        RETURN lCallStack;
    END;
    
    FUNCTION getDepth(iLevel IN PLS_INTEGER DEFAULT 2)
    RETURN CS_Trace.Depth_Trace%Type
    IS
    BEGIN
        RETURN UTL_CALL_STACK.DYNAMIC_DEPTH - iLevel;
    END;
    
    PROCEDURE InitSession
    IS  PRAGMA AUTONOMOUS_TRANSACTION;
        rcSession CS_SESSION%ROWTYPE;
    BEGIN
        rcSession.PK_SESSION  := SQT_CS_SESSION.NEXTVAL;
        rcSession.CREATED_T   := SYSTIMESTAMP;
        rcSession.CLOSED_T    := NULL;
        rcSession.SID         := SYS_CONTEXT('USERENV', 'SID');
        rcSession.SESSIONID   := SYS_CONTEXT('USERENV', 'SESSIONID');
        rcSession.OS_USER     := SYS_CONTEXT('USERENV', 'OS_USER');
        rcSession.HOST        := SYS_CONTEXT('USERENV', 'HOST');
        rcSession.IP_ADDRESS  := SYS_CONTEXT('USERENV', 'IP_ADDRESS');
        rcSession.TERMINAL    := SYS_CONTEXT('USERENV', 'TERMINAL');
        rcSession.FG_JOB_ID   := SYS_CONTEXT('USERENV', 'FG_JOB_ID');
        rcSession.BG_JOB_ID   := SYS_CONTEXT('USERENV', 'BG_JOB_ID');
        
        INSERT  --+ APPEND
        INTO    CS_SESSION NOLOGGING
        VALUES  rcSession
        RETURNING   PK_SESSION, ROWID INTO gSession, gSessionRowid;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
    END InitSession;
    
    PROCEDURE CloseSession
    IS
    BEGIN
        UPDATE  CS_SESSION
        SET     CLOSED_T = SYSTIMESTAMP
        WHERE   ROWID = gSessionRowid;
        COMMIT;
    END CloseSession;
    
    PROCEDURE WriteTrace(iMessage IN CS_Trace.Trace_Message%TYPE)
    IS  PRAGMA AUTONOMOUS_TRANSACTION;
        rcTrace CS_TRACE%ROWTYPE;
    BEGIN
        rcTrace.PK_TRACE      := SQT_CS_TRACE.NEXTVAL;
        rcTrace.FK_SESSION    := gSession;
        rcTrace.CREATED_T     := SYSTIMESTAMP;
        rcTrace.TRACE_MESSAGE := iMessage;
        rcTrace.WHO_CALL_ME   := getWhoCallMe(3);
        rcTrace.DEPTH_TRACE   := getDepth(3);
        rcTrace.CLIENT_INFO   := SYS_CONTEXT('USERENV', 'CLIENT_INFO');
        rcTrace.MODULE        := SYS_CONTEXT('USERENV', 'MODULE');
        rcTrace.ACTION        := SYS_CONTEXT('USERENV', 'ACTION');
        rcTrace.SESSION_USER  := SYS_CONTEXT('USERENV', 'SESSION_USER');
        
        INSERT  --+ APPEND
        INTO    CS_TRACE NOLOGGING
        VALUES  rcTrace;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
    END WriteTrace;
    
    PROCEDURE Dummy
    IS
    BEGIN
        NULL;
    END Dummy;
BEGIN
    MgrSession.InitSession;
END MgrSession;
/
SET SERVEROUTPUT ON;
DECLARE
    PROCEDURE Prc1
    IS
    BEGIN
        MgrSession.WriteTrace('Patatica-2');
    END;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Session:'||MgrSession.getCurrentSession);
    DBMS_OUTPUT.PUT_LINE('Session rowid:'||MgrSession.getCurrentSessionRowid);
    
    MgrSession.WriteTrace('Patatica');
    Prc1;
END;
/
TRUNCATE TABLE CS_TRACE;
TRUNCATE TABLE CS_SESSION;

SELECT * FROM CS_SESSION ORDER BY CREATED_T DESC;
SELECT * FROM CS_TRACE   ORDER BY CREATED_T DESC;
/
CREATE OR REPLACE TRIGGER TRG_LOGON
AFTER LOGON ON DEVELOPER.SCHEMA
DECLARE
BEGIN
    MgrSession.Dummy;
END TRG_LOGON;
/
CREATE OR REPLACE TRIGGER TRG_LOGOFF
BEFORE LOGOFF ON DATABASE
--BEFORE LOGOFF ON DEVELOPER.SCHEMA
DECLARE
BEGIN
    MgrSession.CloseSession;
END TRG_LOGOFF;
/
CREATE OR REPLACE VIEW VW_MY_TRACE AS
SELECT  CS_TRACE.*
FROM    CS_SESSION,
        CS_TRACE
WHERE   CS_SESSION.ROWID = MgrSession.getCurrentSessionRowid
    AND PK_SESSION = FK_SESSION
ORDER BY CS_TRACE.CREATED_T DESC;

SELECT * FROM VW_MY_TRACE;