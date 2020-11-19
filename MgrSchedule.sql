DROP TABLE CS_SCHEDULE;
CREATE TABLE CS_SCHEDULE (
    PK_SCHEDULE INTEGER,
    SCHEDULE_T  TIMESTAMP,
    EXECUTED_T  TIMESTAMP,
    PLSQL       VARCHAR2(1000)
);
CREATE SEQUENCE SQT_CS_SCHEDULE;

CREATE TABLE CS_SCHEDULE_PARAMS (
    FK_SCHEDULE INTEGER,
    PARAM_ID    INTEGER,
    PARAM_VALUE VARCHAR2(1000)
);
CREATE SEQUENCE SQT_CS_SCHEDULE_PARAMS;
/
CREATE OR REPLACE PACKAGE MgrSchedule
IS
    TYPE tyParamValue IS TABLE OF CS_SCHEDULE_PARAMS.PARAM_VALUE%TYPE;
    
    PROCEDURE AddSchedule(
        iPlsql    IN CS_SCHEDULE.Plsql%TYPE,
        iValues   IN tyParamValue                DEFAULT NULL,
        iSchedule IN CS_SCHEDULE.Executed_T%TYPE DEFAULT NULL
    );
    
    PROCEDURE RunSchedule(
        iSchedule IN CS_SCHEDULE.PK_SCHEDULE%TYPE
    );
END MgrSchedule;
/
CREATE OR REPLACE PACKAGE BODY MgrSchedule
IS
    PROCEDURE AddSchedule(
        iPlsql    IN CS_SCHEDULE.Plsql%TYPE,
        iValues   IN tyParamValue                DEFAULT NULL,
        iSchedule IN CS_SCHEDULE.Executed_T%TYPE DEFAULT NULL
    )
    IS
        rcSchedule CS_SCHEDULE%ROWTYPE;
        rcParam    CS_SCHEDULE_PARAMS%ROWTYPE;
    BEGIN
        rcSchedule.PK_SCHEDULE := SQT_CS_SCHEDULE.NEXTVAL;
        rcSchedule.SCHEDULE_T  := COALESCE(iSchedule, SYSTIMESTAMP + INTERVAL '5' SECOND);
        rcSchedule.EXECUTED_T  := NULL;
        rcSchedule.PLSQL       := iPlsql;
        
        INSERT  --+ APPEND
        INTO    CS_SCHEDULE NOLOGGING
        VALUES  rcSchedule;
        
        IF iValues IS NOT NULL THEN
            FOR i IN 1..iValues.COUNT LOOP
                rcParam.FK_SCHEDULE := rcSchedule.PK_SCHEDULE;
                rcParam.PARAM_ID    := i;
                rcParam.PARAM_VALUE := iValues(i);
                
                INSERT  --+ APPEND
                INTO    CS_SCHEDULE_PARAMS NOLOGGING
                VALUES  rcParam;
            END LOOP;
        END IF;
        
        DBMS_SCHEDULER.CREATE_JOB (
            job_name        => 'JOB_'||rcSchedule.PK_SCHEDULE,
            job_type        => 'PLSQL_BLOCK',
            job_action      => 'BEGIN MgrSchedule.RunSchedule('||rcSchedule.PK_SCHEDULE||'); END;',
            start_date      => rcSchedule.SCHEDULE_T,
            repeat_interval => NULL,
            enabled         => TRUE
        );
    END AddSchedule;
    
    PROCEDURE RunSchedule(
        iSchedule CS_SCHEDULE%ROWTYPE
    )
    IS  PRAGMA AUTONOMOUS_TRANSACTION;
        lData VARCHAR2(4000);
    BEGIN
        EXECUTE IMMEDIATE 'BEGIN '||iSchedule.PLSQL||'; END;';
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END RunSchedule;
    
    PROCEDURE RunSchedule(
        iSchedule IN CS_SCHEDULE.PK_SCHEDULE%TYPE
    )
    IS
        rcSchedule CS_SCHEDULE%ROWTYPE;
    BEGIN
        SELECT  *
        INTO    rcSchedule
        FROM    CS_SCHEDULE
        WHERE   PK_SCHEDULE = iSchedule;
        
        UPDATE  CS_SCHEDULE
        SET     EXECUTED_T = SYSTIMESTAMP
        WHERE   PK_Schedule = iSchedule;
        
        COMMIT;
        
        RunSchedule(rcSchedule);
    EXCEPTION
        WHEN OTHERS THEN
            COMMIT;
            MgrSession.WriteTrace('ERROR: '||SQLERRM);
            RAISE;
    END RunSchedule;
END MgrSchedule;
/
TRUNCATE TABLE CS_SESSION;
TRUNCATE TABLE CS_TRACE;
TRUNCATE TABLE CS_SCHEDULE;

SELECT * FROM CS_SESSION ORDER BY CREATED_T DESC;
SELECT * FROM CS_TRACE   ORDER BY CREATED_T DESC;
SELECT * FROM VW_MY_TRACE;
SELECT * FROM CS_SCHEDULE;

SELECT * FROM ALL_SCHEDULER_JOBS;

--EXEC MgrSession.InitSession;
EXEC MgrSchedule.AddSchedule('MgrSession.WriteTrace(''PATATA KBOOM-3!!!'')');
EXEC MgrSession.WriteTrace('PATATA HELLO!!!');
EXEC MgrSession.WriteTrace('PATATA KBOOM-3!!!');