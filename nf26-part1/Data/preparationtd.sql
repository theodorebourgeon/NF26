CREATE TABLE Test(name varchar(255));
INSERT INTO Test (name) VALUES ('Test');
SELECT
    *
FROM Test;
SELECT sysdate FROM dual;
SELECT * FROM user_catalog;

SET SERVEROUTPUT ON;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Hello World');
END;
