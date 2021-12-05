MAJOR PROJECT/

CREATE TABLE TBL_LOCATIONS
(
LOCID NUMBER ,
LOCNAME VARCHAR2(50),
CONSTRAINT LOCPK PRIMARY KEY(LOCID)
);

INSERT INTO TBL_LOCATIONS SELECT LOCATION_ID,CITY FROM HR.LOCATIONS;

SELECT * FROM TBL_LOCATIONS;

GROUPING/

CREATE TABLE TBL_PARENT_GROUP
(
GRPID NUMBER,
GRPNAME VARCHAR2(50),
GRPDIRECTION CHAR(1),
GRPCREATEDON DATE,
GRPCREATEDBY NUMBER,
CONSTRAINT GRPPK PRIMARY KEY (GRPID),
CONSTRAINT EMPIDFK FOREIGN KEY(GRPCREATEDBY) REFERENCES hr.EMPLOYEES(EMPLOYEE_ID)
);

create table tbl_child_group
(
grpchildid number primary key,
grpid number,
locid number,
constraint locidfk foreign key (locid) references tbl_locations(locid),
constraint grpidfk foreign key (grpid) references tbl_parent_group(grpid)
);


select * from hr.employees;
GROUP NAME
GROUP DIRECTION

SELECT PORTS- DROP DOWN



LOCATIONS
/

CREATE TABLE TBL_ERRORLOG
(
ERROR_ID NUMBER PRIMARY KEY,
ERROR_CODE VARCHAR2(50),
ERROR_MSG VARCHAR2(500),
ERROR_WHEN DATE,
ERROR_OBJECT VARCHAR2(50),
ERROR_LINE VARCHAR2(50)
);

CREATE SEQUENCE SQERROR;

CREATE OR REPLACE PROCEDURE SP_ERRORLOG(P_ERROR_CODE IN VARCHAR2,P_ERROR_MSG IN VARCHAR2, P_ERROR_OBJECT IN VARCHAR2,
P_ERROR_LINE IN VARCHAR2) AS
BEGIN
INSERT INTO TBL_ERRORLOG
values (sqerror.nextval, p_error_code, p_error_msg, sysdate, p_error_object, p_error_line);
commit;
end;
/

CREATE OR REPLACE PACKAGE PKG_GROUPING AS
TYPE TYPLIST IS REF CURSOR;
ec varchar2(500);
em varchar2(500);
el varchar2(500);
PROCEDURE SP_LOCATIONS_LIST(P_LOCATIONS OUT PKG_GROUPING.TYPLIST);

procedure sp_group_creation
(
p_grpid in number,
p_grpname in varchar2,
p_direction in varchar2,    /* this procedure is used to insert data for group creation*/
p_location in idtyp,
p_createdby in number,
p_flag in number,
p_msg out varchar2
);

PROCEDURE SP_GROUP_LIST (P_FROM_LIST OUT PKG_GROUPING.TYPLIST, P_TO_LIST OUT PKG_GROUPING.TYPLIST);

END;
/

CREATE OR REPLACE PACKAGE BODY PKG_GROUPING AS

PROCEDURE SP_LOCATIONS_LIST(P_LOCATIONS OUT PKG_GROUPING.TYPLIST) AS
/* this procedure is used to display location list to group
*/
BEGIN
OPEN P_LOCATIONS FOR SELECT LOCID,LOCNAME FROM TBL_LOCATIONS;

EXCEPTION
WHEN OTHERS THEN
PKG_GROUPING.EC:=SQLCODE;
PKG_GROUPING.EM:=SQLERRM;
PKG_GROUPING.EL:=DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
SP_ERRORLOG (EC,EM,'SP_LOCATIONS_LIST', EL);
end;

procedure sp_group_creation
(
p_grpid in number,
p_grpname in varchar2,
p_direction in varchar2,    /* this procedure is used to insert data for group creation*/
p_location in idtyp,
p_createdby in number,
p_flag in number,
p_msg out varchar2
) as

l_cnt number;
l_grpid number;
input_ports varchar2(500);
l_addloc varchar2(500);
l_addcnt number;
l_grpname varchar2(50);
begin

if p_flag = 0 then

SELECT COUNT(1) INTO L_CNT FROM TBL_PARENT_GROUP WHERE LOWER(GRPNAME) = LOWER(P_GRPNAME);

IF L_CNT = 0 THEN

select listagg(column_value, ',') within group (order by 1) into input_ports
from table(p_location);

l_cnt := p_location.count; 

for j in (select listagg(locid, ',') within group (order by 1) as table_ports
from tbl_child_group group by grpid having count(1) = l_cnt) 
loop
if j.table_ports = input_ports then
raise_application_error(-20002, 'same set of ports exist');

end if;
end loop;
 
insert into tbl_parent_group
values(sqgrpid.nextval, p_grpname , p_direction , sysdate, p_createdby)
returning grpid into l_grpid;

for ty in 1..p_location.count loop
insert into tbl_child_group 
values (sqchildid.nextval , l_grpid, p_location(ty) );
end loop;
commit;

p_msg :='Group created successfully';

ELSE

RAISE_APPLICATION_ERROR(-20001 , 'Same Group Name Already Exist');
end if;

elsif p_flag = 1 then -- for group name edit

SELECT COUNT(1) INTO L_CNT FROM TBL_PARENT_GROUP WHERE LOWER(GRPNAME) = LOWER(P_GRPNAME);

if l_cnt!=0 then
raise_application_error(-20003,'Same Name Exists');
else 
update tbl_parent_group set grpname = p_grpname where grpid = p_grpid;
commit;
p_msg :='Group Name Updated Successfully';
end if;

ELSIF P_FLAG = 1.1 THEN --FOR ADDING LOCATION EDIT

for ty in 1..p_location.count loop
insert into tbl_child_group 
values (sqchildid.nextval , P_grpid, p_location(ty) );
end loop;

SELECT LISTAGG(LOCID, ',') WITHIN GROUP(ORDER BY 1) , COUNT(1) INTO l_addloc, l_addcnt 
FROM TBL_CHILD_GROUP
WHERE GRPID = P_GRPID;
 
FOR I IN 
 ( SELECT GRPID, LISTAGG(LOCID, ',') WITHIN GROUP(ORDER BY 1) AS checkloc
 FROM TBL_CHILD_GROUP WHERE GRPID != P_GRPID
 GROUP BY GRPID HAVING COUNT(1) = l_addcnt) LOOP
 
 IF l_addloc = I.checkloc THEN
 ROLLBACK;
 SELECT GRPNAME INTO L_GRPNAME FROM TBL_PARENT_GROUP 
 WHERE GRPID = I.GRPID;
 
 RAISE_APPLICATION_ERROR(-20004,'Same Set Already Exists in'||' '||L_GRPNAME);
 END IF;
 
 END LOOP;
 
 COMMIT;
 p_msg :='Location Added successfully';
 
 ELSIF P_FLAG = 1.2 THEN --FOR removing LOCATION EDIT

 delete from tbl_child_group
 where grpid = p_grpid
 and locid in(select * from table(p_location));
 
SELECT LISTAGG(LOCID, ',') WITHIN GROUP(ORDER BY 1) , COUNT(1) INTO l_addloc, l_addcnt 
FROM TBL_CHILD_GROUP
WHERE GRPID = P_GRPID;
 
FOR I IN 
 ( SELECT GRPID, LISTAGG(LOCID, ',') WITHIN GROUP(ORDER BY 1) AS checkloc
 FROM TBL_CHILD_GROUP WHERE GRPID != P_GRPID
 GROUP BY GRPID HAVING COUNT(1) = l_addcnt) LOOP
 
 IF l_addloc = I.checkloc THEN
 ROLLBACK;
 SELECT GRPNAME INTO L_GRPNAME FROM TBL_PARENT_GROUP 
 WHERE GRPID = I.GRPID;
 
 RAISE_APPLICATION_ERROR(-20004,'Same Set Already Exists in'||' '||L_GRPNAME);
 END IF;
 
 END LOOP;
 
 COMMIT;
 p_msg :='Location Removed Successfully';

end if; -- flag 0 for creation

end;

PROCEDURE SP_GROUP_LIST (P_FROM_LIST OUT PKG_GROUPING.TYPLIST, P_TO_LIST OUT PKG_GROUPING.TYPLIST) AS
BEGIN
OPEN P_FROM_LIST FOR SELECT GRPID, GRPNAME FROM TBL_PARENT_GROUP 
WHERE GRPDIRECTION = 'F';
OPEN P_TO_LIST FOR SELECT GRPID, GRPNAME FROM TBL_PARENT_GROUP 
WHERE GRPDIRECTION = 'T';
END;
 
END PKG_GROUPING;
/
 
create sequence sqgrpid ;
create sequence sqchildid ;
DROP SEQUENCE sqchildid;
create or replace type idtyp is table of number;
/
UNIT TEST
/

variable s refcursor;
exec pkg_grouping.sp_locations_list(:s);
print s; 

declare
locid idtyp:= idtyp(1000,1200,1300);
m varchar2(50);
begin
 pkg_grouping.sp_group_creation('ABC','F',locid,100,m);
 dbms_output.put_line(m);
 end;
 /
 
 ALTER PROCEDURE sp_group_creation COMPILE;
 
set serveroutput on;

select * from tbl_parent_group;
select * from tbl_child_group;

ALTER TABLE tbl_parent_group ENABLE PRIMARY KEY ;
ALTER TABLE TBL_CHILD_GROUP ENABLE CONSTRAINT grpidfk ;

TRUNCATE TABLE tbl_child_group;

SELECT * FROM USER_CONSTRAINTS WHERE TABLE_NAME = 'TBL_PARENT_GROUP';

declare
locid idtyp:= idtyp(1000,1800,1900,2300);
m varchar2(50);
begin
 pkg_grouping.sp_group_creation('MNOP','T',locid,104,m);
 dbms_output.put_line(m);
 end;
 /
 
 SELECT GRPNAME,COUNT(1) AS NO_OF_LOCATIONS, LISTAGG(LOCNAME,',') WITHIN GROUP (ORDER BY 1) AS SET_OF_PORTS
 FROM TBL_PARENT_GROUP P , TBL_CHILD_GROUP C , TBL_LOCATIONS L
 WHERE P.GRPID = C.GRPID
 AND C.LOCID = L.LOCID 
 GROUP BY GRPNAME;
 
 SELECT COUNT(1)  FROM TBL_PARENT_GROUP WHERE LOWER(GRPNAME) = LOWER('ABC');
 
 SELECT GRPNAME,grpdirection, COUNT(1) AS NO_OF_LOCATIONS, LISTAGG(L.LOCNAME,',') WITHIN GROUP (ORDER BY 1) AS SET_OF_PORTS
 FROM TBL_PARENT_GROUP P , TBL_CHILD_GROUP C , TBL_LOCATIONS L
 WHERE P.GRPID = C.GRPID
 AND C.LOCID = L.LOCID 
 GROUP BY GRPNAME, grpdirection;
 
 
 select COLUMN_VALUE from TABLE(P_LOCATION); ==> COLLECTION DATATYPE CAN BE USED AS TABLE/
 
 
 UNIT TEST FOR SAME SET OF PORTS/
 
 declare
locid idtyp:= idtyp(1300,1700,2400);
m varchar2(50);
begin
 pkg_grouping.sp_group_creation('RSQ','F',locid,107,m);
 dbms_output.put_line(m);
 end;
 /
 
 
 PROCEDURE SP_GROUP_LIST (P_FROM_LIST OUT PKG_GROUPING.TYPLIST, P_TO_LIST OUT PKG_GROUPING.TYPLIST) AS
 BEGIN
 OPEN P_FROM_LIST FOR SELECT GRPID, GRPNAME FROM TBL_PARENT_GROUP 
 WHERE GRPDIRECTION = 'F';
  OPEN P_TO_LIST FOR SELECT GRPID, GRPNAME FROM TBL_PARENT_GROUP 
 WHERE GRPDIRECTION = 'T';
 END;
 /
 
 create or replace procedure sp_get_group_details(p_userid in number, p_group_details out pkg_grouping.typlist) as
 begin
 open p_group_details for
 select p.grpid, grpname, grpdirection, count(1) as no_of_locations,
 listagg(l.locname, ',') within group(order by 1) as
 set_of_ports
 from tbl_parent_group p, tbl_locations l, tbl_child_group c
 where p.grpid = c.grpid
 and c.locid = l.locid
 and grpcreatedby = p_userid
 group by grpname, grpdirection, p.grpid;
 end;
 /
 
declare
m varchar2(50);
begin
 pkg_grouping.sp_group_creation(1,'ZWR',null,null,null,1,m);
 dbms_output.put_line(m);
 end;
 /
 
 set serveroutput on;
 
 
 select * from user_objects where object_name = 'PACKAGE';
 
 SELECT LISTAGG(LOCID, ',') WITHIN GROUP(ORDER BY 1) , COUNT(1) INTO X,Y 
 FROM TBL_CHILD_GROUP
 WHERE GRPID = P_GRPID;
 
 FOR I IN 
 ( SELECT LISTAGG(LOCID, ',') WITHIN GROUP(ORDER BY 1) AS Z
 FROM TBL_CHILD_GROUP WHERE GRPID != P_GRPID
 GROUP BY GRPID HAVING COUNT(1) = Y) LOOP
 
 IF X=I.Z THEN
 ROLLBACK;
 RAISE_APPLICATION_ERROR(-20004,'Same Set Already Exists');
 END IF;
 
 END LOOP;
 
 COMMIT;
 
  SELECT p.grpid,GRPNAME,grpdirection, COUNT(1) AS NO_OF_LOCATIONS, LISTAGG(L.LOCid,',') WITHIN GROUP (ORDER BY 1) AS SET_OF_PORTS
 FROM TBL_PARENT_GROUP P , TBL_CHILD_GROUP C , TBL_LOCATIONS L
 WHERE P.GRPID = C.GRPID
 AND C.LOCID = L.LOCID 
 GROUP BY GRPNAME, grpdirection,p.grpid;
 
 DELETE FROM TBL_CHILD_GROUP WHERE LOCID = 1700;
 
 
 declare
 locid idtyp := idtyp(1900);
m varchar2(50);
begin
 pkg_grouping.sp_group_creation(2,null,null,locid,null,1.2,m);
 dbms_output.put_line(m);
 end;
 /
 
 declare
 loc idtyp := idtyp(1000,1800);
 begin
 delete from tbl_child_group
 where grpid = 2
 and locid in(select * from table(loc));
 end;
 /
 
 rollback;
 