
	
1- partner table with identifier (partner_id)
2- counterparty table with identifier (counterparty_id) and foreign key (partner_id)
3- partner account table "partner_filalkunde" with identifier (counterparty_id, sequence_no) and foreign key (partner_id)
4- partner customer support table with identifier (counterparty_id, sequence_no, BETREUEINHID ) and foreign key  (acct_mngr_unit_id)
5- partner account manager table with identifier (branch, acct_mngr_id, acct_mngr_dept_id, acct_mngr_area)

Kindly note the following also:
Partner is any entity which has a relationship with the bank and based on this relationship type like being customer then a counterparty Id is generated 
Any partner could have multiple relationships with the bank and also each counterparty could have multiple accounts within the bank 
The account identifier is branch, acct_customer_no
Each account for same counterparty generates a sequence number 
The customer support is at account "filalkunde" level  and it has a field acct_mngr_unit_id is 13 digit number that contains all these info combined branch, acct_mngr_id, acct_mngr_dept_id, acct_mngr_area	
that will be used to join with partner account manager table

below if the joining logic
select * from partner partner_info
left join 
partner_natural_person 
on partner_info.PARTID = partner_natural_person.PARTID
left join 
partner_org_legal_person 
on partner_info.PARTID = partner_org_legal_person.PARTID
left join 
partner_postal_address
on partner_info.PARTID = partner_postal_address.PARTID
left join 
partner_counterparty
on partner_info.PARTID = partner_counterparty.PARTID
left join 
partner_filalkunde
on partner_filalkunde.PARTID = partner_info.PARTID  AND partner_filalkunde.counterparty_id = partner_counterparty.counterparty_id
left join 
partner_cust_support
on partner_filalkunde.counterparty_id = partner_cust_support.counterparty_id  AND partner_filalkunde.sequence_no = partner_cust_support.sequence_no
left join 
partner_acct_manager
on partner_acct_manager.branch             = partner_cust_support.substr(acct_mngr_unit_id, 2,2)
AND partner_acct_manager.acct_mngr_id      = partner_cust_support.substr(acct_mngr_unit_id, 4,2)
AND partner_acct_manager.acct_mngr_dept_id = partner_cust_support.substr(acct_mngr_unit_id, 6,3)
AND partner_acct_manager.acct_mngr_area    = partner_cust_support.substr(acct_mngr_unit_id, 9,3)

Please suggest the cluster columns for each table knowing that on every day i will do ranking of recrods from each table based on the change_date to pull the latest change per each identifier from these tables



CREATE MATERIALIZED VIEW v_partner_info_current AS
SELECT * 
FROM ( 
  SELECT *, ROW_NUMBER() OVER (PARTITION BY PARTID ORDER BY TSAEND DESC) AS rn
  FROM partner_info
) WHERE rn = 1;

CREATE MATERIALIZED VIEW v_partner_natural_person_current AS
SELECT * 
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY PARTID ORDER BY TSAEND DESC) AS rn
  FROM partner_natural_person
) WHERE rn = 1;

CREATE MATERIALIZED VIEW v_partner_org_legal_person_current AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY PARTID ORDER BY TSAEND DESC) AS rn
  FROM partner_org_legal_person
) WHERE rn = 1;

CREATE MATERIALIZED VIEW v_partner_postal_address_current AS
SELECT  *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY PARTID ORDER BY TSAEND DESC) AS rn
  FROM partner_postal_address
) WHERE rn = 1;

CREATE MATERIALIZED VIEW v_partner_counterparty_current AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY counterparty_id ORDER BY TSAEND DESC) AS rn
  FROM partner_counterparty
) WHERE rn = 1;

CREATE MATERIALIZED VIEW v_partner_filalkunde_current AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY counterparty_id, sequence_no ORDER BY TSAEND DESC) AS rn
  FROM partner_filalkunde
) WHERE rn = 1; 

CREATE MATERIALIZED VIEW v_partner_cust_support_current AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY counterparty_id, sequence_no, BETREUEINHID ORDER BY TSAEND DESC) AS rn
  FROM partner_cust_support
) WHERE rn = 1;

CREATE MATERIALIZED VIEW v_partner_acct_manager_current AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY branch, acct_mngr_id, acct_mngr_dept_id, acct_mngr_area ORDER BY TSAEND DESC) AS rn
  FROM partner_cust_support
) WHERE rn = 1;


below is the joining logic to create flat partnerdata table
select * from v_partner_info_current partner_info 
left join 
v_partner_natural_person_current partner_natural_person
on partner_info.PARTID = partner_natural_person.PARTID
left join 
v_partner_org_legal_person_current partner_org_legal_person
on partner_info.PARTID = partner_org_legal_person.PARTID
left join 
v_partner_postal_address_current partner_postal_address
on partner_info.PARTID = partner_postal_address.PARTID
left join 
v_partner_counterparty_current partner_counterparty
on partner_info.PARTID = partner_counterparty.PARTID
left join 
v_partner_filalkunde_current partner_filalkunde
on partner_filalkunde.PARTID = partner_info.PARTID  AND partner_filalkunde.counterparty_id = partner_counterparty.counterparty_id
left join 
v_partner_cust_support_current partner_cust_support
on partner_filalkunde.counterparty_id = partner_cust_support.counterparty_id  AND partner_filalkunde.sequence_no = partner_cust_support.sequence_no
left join 
v_partner_acct_manager_current partner_acct_manager
on partner_acct_manager.branch             = partner_cust_support.substr(acct_mngr_unit_id, 2,2)
AND partner_acct_manager.acct_mngr_id      = partner_cust_support.substr(acct_mngr_unit_id, 4,2)
AND partner_acct_manager.acct_mngr_dept_id = partner_cust_support.substr(acct_mngr_unit_id, 6,3)
AND partner_acct_manager.acct_mngr_area    = partner_cust_support.substr(acct_mngr_unit_id, 9,3)





WITH partner_info_current AS (
SELECT * FROM 
( 
  SELECT *, ROW_NUMBER() OVER (PARTITION BY PARTID ORDER BY TSAEND DESC) AS rn
  FROM partner_info
) Ranked
WHERE rn = 1)

,counterparty_current AS (
SELECT * FROM 
(
  SELECT *, ROW_NUMBER() OVER (PARTITION BY counterparty_id ORDER BY TSAEND DESC) AS rn
  FROM partner_counterparty
) Ranked
 WHERE rn = 1) 

,filalkunde_current AS (
SELECT * FROM 
(
  SELECT *, ROW_NUMBER() OVER (PARTITION BY counterparty_id, sequence_no ORDER BY TSAEND DESC) AS rn
  FROM partner_filalkunde
) Ranked
WHERE rn = 1)

,cust_support_current AS (
SELECT * FROM 
(
  SELECT *, ROW_NUMBER() OVER (PARTITION BY counterparty_id, sequence_no, BETREUEINHID ORDER BY TSAEND DESC) AS rn
  FROM partner_cust_support
) Ranked
WHERE rn = 1)

,acct_manager_current AS (
SELECT * FROM 
(
  SELECT *, ROW_NUMBER() OVER (PARTITION BY branch, acct_mngr_id, acct_mngr_dept_id, acct_mngr_area ORDER BY TSAEND DESC) AS rn
  FROM partner_cust_support
) Ranked
WHERE rn = 1)


select * from partner_info_current partner_info 
left join 
counterparty_current partner_counterparty
on partner_info.PARTID = partner_counterparty.PARTID
left join 
filalkunde_current partner_filalkunde
on partner_filalkunde.PARTID = partner_info.PARTID  AND partner_filalkunde.counterparty_id = partner_counterparty.counterparty_id
left join 
cust_support_current partner_cust_support
on partner_filalkunde.counterparty_id = partner_cust_support.counterparty_id  AND partner_filalkunde.sequence_no = partner_cust_support.sequence_no
left join 
acct_manager_current partner_acct_manager
on partner_acct_manager.branch             = partner_cust_support.substr(acct_mngr_unit_id, 2,2)
AND partner_acct_manager.acct_mngr_id      = partner_cust_support.substr(acct_mngr_unit_id, 4,2)
AND partner_acct_manager.acct_mngr_dept_id = partner_cust_support.substr(acct_mngr_unit_id, 6,3)
AND partner_acct_manager.acct_mngr_area    = partner_cust_support.substr(acct_mngr_unit_id, 9,3)


Partner_info    --> Partid, TSAEND
					PARTID (used for deduplication and joins)
					TSAEND (used for sorting and filtering)
natural_person  --> Partid, TSAEND
					PARTID (used for deduplication and joins)
					TSAEND (used for sorting and filtering)
org_legl_person --> Partid, TSAEND
					PARTID (used for deduplication and joins)
					TSAEND (used for sorting and filtering)
postal_addr     --> Partid, TSAEND
					PARTID (used for deduplication and joins)
					TSAEND (used for sorting and filtering)
industry        --> Partid, TSAEND
					PARTID (used for deduplication and joins)
					TSAEND (used for sorting and filtering)
partner_counterparty -> PARTID , counterparty_id , TSAEND 
						PARTID (join field with partner_info)
						counterparty_id (deduplication key)
						TSAEND (used for sorting and filtering)
partner_filalkunde  ->  PARTID , counterparty_id , sequence_no, TSAEND 
						PARTID (join field with partner_info)
					 	counterparty_id (deduplication and join key)
					 	sequence_no (deduplication key)
						TSAEND (used for sorting and filtering)
partner_cust_support ->	counterparty_id , sequence_no, BETREUEINHID, TSAEND				
						counterparty_id (deduplication and join key)
						sequence_no (deduplication and join key)
						BETREUEINHID (deduplication key)
						TSAEND (used for sorting and filtering)
partner_acct_manager -> 
						branch (deduplication and join key)
						acct_mngr_id (deduplication and join key)
						acct_mngr_dept_id (deduplication and join key)
						acct_mngr_area (deduplication and join key)
						TSAEND (used for sorting and filtering)			
						
Summary of Clustering Fields
---------------------------
Table	Clustering Fields
partner_info	PARTID, TSAEND
partner_counterparty	PARTID, counterparty_id, TSAEND
partner_filalkunde	PARTID, counterparty_id, sequence_no, TSAEND
partner_cust_support	counterparty_id, sequence_no, BETREUEINHID, TSAEND
partner_acct_manager	branch, acct_mngr_id, acct_mngr_dept_id, acct_mngr_area, TSAEND

Reasoning for Clustering Order
------------------------------
Fields used for joins come first because BigQuery will optimize data locality for join operations.
Deduplication fields are next to reduce the number of rows scanned during ranking.
Sorting and filtering fields (TSAEND) are last, as they support the ORDER BY in the deduplication logic.