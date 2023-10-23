SET SQL_SAFE_UPDATES = 0;

SELECT * FROM incognia_database.digital_banking_transaction limit 100;

alter table incognia_database.digital_banking_transaction
add column transaction_hour_24 int;

update incognia_database.digital_banking_transaction
set transaction_hour_24 = hour(transaction_datetime);

-- creating the new classification logic in sql
select * from incognia_database.fraud_transactions_feedback limit 100;


select
client_decision
,transaction_hour_24
,count(transaction_id) as transactions
from incognia_database.digital_banking_transaction
where client_decision = 'approved'
group by 1,2
order by 2 desc limit 10;

select
client_decision
,transaction_hour_24
,count(transaction_id) as n_of_transactions
,round(avg(transaction_value),2) as average_transaction_value
from incognia_database.digital_banking_transaction
where client_decision = 'denied'
group by 1,2
order by 3 desc;

select
transaction_hour_24
,count(transaction_id) as n_of_transactions
,round(avg(transaction_value),2) as average_transaction_value
from incognia_database.digital_banking_transaction
group by 1
order by 3 desc;

select
client_decision
,month(transaction_datetime) as month
,count(transaction_id) as transactions
from incognia_database.digital_banking_transaction
where client_decision = 'approved'
group by 1,2
order by 2 desc;

with a as(
select
device_id,
count(account_id)
from incognia_database.digital_banking_transaction
group by device_id
having count(account_id)>1)
select device_id from a;

select count(a.device_id) from
(
select d.device_id from (
select
device_id,
count(account_id)
from incognia_database.digital_banking_transaction
group by device_id
having count(account_id)>1) d) a;

select
count(d.device_id)
from (select distinct device_id, account_id from incognia_database.digital_banking_transaction) d
having count(distinct d.account_id)>1;

select sum(d.transaction_id_count) as sum_of_transactions
    from (
		select
		device_id,
        count(distinct transaction_id) as transaction_id_count,
		count(account_id)
		from incognia_database.digital_banking_transaction
		group by device_id
		having count(account_id)>5) d;


select 
count(db.transaction_id)
,case 
	-- categorical values filters
	when (
		db.is_emulator = true OR
		db.has_root_permissions = true OR
		db.has_fake_location = true OR
		db.app_is_tampered = true) then 'High Risk'
    -- transaction value treshold based on the transaction hour -> high risk hours with less transaction_value tolerance
     when 
		(db.transaction_hour_24 BETWEEN 4 AND 20 AND db.transaction_value > 10000) OR
		(db.transaction_hour_24 < 4 AND db.transaction_value > 1000) OR
		(db.transaction_hour_24 > 20 AND db.transaction_value > 1000) then 'High Risk'
    
    -- treshold for distance_from frequent_location - 50km, paired with high transaction_value
    when
		(db.transaction_value > 1000 and db.distance_to_frequent_location > 50000) then 'High Risk'
    -- n of accounts per device greater than one. Subquery to extract the device_ids
    -- for less impact to user experience, we're defining riskier transactions with devices with more than 5 account_ids linked
    when
		db.device_id in (select d.device_id 
		from (
			select
			device_id,
			count(account_id)
			from incognia_database.digital_banking_transaction
			group by device_id
			having count(account_id)>5) d) then 'High Risk'
    when
		-- low device age paired with high transaction value as Medium risk
        (db.device_age_days <10 and db.transaction_value > 1000) then 'Medium Risk'
    when
		-- greater values for distance_to_frequent_location
		db.distance_to_frequent_location > 50000 then 'Medium Risk'
	when
		db.device_id in (select d.device_id 
		from (
			select
			device_id,
			count(account_id)
			from incognia_database.digital_banking_transaction
			group by device_id
			having count(account_id)>1  and count(account_id)<=5) d) then 'Medium Risk'
    else 
    'Low Risk' 
		end as updated_risk_classification
from incognia_database.digital_banking_transaction db
group by 2
order by 1 desc;


select 
count(transaction_id)
,case 
	-- categorical values filters
	when (
		db.is_emulator = true OR
		db.has_root_permissions = true OR
		db.has_fake_location = true OR
		db.app_is_tampered = true) then 'High Risk'
    -- transaction value treshold based on the transaction hour -> high risk hours with less transaction_value tolerance
     when 
		(db.transaction_hour_24 BETWEEN 4 AND 20 AND db.transaction_value > 10000) OR
		(db.transaction_hour_24 < 4 AND db.transaction_value > 1000) OR
		(db.transaction_hour_24 > 20 AND db.transaction_value > 1000) then 'High Risk'
    
    -- treshold for distance_from frequent_location - 50km, paired with high transaction_value
    when
		(db.transaction_value > 1000 and db.distance_to_frequent_location > 50000) then 'High Risk'
    -- n of accounts per device greater than one. Subquery to extract the device_ids
    -- for less impact to user experience, we're defining riskier transactions with devices with more than 5 account_ids linked
    when
		db.device_id in (select d.device_id 
		from (
			select
			device_id,
			count(account_id)
			from incognia_database.digital_banking_transaction
			group by device_id
			having count(account_id)>5) d) then 'High Risk'
    when
		-- low device age paired with high transaction value as Medium risk
        (db.device_age_days <10 and db.transaction_value > 1000) then 'Medium Risk'
    when
		-- greater values for distance_to_frequent_location
		db.distance_to_frequent_location > 50000 then 'Medium Risk'
	when
		db.device_id in (select d.device_id 
		from (
			select
			device_id,
			count(account_id)
			from incognia_database.digital_banking_transaction
			group by device_id
			having count(account_id)>1  and count(account_id)<=5) d) then 'Medium Risk'
    else 
    'Low Risk' 
		end as updated_risk_classification
from incognia_database.digital_banking_transaction db
order by 1 desc
;



alter table incognia_database.digital_banking_data
add column updated_risk_classification varchar(255);

update incognia_database.digital_banking_data
set updated_risk_classification = hour(transaction_datetime);

update incognia_database.digital_banking_data
set updated_risk_classification = (case
    -- categorical values filters
    when (is_emulator = true OR
   	 has_root_permissions = true OR
   	 has_fake_location = true OR
   	 app_is_tampered = true) then 'High Risk'
	-- transaction value threshold based on the transaction hour -> high risk hours with less transaction_value tolerance
 	when
   	 (transaction_hour_24 BETWEEN 4 AND 20 AND transaction_value > 10000) OR
   	 (transaction_hour_24 < 4 AND transaction_value > 1000) OR
   	 (transaction_hour_24 > 20 AND transaction_value > 1000) then 'High Risk'
    
	-- threshold for distance_from frequent_location - 50km, paired with high transaction_value
	when
   	 (transaction_value > 1000 and distance_to_frequent_location > 50000) then 'High Risk'
	-- Number of accounts per device greater than one. Subquery to extract the device_ids
	-- for less impact on user experience, we're defining riskier transactions with devices with more than 5 account_ids linked
	when
   	 device_id in (select d.device_id
   	 from (
   		 select
   		 device_id,
   		 count(account_id)
   		 from incognia_database.digital_banking_data
   		 group by device_id
   		 having count(account_id) > 5) d) then 'High Risk'
	when
   	 -- Low device age paired with high transaction value as Medium risk
    	(device_age_days < 10 and transaction_value > 1000) then 'Medium Risk'
	when
   	 -- Greater values for distance_to_frequent_location
   	 distance_to_frequent_location > 50000 then 'Medium Risk'
    when
   	 device_id in (select d.device_id
   	 from (
   		 select
   		 device_id,
   		 count(account_id)
   		 from incognia_database.digital_banking_data
   		 group by device_id
   		 having count(account_id) > 1  and count(account_id) <= 5) d) then 'Medium Risk'
	else
	'Low Risk'
   	 end);
     
-- not fraud selection
(select
'not fraud' as fraud,
updated_risk_classification,
count(distinct db.transaction_id) as count_of_transactions
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id
where ft.transaction_id is null
group by 1,2)
union
-- fraud selection
 (
select
'fraud' as fraud,
updated_risk_classification,
count(distinct db.transaction_id) as count_of_transactions
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id
where ft.transaction_id is not null
group by 1,2);

-- current decision flow financial impact

with a as (
select
-- no fraud, revenue calculation
round((select
sum(transaction_value)*0.15
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id
where ft.transaction_id is null
and db.client_decision='Approved'),2) as revenue
-- type 2 error cost calculation
,round((
select
sum(transaction_value)*0.15
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id
where ft.transaction_id is not null
and db.client_decision ='Approved'),2) as type_2_error_cost
,count(transaction_id)*0.05 as transaction_cost
from incognia_database.digital_banking_data)
select *,
round(revenue - type_2_error_cost - transaction_cost,2) as profit
from a
;

-- proposed flow P/L
with a as (
select
-- no fraud, revenue calculation
round((select
sum(transaction_value)*0.15
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id
where ft.transaction_id is null
and db.updated_risk_classification <> 'High Risk'),2) as revenue
-- type 2 error cost calculation
,round((
select
sum(transaction_value)*0.15
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id
where ft.transaction_id is not null
-- if False Negative, risk classification must be either Low or Medium risk
and db.updated_risk_classification <> 'High Risk'),2) as type_2_error_cost 
,count(transaction_id)*0.05 as transaction_cost
from incognia_database.digital_banking_data)
select *,
round(revenue - type_2_error_cost - transaction_cost,2) as profit
from a
;



select db.transaction_id,db.client_decision,db.updated_risk_classification,db.transaction_value, ft.transaction_id as fraud_transaction
from incognia_database.digital_banking_data db
left join incognia_database.fraud_transactions_feedback ft on db.transaction_id = ft.transaction_id;









