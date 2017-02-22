# Brand-Penetration

select
CASE
WHEN BRAND_NAME = 'Applebees' THEN BRAND_NAME
WHEN BRAND_NAME = 'McDonalds' THEN Brand_name
WHEN BRAND_NAME = 'McDonalds' THEN Brand_name
WHEN PARENT_BRAND_NAME = 'McDonalds' then 'McDonalds'
WHEN ACCOUNT_RECORD_TYPE = 'Partner' THEN ACCOUNT_NAME
WHEN ACCOUNT_RECORD_TYPE = 'Ad Agency' THEN Brand_name
WHEN ACCOUNT_RECORD_TYPE = 'Brand' THEN ACCOUNT_NAME
WHEN TYPE = 'Customer' AND Brand_name = 'Unknown Brand' THEN ACCOUNT_NAME
WHEN TYPE = 'Customer' AND Brand_name is null THEN ACCOUNT_NAME
WHEN TYPE = 'Customer' THEN Brand_name
WHEN TYPE = 'Prospect' AND Brand_name is null AND Parent_brand_Name = 'Unknown Brand' THEN ACCOUNT_NAME
WHEN TYPE = 'Prospect' AND Brand_name is null AND Parent_brand_Name is null THEN ACCOUNT_NAME
WHEN TYPE = 'Prospect' AND Brand_name is null THEN Parent_brand_Name
WHEN TYPE = 'Prospect' AND Brand_name = 'Unknown Brand' THEN ACCOUNT_Name
WHEN TYPE = 'Prospect' THEN Brand_name
WHEN TYPE = 'Unknown Brand' THEN Account_name
WHEN STATUS_C = 'Customer' THEN ACCOUNT_NAME
WHEN STATUS_C = 'Prospect' AND TYPE = 'Unknown Brand' THEN ACCOUNT_NAME
WHEN STATUS_C = 'Prospect' AND TYPE is null THEN Parent_Brand_id
WHEN type_status = 'Prospect' THEN Parent_Brand_ID
WHEN STATUS_C = 'Prospect' THEN TYPE
ELSE null
end Consolidate,
* from
(
select 
sfa.id account_id
--,netsuiteaccountid
,sfa.ns_id_c
,sfa.net_suite_internal_id_c
,sfa.name account_name
,sfa.brand_c
,sfba.name brand_name
,sfa.type
,sfa.status_c
,case when sfa.type = 'Prospect' then 'Prospect' else sfa.type||'-'||sfa.status_c end type_status
,sfa.parent_id
,sfpa.name parent_account_name
,sfpa.brand_c parent_brand_id
,sfpb.name parent_brand_name
,art.name account_record_type
--,sfpa.type||'-'||sfpa.status_c parent_type_status
,left(sfa.billing_postal_code,5) billing_postal_code
,sfa.billing_city
,sfa.billing_state_code billing_state
,sfpa.billing_state_code parent_billing_state
,t.territory_name
,sfa.number_of_employees
,sfa.of_locations_c
,sfa.naics_code
,case when sfa.naics_code like '44%' then 'Retail'
      when sfa.naics_code like '45%' then 'Retail'
      when sfa.naics_code like '722%' then 'Restaurant'
      when sfa.naics_code like '721%' then 'Hospitality'
      when sfa.naics_code like '6216%' then 'In Home Care'
      when sfa.naics_code between '623110' and '623990' then 'Residential Care'
      when sfa.naics_code like '81%' then 'Services'
      else 'Other' end naics_industry
,sfa.industry 
,sfa.named_account_c
,sfa.franchise_indicator_c
,sfa.direct_customer_in_family_tree_c
,sfa.location_c
,sfa.site
,sfa.last_activity_date
--,c.phone
--,c.email
--,u.id acct_owner_id
--,u.name acct_owner
,tu.id territory_owner_id
,tu.name territory_owner
,case when c.phone is not null or c.email is not null then 1
      when c.phone is null and c.email is null then 0
      else -1 end contactable
,c.phone_count
,c.email_count
--,act.last_activity
--,act.act_cnt_30
--,act.act_cnt_90
--,act.act_cnt_180
--,sourcing_locations
--,sourcing_all
--,hiring
--,onboarding
,nsars.NETSUITEACCOUNTID
,sajl.location_count
,'' as actual_locations
,'' as total_sourcing
,'' as total_slot
,'' as total_location_orders
,nsars.active_itemquantity sourcing_active_itemquantity
,nsars.Active_Rate sourcing_Active_Rate
,nsars.ordered_itemquantity sourcing_ordered_itemquantity
,nsars.Ordered_Rate sourcing_Ordered_Rate
,nsarsl.active_itemquantity sourcing_slot_active_itemquantity
,nsarsl.Active_Rate sourcing_slot_Active_Rate
,nsarsl.ordered_itemquantity sourcing_slot_ordered_itemquantity
,nsarsl.Ordered_Rate sourcing_slot_Ordered_Rate
,nsaro.active_itemquantity onboarding_active_itemquantity
,nsaro.Active_Rate onboarding_Active_Rate
,nsaro.ordered_itemquantity onboarding_ordered_itemquantity
,nsaro.Ordered_Rate onboarding_Ordered_Rate
,nsarh.active_itemquantity hiring_active_itemquantity
,nsarh.Active_Rate hiring_Active_Rate
,nsarh.ordered_itemquantity hiring_ordered_itemquantity
,nsarh.Ordered_Rate hiring_Ordered_Rate
--,ecomm.revenue_amount
--,ecomm.quantity
--,ecomm.date
--,ecomm.transactionnumber
--,ecomm.ecomm_account

from salesforce.account sfa
join salesforce.record_type art
on sfa.record_type_id = art.id
/*left join prod_db.dbo.dimnetsuiteaccount dnsa
on sfa.ns_id_c = dnsa.netsuiteaccountid 
*/left join salesforce.account sfpa
on sfa.parent_id = sfpa.id
left join salesforce.account sfba
on sfa.brand_c = sfba.id
left join salesforce.account sfpb
on sfpa.brand_c = sfpb.id
left join prod_db.work.sf_territory t
on sfa.id = t.account_id
left join (select ut.*,row_number() over(partition by territory_2_id order by system_modstamp desc) rn from fivetran_db.salesforce.user_territory_2_association ut) ut
on t.territory_2_id = ut.territory_2_id and ut.rn = 1
left join salesforce.user u
on sfa.owner_id = u.id
left join salesforce.user tu
on ut.user_id = tu.id
left join 
        (select 
        account_id
        ,max(phone) phone
        ,max(email) email
        ,count(phone) phone_count
        ,count(email) email_count
        from fivetran_db.salesforce.contact
        group by 1) c
on sfa.id = c.account_id       
--left join prod_db.work.sf_act_summary act 
--on sfa.id = act.account_id
left join
        (
        SELECT
        NETSUITEACCOUNTID,
        Status,
        MAX(REVRECSTARTDATE) REVRECSTARTDATE,
        MAX(REVRECENDDATE) REVRECENDDATE,
        MAX(itemquantity) itemquantity,
        CASE WHEN status = 'Active' THEN MAX(itemquantity) end active_itemquantity,
        CASE WHEN status = 'Active' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Active_Rate,
        CASE WHEN status = 'Ordered' THEN MAX(itemquantity) end ordered_itemquantity,
        CASE WHEN status = 'Ordered' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Ordered_Rate
        FROM
        (Select 
        *,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID ORDER BY REVRECSTARTDATE ASC) active_tag
         from
        (
        SELECT
        NETSUITEACCOUNTID,
        status,
        min(REVRECSTARTDATE) REVRECSTARTDATE,
        REVRECENDDATE,
        --MAX(REVRECENDDATE) REVRECENDDATE,
        SUM(itemquantity) itemquantity,
        SUM(itemrate*itemquantity)/sum(itemquantity) itemrate,
        IFNULL(max(ITEMTERM),1) ITEMTERM
        FROM
        (SELECT
        NETSUITEACCOUNTID,
        CASE WHEN REVRECSTARTDATE > current_date THEN 'Ordered' else 'Active' end Status,
        ORDERNUMBER,
        ITEMNAME,
        REVRECSTARTDATE,
        REVRECENDDATE,
        Itemquantity,
        ITEMRATE,
        ITEMTERM,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID,ITEMNAME,REVRECENDDATE ORDER BY REVRECENDDATE ASC) rn
        FROM prod_db.employer.factnetsuiteorder
        WHERE ITEMNAME in ('N - Postings: Location Based','N - Discount - Sourcing','S - Postings: Location Based','S - Discount - Sourcing','S - Job Postings: Promoted','N - Job Postings: Promoted','S - Job Postings: Promoted: 5 Pack')
        AND REVRECENDDATE >= current_date
        AND ITEMRATE != 0
        AND PRODUCTCATEGORY = 'Sourcing'
        --AND NETSUITEACCOUNTID = '7696'
        AND ISCLOSED = 'false'
         ) nsars
        --WHERE rn = 1
        GROUP BY 1,2,4
        order by 4 asc
        ) nsars 
        ) nsars 
        where active_tag = '1'
        and itemrate > 0
        GROUP BY 1,2) nsars
on cast(sfa.ns_id_c as varchar(50)) = cast(nsars.netsuiteaccountid as varchar(50))
left join
        (
        SELECT
        NETSUITEACCOUNTID,
        Status,
        MAX(REVRECSTARTDATE) REVRECSTARTDATE,
        MAX(REVRECENDDATE) REVRECENDDATE,
        MAX(itemquantity) itemquantity,
        CASE WHEN status = 'Active' THEN MAX(itemquantity) end active_itemquantity,
        CASE WHEN status = 'Active' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Active_Rate,
        CASE WHEN status = 'Ordered' THEN MAX(itemquantity) end ordered_itemquantity,
        CASE WHEN status = 'Ordered' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Ordered_Rate
        FROM
        (Select 
        *,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID ORDER BY REVRECSTARTDATE ASC) active_tag
         from
        (
        SELECT
        NETSUITEACCOUNTID,
        status,
        min(REVRECSTARTDATE) REVRECSTARTDATE,
        REVRECENDDATE,
        --MAX(REVRECENDDATE) REVRECENDDATE,
        SUM(itemquantity) itemquantity,
        SUM(itemrate*itemquantity)/sum(itemquantity) itemrate,
        IFNULL(max(ITEMTERM),1) ITEMTERM
        FROM
        (SELECT
        NETSUITEACCOUNTID,
        CASE WHEN REVRECSTARTDATE > current_date THEN 'Ordered' else 'Active' end Status,
        ORDERNUMBER,
        ITEMNAME,
        REVRECSTARTDATE,
        REVRECENDDATE,
        Itemquantity,
        ITEMRATE,
        ITEMTERM,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID,ITEMNAME,REVRECENDDATE ORDER BY REVRECENDDATE ASC) rn
        FROM prod_db.employer.factnetsuiteorder
        WHERE ITEMNAME in ('N - Job Posting Package','N - Postings: Slot Based','S - Postings: Slot Based','N - Discount - Sourcing','S - Discount - Sourcing')
        AND REVRECENDDATE >= current_date
        AND ITEMRATE != 0
        AND PRODUCTCATEGORY = 'Sourcing'
        --AND NETSUITEACCOUNTID = '7696'
        AND ISCLOSED = 'false'
         ) nsars
        --WHERE rn = 1
        GROUP BY 1,2,4
        order by 4 asc
        ) nsars 
        ) nsars 
        where active_tag = '1'
        and itemrate > 0
        GROUP BY 1,2) nsarsl
on cast(sfa.ns_id_c as varchar(50)) = cast(nsarsl.netsuiteaccountid as varchar(50))
left join
        (
        SELECT
        NETSUITEACCOUNTID,
        Status,
        MAX(REVRECSTARTDATE) REVRECSTARTDATE,
        MAX(REVRECENDDATE) REVRECENDDATE,
        MAX(itemquantity) itemquantity,
        CASE WHEN status = 'Active' THEN MAX(itemquantity) end active_itemquantity,
        CASE WHEN status = 'Active' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Active_Rate,
        CASE WHEN status = 'Ordered' THEN MAX(itemquantity) end ordered_itemquantity,
        CASE WHEN status = 'Ordered' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Ordered_Rate
        FROM
        (Select 
        *,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID ORDER BY REVRECSTARTDATE ASC) active_tag
         from
        (
        SELECT
        NETSUITEACCOUNTID,
        status,
        min(REVRECSTARTDATE) REVRECSTARTDATE,
        REVRECENDDATE,
        --MAX(REVRECENDDATE) REVRECENDDATE,
        SUM(itemquantity) itemquantity,
        SUM(itemrate*itemquantity)/sum(itemquantity) itemrate,
        IFNULL(max(ITEMTERM),1) ITEMTERM
        FROM
        (SELECT
        NETSUITEACCOUNTID,
        CASE WHEN REVRECSTARTDATE > current_date THEN 'Ordered' else 'Active' end Status,
        ORDERNUMBER,
        ITEMNAME,
        REVRECSTARTDATE,
        REVRECENDDATE,
        Itemquantity,
        ITEMRATE,
        ITEMTERM,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID,ITEMNAME,REVRECENDDATE ORDER BY REVRECENDDATE ASC) rn
        FROM prod_db.employer.factnetsuiteorder
        WHERE ITEMNAME in ('N - New Hire Paperwork','S - New Hire Paperwork','S - PM: HIRE: ENTERPRISE','S - PM: HIRE: PRO')
        AND REVRECENDDATE >= current_date
        AND ITEMRATE != 0
        AND PRODUCTCATEGORY IN ('Onboarding','Hiring')
        --AND NETSUITEACCOUNTID = '7696'
        AND ISCLOSED = 'false'
         ) nsars
        --WHERE rn = 1
        GROUP BY 1,2,4
        order by 4 asc
        ) nsars 
        ) nsars 
        where active_tag = '1'
        and itemrate > 0
        GROUP BY 1,2) nsaro
on cast(sfa.ns_id_c as varchar(50)) = cast(nsaro.netsuiteaccountid as varchar(50))
left join
        (
        SELECT
        NETSUITEACCOUNTID,
        Status,
        MAX(REVRECSTARTDATE) REVRECSTARTDATE,
        MAX(REVRECENDDATE) REVRECENDDATE,
        MAX(itemquantity) itemquantity,
        CASE WHEN status = 'Active' THEN MAX(itemquantity) end active_itemquantity,
        CASE WHEN status = 'Active' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Active_Rate,
        CASE WHEN status = 'Ordered' THEN MAX(itemquantity) end ordered_itemquantity,
        CASE WHEN status = 'Ordered' THEN (SUM(ITEMRATE)*SUM(ITEMTERM))/SUM(ITEMTERM) end Ordered_Rate
        FROM
        (Select 
        *,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID ORDER BY REVRECSTARTDATE ASC) active_tag
         from
        (
        SELECT
        NETSUITEACCOUNTID,
        status,
        min(REVRECSTARTDATE) REVRECSTARTDATE,
        REVRECENDDATE,
        --MAX(REVRECENDDATE) REVRECENDDATE,
        SUM(itemquantity) itemquantity,
        SUM(itemrate*itemquantity)/sum(itemquantity) itemrate,
        IFNULL(max(ITEMTERM),1) ITEMTERM
        FROM
        (SELECT
        NETSUITEACCOUNTID,
        CASE WHEN REVRECSTARTDATE > current_date THEN 'Ordered' else 'Active' end Status,
        ORDERNUMBER,
        ITEMNAME,
        REVRECSTARTDATE,
        REVRECENDDATE,
        Itemquantity,
        ITEMRATE,
        ITEMTERM,
        ROW_Number() OVER(PARTITION BY NETSUITEACCOUNTID,ITEMNAME,REVRECENDDATE ORDER BY REVRECENDDATE ASC) rn
        FROM prod_db.employer.factnetsuiteorder
        WHERE ITEMNAME in ('N - Hiring Manager','S - Hiring Manager','N - Discount - Referral Bonus','S - PM: Applicant Tracking System','N - PM: Applicant Tracking System')
        AND REVRECENDDATE >= current_date
        AND ITEMRATE != 0
        AND PRODUCTCATEGORY IN ('SAJ: Hiring Manager','PM: HIRE')
        --AND NETSUITEACCOUNTID = '7696'
        AND ISCLOSED = 'false'
         ) nsars
        --WHERE rn = 1
        GROUP BY 1,2,4
        order by 4 asc
        ) nsars 
        ) nsars 
        where active_tag = '1'
        and itemrate > 0
        GROUP BY 1,2) nsarh
on cast(sfa.ns_id_c as varchar(50)) = cast(nsarh.netsuiteaccountid as varchar(50))
LEFT JOIN (
    SELECT 
    ns.NetSuiteAccountId  netsuiteaccountid,
    COUNT(DISTINCT ps.dimJobLocationKey ) location_count
    FROM prod_db.dbo.factPostingPerformanceSummary  ps
    LEFT JOIN prod_db.dbo.dimCustomer  AS customer ON ps.dimCustomerKey  = customer.dimCustomerKey 
    LEFT JOIN prod_db.dbo.dimNetSuiteAccount  AS ns ON ns.dimNetSuiteAccountKey = customer.dimNetSuiteAccountKey 
    INNER JOIN prod_db.COMMON.DIMDATE  AS dimdate ON ps.dimDateKey = dimdate.DIMDATEKEY 
    WHERE (cast(cast(ps.dimDateKey as char(8)) as date) ) >= dateadd(month,-1,current_date)
    --AND ns.NetSuiteAccountId  = '1451'
    GROUP BY 1
    ORDER BY 1) sajl
    ON cast(sfa.ns_id_c as varchar(50)) = cast(sajl.netsuiteaccountid as varchar(50))

/*left join (select 
netsuiteaccountid ecomm_account,
transactionnumber,
max(dimdatekey) date,
sum(revenueamount) revenue_amount,
max(abs(quantity)) quantity
from prod_db.employer.factnetsuiterevenue
--where netsuiteaccountid = '399524'
group by 1,2) ecomm on ecomm.ecomm_account = sfa.ns_id_c*/

where ACCOUNT_RECORD_TYPE != 'Ecomm Customer'
and account_record_type = 'Direct Customer'
)cp
