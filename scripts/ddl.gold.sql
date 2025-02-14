==============================
  DDL script:create gold views
  script purpuse:
       this script creates views for the gold layer in the data warehouse.
      the gold layer represents the final dimentions and fact tables(star chema)
     each veiw performs transformatios and combines data from the silver layer to provide a clean,
  enriched, and business-ready dataset
  usage:
  these views can be queried directly fo analytics and reporting 
=========================

/*
lets check our joind tables if they have duplicate
select prd_key, count(*) from(select----
----)t group by prd_key
having count(*)>1 ---our joind tables do not have any duplicates
*/
/*
sort the colums into logical groups to improve readablity
*/
/*
rename columns to friendly meaningful names
*/
/*so these are describing the business object we dont have
transactions events we dont have a fact we have a dimention

since this is a dimention we have to create a soregate key
using the window function
row_number() over (order by pn.pr.prd_start_dt, pn.prd_key) as product_key
*/
/*
finally lets built our dimention view
*/

-- the relationship between the tables may be not one to one so 
-- we are hvaing duplicates so now we encapsulate the whole query with
/*
select cst_id, count(*) from (select------
------)t group by cst_id
having count(*)>1

*/
/*
--second we have two columns for gender from the master table and the join
so if there is two different values we have to take the one from the 
master table
and if we donot have value(n/a) from the master table and we have value
from the join we have to take value from the join
if there is null since we already clean the tables this value is
coming from the joining
to fix this we edited the following query
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr !='n/a' then ci.cst_gndr --CRM is the master for gender info
          else coalesce(ca.gen, 'n/a' )
		  end as new_gen(gender)
 */
 /*--since this is the gold layer we have to give the colums name
 a customer friendly name
 --naming convention: use snake_case whith lower letters and underscore(_) to separate words
 --language: use english language
 --avoid reserved words: Donot use sql reserved wordss as object names

*/
/*
sort the colums based up on their importance
*/
/*finally since this is a dimention table lets add a sorogate key 
using the window function row_number
row_number() over (order by cst_id) as customer_key
*/
create view gold.dim_customers as --finally we make our dimention query a view
SELECT 
       row_number() over (order by cst_id) as customer_key
      ,ci.cst_id as customer_id
      ,ci.cst_key as customer_number
      ,ci.cst_firstname as first_name
      ,ci.cst_lastname as last_name
	  ,la.cntry as country
      ,ci.cst_material_status as marital_status
      ,case when ci.cst_gndr !='n/a' then ci.cst_gndr --CRM is the master for gender info
          else coalesce(ca.gen, 'n/a' )
		  end as gender
	  ,ca.bdate as birthdate
      ,ci.cst_create_data as created_date
  FROM DataWarehouse.silver.crm_cust_info ci
  left join silver.erp_cust_az12 ca
  on ci.cst_key= ca.cid
  left join silver.erp_loc_a101 la
  on ci.cst_key=la.cid;

 --=============================================
 --================================================

create view gold.dim_products as 
SELECT 
       row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key
	  ,pn.prd_id as product_id
      ,pn.prd_key as product_number
      ,pn.prd_nm as product_name
      ,pn.cat_id as category_id
      ,pc.cat as category
	  ,pc.subcat as subcategory
	  ,pc.maintenance
      ,pn.prd_cost as cost
      ,pn.prd_line as product_line
      ,pn.prd_start_dt as start_date
  FROM DataWarehouse.silver.crm_prd_info pn
  -- lets stay with currentdata filter out the historical data
  --null for product end date means current infor
  left join silver.erp_px_cat_g1v2 pc
  on pn.cat_id =pc.id
  where prd_end_dt is null ;-- so we can take off the prd_end_dt coulumn

  ---========================================
  --=========================================
  
/*
use dimentions surrogate keys instead of ids to easily
connect facts with dimentions
by doing so we have the two keys(surrogate) from the dim tables 
to connect the fact table witht the dimention
--****important inorder to built the fact table you have to put
the surragate key in to the fackt
*/
/*
then lets give friendly names abd sort the columns into logical groups
to improve readability
*/
/*
finally lets buid the view*/
create view gold.fact_sales as
SELECT sd.sls_ord_num as order_number
      ,pr.product_key 
      ,cu.customer_key
      ,sd.sls_order_dt
      ,sd.sls_ship_dt as order_date
      ,sd.sls_due_dt as shipping_date
      ,sd.sls_sales as due_date
      ,sd.sls_quantity as sales_amount
      ,sd.sls_price as quantity
      ,sd.dwh_created_date
  FROM DataWarehouse.silver.crm_sales_details sd
  left join gold.dim_products pr
  on sd.sls_prd_key= pr.product_number
  left join gold.dim_customers cu
  on sd.sls_cust_id= cu.customer_id;

  --=========================================
  --======================================

