/*
ddl scripts create silver tables
*/
if OBJECT_ID ('silver.crm_cust_info', 'U') is not null
drop table silver.crm_cust_info

Create table silver.crm_cust_info (
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_data DATE,
--lets add the next column inorder to track the date of our datawarehs
dwh_created_date datetime2 default getdate()
);

if OBJECT_ID ('silver.crm_prd_info', 'U') is not null
drop table silver.crm_prd_info
Create table silver.crm_prd_info (
 prd_id int,
 cat_id nvarchar(50),
 prd_key nvarchar(50),
 prd_nm nvarchar(50),
 prd_cost int,
 prd_line nvarchar(50),
 prd_start_dt datetime,
 prd_end_dt datetime,
 --lets add the next column inorder to track the date of our datawarehs
dwh_created_date datetime2 default getdate()
);


if OBJECT_ID ('silver.crm_sales_details', 'U') is not null
drop table silver.crm_sales_details

Create table silver.crm_sales_details (
 sls_ord_num nvarchar(50),
 sls_prd_key nvarchar(50),
 sls_cust_id int,
 sls_order_dt date,
 sls_ship_dt date,
 sls_due_dt date,
 sls_sales int,
 sls_quantity int,
 sls_price int,
 --lets add the next column inorder to track the date of our datawarehs
dwh_created_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_loc_a101', 'U') is not null
drop table silver.erp_loc_a101

create table silver.erp_loc_a101
(
cid nvarchar(50),
cntry nvarchar(50)
);

if OBJECT_ID ('silver.erp_cust_az12', 'U') is not null
drop table silver.erp_cust_az12
create table silver.erp_cust_az12 (
 cid nvarchar(50),
 bdate date,
 gen nvarchar(50),
 --lets add the next column inorder to track the date of our datawarehs
dwh_created_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') is not null
drop table silver.erp_px_cat_g1v2
create table silver.erp_px_cat_g1v2 (
 
  id nvarchar(50),
  cat nvarchar(50),
  subcat nvarchar(50),
  maintenance nvarchar(50),
  --lets add the next column inorder to track the date of our datawarehs
dwh_created_date datetime2 default getdate()
);

