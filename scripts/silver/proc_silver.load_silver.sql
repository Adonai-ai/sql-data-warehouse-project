/*
=============================
stored procedure: load silver layer(bronze->silver)

if needed more description ----
===============================
*/

create or alter procedure silver.load_silver as
Begin  --NB at in a print all nessesary messages at ech query as we did for the
       -- bronze layer like total time for individual and the whole stprocedure
		-- and logginf information as far as you can

		--lets do the nessesary data cleansing and then insert into the silver
		--such as removing duplicate trim spaces make readable abrevationas
		print'>>truncating table: silver.crm_cust_info'
		truncate table silver.crm_cust_info --this is to avoid duplicate
		print'>>inserting data into: silver.crm_cust_info'
		insert into silver.crm_cust_info (  [cst_id]
			  ,[cst_key]
			  ,[cst_firstname]
			  ,[cst_lastname]
			  ,[cst_material_status]
			  ,[cst_gndr]
			  ,[cst_create_data]
			  )
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,--this isto remove empty spacess
		trim(cst_lastname) as cst_firstname,--this isto remove empty spacess
		case when upper(trim(cst_material_status)) ='S' then 'single'
			when upper(trim(cst_material_status)) ='M' then 'married'
			else 'n/a'
			end cst_marital_status,--narmalize marital status values to readable format
		case when upper(trim(cst_gndr)) ='F' then 'Female'--remove space and make it full
			when upper(trim(cst_gndr)) ='M' then 'Male'
			else 'n/a'
			end cst_gndr, --Normalize gender values to readable format
			cst_create_data
			from (
			select*,
			 row_number() over (partition by cst_id order by cst_create_data desc) as flag_last
			 from bronze.crm_cust_info
			 where cst_id is  not null) t
			 where flag_last=1; --select avoid duplicates

		  --=======================================================
		   --=======================================================
		print'>>truncating table: silver.crm_prd_info'
		truncate table silver.crm_cust_info --this is to avoid duplicate
		print'>>inserting data into: silver.crm_prd_info'
		insert into DataWarehouse.silver.crm_prd_info (
			  [prd_id]
			  ,[cat_id]
			  ,[prd_key]
			  ,[prd_nm]
			  ,[prd_cost]
			  ,[prd_line]
			  ,[prd_start_dt]
			  ,[prd_end_dt]
			  ) (
		SELECT prd_id
			  ,replace(substring(prd_key,1,5), '-','_') as cat_id--seperate the composite colums
			  ,substring(prd_key,7,len(prd_key)) as prd_key--separate composit key
			  ,prd_nm
			  ,isnull(prd_cost,0) as prd_cost --check price is not negative or null
			  , case when upper(trim(prd_line))='M' then 'Mountain'
					 when upper(trim(prd_line))='R' then 'Road'
					 when upper(trim(prd_line))='S' then 'Other Sales'
					 when upper(trim(prd_line))='T' then 'Touring'
					 else 'n/a'---here we are tring to have meaning full value as part of data cleansing
					 end as prd_line
			  ,cast(prd_start_dt as date) prd_start_dt---lets check if start date is before or after end dat
			  --here we are replacing out product end date with the folowing  and cast both as date
			  ,cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		  FROM DataWarehouse.bronze.crm_prd_info)
  
		  --=======================================================
		   --=======================================================
		  print'>>truncating table: silver.crm_sales_details'
		truncate table silver.crm_cust_info --this is to avoid duplicate
		print'>>inserting data into: crm_sales_details'

		insert into silver.crm_sales_details(
			   [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,[sls_order_dt]
			  ,[sls_ship_dt]
			  ,[sls_due_dt]
			  ,[sls_sales]
			  ,[sls_quantity]
			  ,[sls_price]
			  ) (
		SELECT
			   sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,case when sls_order_dt =0 or len(sls_order_dt) !=8 then null
					else cast (cast(sls_order_dt as varchar) as date)
			  end as sls_order_dt -- always need to check the sale or order and ship date to be in their right place
					  ---shiping date have not to be earlier than either of them
			  ,case when sls_ship_dt =0 or len(sls_ship_dt) !=8 then null
					else cast (cast(sls_ship_dt as varchar) as date)
			  end as sls_ship_dt --same as above plus we need to check the size to 8 figures
			  ,case when sls_due_dt =0 or len(sls_due_dt) !=8 then null
					else cast (cast(sls_due_dt as varchar) as date)
			  end as  sls_due_dt
			  --sales have to be non negative,not null, probably not zero and it is quntitiy times price 
			  -- so this hve to be fulfilled as part of cleansing 
			  ---here we hve to do back and fourth between quantity price and sales
			  --so instead of straight sales the following 
			  , case when sls_sales is null or sls_sales<=0 or 
						   sls_sales!=sls_quantity*abs(sls_price)
						   then sls_quantity*abs(sls_price)
					  else sls_sales
				end as sls_sales
			  ,sls_quantity
			  --and for price if we have negative
			  ,case when sls_price is null or sls_price<= 0 
					   then sls_price / nullif(sls_quantity,0)
					   else sls_price
			   end as sls_price
      
		  FROM DataWarehouse.bronze.crm_sales_details)


		  --=======================================================
		   --=======================================================
		  print'>>truncating table: [bronze].[erp_cust_az12]'
		truncate table silver.crm_cust_info --this is to avoid duplicate
		print'>>inserting data into: [bronze].[erp_cust_az12]'

		  insert into  silver.erp_cust_az12(cid, bdate, gen)
		SELECT 
			 case when  cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
				else  cid
			end cid
			-- make sure we dont have bdate in the future or really beg like 200 years old
			-- lets do our cleaning 
			,case when bdate > getdate() then null
				   else bdate
			  end as bdate
			  --check possible values of gender have to be male female and not avilable
			  -- avoid abrivation and null
			  ,case when upper(trim(gen))in ('F','FEMALE') then 'Female' 
					when upper(trim(gen)) in ('M','MALE') then 'Male' 
					else 'n/a'
				end as gen
		  FROM [DataWarehouse].[bronze].[erp_cust_az12]


  
		  --=======================================================
		   --=======================================================
			print'>>truncating table: bronze.erp_loc_a101'
		truncate table silver.crm_cust_info --this is to avoid duplicate
		print'>>inserting data into: bronze.erp_loc_a101'

		insert into silver.erp_loc_a101 (cid, cntry)

		SELECT replace(cid,'-', '') cid,---we have to remove the (-) between cid
			  case when trim(cntry)='DE' then 'Germany'
				   when trim(cntry) in ('US', 'USA') then 'United States'
				   when trim(cntry) = '' or cntry is null  then 'n/a'
				   else trim(cntry)
			 end as cntry
		  FROM DataWarehouse.bronze.erp_loc_a101

		   --=======================================================
		   --=======================================================
			   print'>>truncating table: erp_px_cat_g1v2'
		truncate table silver.crm_cust_info --this is to avoid duplicate
		print'>>inserting data into: erp_px_cat_g1v2'
   
		   insert into silver.erp_px_cat_g1v2( id , cat, subcat, maintenance)

		SELECT  id
			  ,cat
			  ,subcat
			  ,maintenance
		  FROM DataWarehouse.bronze.erp_px_cat_g1v2
end
