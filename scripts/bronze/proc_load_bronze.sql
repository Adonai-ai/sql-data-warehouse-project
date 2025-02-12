/*
==============================================
this stored procedure: load bronze layer (source -> bronze)
==============================================
script purpoze:
        this stored procedure loads data into the 'bronze' schema from external csv files
it performa the following actions;
truncates the bronze tables before loading data 
uses the 'bulk insert' command to load data from csv files to bronze tables 
parameters: none 
this stored procedure does not accept any parameters or return andy values
usage  example: exec bronze.load_bronze
*/



create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime,
@batch_end_time datetime;
	begin try
	set @batch_start_time=GETDATE();
	--since we are encapsulating out query wit stored procedure lets add the following separator
	 print'========================================';
	 print'>>>>>Loading bronze crm layer';
	 print'========================================';
			--to avid duplicate it is good to add the following line
			set @start_time=getdate();
			truncate table bronze.crm_cust_info 
			--the above life saves as from having duplicates
			bulk insert bronze.crm_cust_info
			from  'C:\Users\19193\OneDrive\Desktop\SQL_tss\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with (
			firstrow =2,
			fieldterminator=',',
			tablock
			);
			set @end_time=getdate();
		    print'>>load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar)+ ' seconds';

			--to avid duplicate it is good to add the following line
			truncate table bronze.crm_prd_info 
			--the above life saves as from having duplicates
			bulk insert bronze.crm_prd_info
			from  'C:\Users\19193\OneDrive\Desktop\SQL_tss\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
			firstrow =2,
			fieldterminator=',',
			tablock
			);

			--to avid duplicate it is good to add the following line
			truncate table bronze.crm_sales_details 
			--the above life saves as from having duplicates
			bulk insert bronze.crm_sales_details
			from  'C:\Users\19193\OneDrive\Desktop\SQL_tss\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with (
			firstrow =2,
			fieldterminator=',',
			tablock
			);

		 print'========================================';
		 print'>>>Loading bronze ERP layer';
		 print'========================================';
				--to avid duplicate it is good to add the following line
			truncate table bronze.erp_loc_a101 
			--the above life saves as from having duplicates
			bulk insert bronze.erp_loc_a101 
			from  'C:\Users\19193\OneDrive\Desktop\SQL_tss\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			with (
			firstrow =2,
			fieldterminator=',',
			tablock
			);

			--to avid duplicate it is good to add the following line
			truncate table bronze.erp_cust_az12 
			--the above life saves as from having duplicates
			bulk insert bronze.erp_cust_az12 
			from  'C:\Users\19193\OneDrive\Desktop\SQL_tss\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			with (
			firstrow =2,
			fieldterminator=',',
			tablock
			);

			--to avid duplicate it is good to add the following line
			truncate table bronze.erp_px_cat_g1v2
			--the above life saves as from having duplicates
			bulk insert bronze.erp_px_cat_g1v2
			from  'C:\Users\19193\OneDrive\Desktop\SQL_tss\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			with (
			firstrow =2,
			fieldterminator=',',
			tablock
			);
			end try
		begin catch
				print '============================';
				print 'error encountered during loadign';
				print'error message '+ error_message();
				print'error message '+ cast(error_message() as nvarchar);
		end catch
		set @batch_end_time= GETDATE()
		 print'>>total load duration: ' + cast(datediff(second, @batch_start_time,@batch_end_time) as nvarchar)+ ' seconds';


end
