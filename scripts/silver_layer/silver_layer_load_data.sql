CREATE OR REPLACE procedure silver.load_silver()
LANGUAGE plpgsql AS $$	
BEGIN
	RAISE NOTICE '=====================================================================================';
	RAISE NOTICE '>>> Loading data to silver layer <<<';
	RAISE NOTICE '=====================================================================================';
	BEGIN
		RAISE NOTICE '------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncating table crm_cust_info <<<';
		truncate table silver.crm_cust_info;
		RAISE NOTICE '>>> Inserting into table crm_cust_info <<<';
		RAISE NOTICE '------------------------------------------------------------------------------------------';
		insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname,cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		select
				cst_id,
				cst_key,
				trim(cst_firstname) as cst_firstname, -- Убираем ненужные пробелы
				trim(cst_lastname) as cst_lastname,
				
				case when upper(trim(cst_marital_status)) = 'S' then 'Single' -- Стандартизируем данные
					when upper(trim(cst_marital_status)) = 'M' then 'Married'
					else 'n/a' -- Заполняем пропуски
				end cst_marital_status,
				
				case when upper(trim(cst_gndr)) = 'F' then 'Female'
					when upper(trim(cst_gndr)) = 'M' then 'Male'
					else 'n/a'
				end cst_gndr,
				
				cst_create_date
			from (
				select 
				*,
				row_number() over (partition by cst_id order by cst_create_date desc)
				from bronze.crm_cust_info) t where row_number = 1; -- Удаляем дубликаты первичных ключей
	
			RAISE NOTICE '>>> Inserting into table crm_cust_info success <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
		END;
		BEGIN
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			RAISE NOTICE '>>> Truncating table crm_prd_info <<<';
			truncate table silver.crm_prd_info;
			RAISE NOTICE '>>> Inserting into table crm_prd_info <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			insert into silver.crm_prd_info (
					prd_id, 
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_end_dt
				)
				select 
				prd_id,
				replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
				substring(prd_key, 7, length(prd_key)) as prd_key,
				prd_nm,
				coalesce(prd_cost, 0) as prd_cost,
				case when upper(trim(prd_line)) = 'M' then 'Mountain'
					when upper(trim(prd_line)) = 'R' then 'Roads'
					when upper(trim(prd_line)) = 'S' then 'Other sales'
					when upper(trim(prd_line)) = 'T' then 'Touring'
					else 'n/a'
				end as prd_line,
				prd_start_dt::DATE as prd_start_dt,
				lead(prd_start_dt::DATE) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
				from bronze.crm_prd_info;
			RAISE NOTICE '>>> Inserting into table crm_prd_info success <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
		END;
		BEGIN
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			RAISE NOTICE '>>> Truncating table crm_sales_details_info <<<';
			truncate table silver.crm_sales_details_info;
			RAISE NOTICE '>>> Inserting into table crm_sales_details_info <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';	
			insert into silver.crm_sales_details_info (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			select
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
			case when sls_order_dt = 0 or length(sls_order_dt::varchar) != 8 then null
				else (sls_order_dt::varchar)::DATE
				end as sls_order_dt,
			case when sls_ship_dt = 0 or length(sls_ship_dt::varchar) != 8 then null
				else (sls_ship_dt::varchar)::DATE
				end as sls_ship_dt,
			case when sls_due_dt = 0 or length(sls_due_dt::varchar) != 8 then null
				else (sls_due_dt::varchar)::DATE
				end as sls_due_dt,
			  case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
			  	then sls_quantity * abs(sls_price)
			  	else sls_sales
			  end as sls_sales,
				sls_quantity,
			case when sls_price is null or sls_price <= 0
			  	then sls_sales / nullif(sls_quantity, 0)
			  	else sls_price
			 end as sls_price
			from bronze.crm_sales_details_info;
			RAISE NOTICE '>>> Inserting into table crm_sales_details_info success <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
		END;
		BEGIN
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			RAISE NOTICE '>>> Truncating table erp_loc_a101 <<<';
			truncate table silver.erp_loc_a101;
			RAISE NOTICE '>>> Inserting into table erp_loc_a101 <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			insert into silver.erp_loc_a101 (cid, cntry)
				select 
				replace(cid, '-', '') as cid,
				case when trim(cntry) = 'DE' then 'Germany'
					when trim(cntry) in ('US', 'USA') then 'United States'
					when trim(cntry) = '' or cntry is null then 'n/a'
					else trim(cntry)
				end as cntry
				from bronze.erp_loc_a101;
			RAISE NOTICE '>>> Inserting into table erp_loc_a101 success <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
		END;
		BEGIN
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			RAISE NOTICE '>>> Truncating table erp_cust_az12 <<<';
			truncate table silver.erp_cust_az12;
			RAISE NOTICE '>>> Inserting into table erp_cust_az12 <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
				insert into silver.erp_cust_az12 (cid, bdate, gen)
				select
				case when cid like 'NAS%' then substring(cid, 4, length(cid))
					else cid
				end cid,
				case when bdate > CURRENT_TIMESTAMP then null
					else bdate
				end as bdate,
				case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
					when upper(trim(gen)) in ('M', 'MALE') then 'Male'
					else 'n/a'
				end as gen
				from bronze.erp_cust_az12;
			RAISE NOTICE '>>> Inserting into table erp_cust_az12 success <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
		END;
		BEGIN
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			RAISE NOTICE '>>> Truncating table erp_px_cat_g1v2 <<<';
			truncate table silver.erp_px_cat_g1v2;
			RAISE NOTICE '>>> Inserting into table erp_px_cat_g1v2 <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
			insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
				select 
					id,
					cat, 
					subcat,
					maintenance
				from bronze.erp_px_cat_g1v2;
			RAISE NOTICE '>>> Inserting into table erp_px_cat_g1v2 success <<<';
			RAISE NOTICE '------------------------------------------------------------------------------------------';
		END;
RAISE NOTICE '>>> Inserting into tables completed successful <<<';
END
$$;

call silver.load_silver();
