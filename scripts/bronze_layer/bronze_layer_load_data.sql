===================================================================================================================================================
Цель: Загрузка данных из csv файлов в созданные таблицы  
===================================================================================================================================================
  Внутри блока кода создается Процедура, которую можно вызывать в любой части кода, в других скриптах, загрзука данных в каждую таблицу отделяется границами
  Также считается время загрузки каждой таблицы и всего батча в целом. Однако здесь данных немного поэтому время минимальное 
===================================================================================================================================================

CREATE OR REPLACE procedure bronze.load_bronze()
LANGUAGE plpgsql
AS $$
	DECLARE start_time TIMESTAMP;
	DECLARE end_time TIMESTAMP;
	DECLARE start_load_batch TIMESTAMP;
	DECLARE finish_load_batch TIMESTAMP;
BEGIN
	start_load_batch := CURRENT_TIMESTAMP;
	RAISE NOTICE '=====================================================================================================================';
	RAISE NOTICE 'Load Bronze layer';
	RAISE NOTICE '=====================================================================================================================';

	RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
	RAISE NOTICE 'TL crm_cust_info';
	RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncate crm_cust_info';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			truncate table bronze.crm_cust_info;
		start_time := CURRENT_TIMESTAMP;
			RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			RAISE NOTICE '<<< Loading crm_cust_info';
			RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			COPY bronze.crm_cust_info
			FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
			WITH (FORMAT csv, HEADER true);
		end_time := CURRENT_TIMESTAMP;
	END;
	RAISE NOTICE 'Время загрузки crm_cust_info: %', end_time - start_time;

	RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
	RAISE NOTICE 'TL crm_prd_info';
	RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';

	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncate crm_prd_info';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			truncate table bronze.crm_prd_info;
		start_time := CURRENT_TIMESTAMP;
			RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			RAISE NOTICE '<<< Loading crm_prd_info';
			RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			COPY bronze.crm_prd_info
			FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
			WITH (FORMAT csv, HEADER true);
		end_time := CURRENT_TIMESTAMP;
	END;
	RAISE NOTICE 'Время загрузки crm_prd_info: %', end_time - start_time;

	 
	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncate crm_sales_details_info';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';

		truncate table bronze.crm_sales_details_info;

		start_time := CURRENT_TIMESTAMP;
			RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			RAISE NOTICE '<<< Loading crm_sales_details_info';
			RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
			COPY bronze.crm_sales_details_info
			FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 
			WITH (FORMAT csv, HEADER true);
		end_time := CURRENT_TIMESTAMP;
	END;
	RAISE NOTICE 'Время загрузки crm_prd_info: %', end_time - start_time;
	
	
	 
	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncate erp_cust_az12';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';

		truncate table bronze.erp_cust_az12;

		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '<<< Loading erp_cust_az12';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE 'Load erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
		WITH (FORMAT csv, HEADER true);
	end_time := CURRENT_TIMESTAMP;
	END;
	RAISE NOTICE 'Время загрузки erp_cust_az12: %', end_time - start_time;
	
	 
	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncate erp_loc_a101';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		truncate table bronze.erp_loc_a101;
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '<<< Loading erp_loc_a101';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		COPY bronze.erp_loc_a101
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
		WITH (FORMAT csv, HEADER true);
	end_time := CURRENT_TIMESTAMP;
	END;
	RAISE NOTICE 'Время загрузки erp_loc_a101: %', end_time - start_time;
	
	 
	BEGIN
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '>>> Truncate erp_px_cat_g1v2';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';

		truncate table bronze.erp_px_cat_g1v2;

		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		RAISE NOTICE '<<< Loading erp_px_cat_g1v2';
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
		COPY bronze.erp_px_cat_g1v2
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
		WITH (FORMAT csv, HEADER true);
		RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
	end_time := CURRENT_TIMESTAMP;
	END;
	RAISE NOTICE 'Время загрузки erp_px_cat_g1v2: %', end_time - start_time;
	RAISE NOTICE '--------------------------------------------------------------------------------------------------------------------';
	finish_load_batch := CURRENT_TIMESTAMP;

	RAISE NOTICE 'Время загрузка всего батча составляет: %', finish_load_batch - start_load_batch;
END
$$;


