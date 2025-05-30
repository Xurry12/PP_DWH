DO $$
	BEGIN
	    -- Проверяем, существует ли таблица
	    IF EXISTS (SELECT 1 FROM information_schema.tables 
	               WHERE table_schema = 'silver' AND table_name = 'crm_cust_info') THEN
	        -- Удаляем таблицу, если она существует
	        DROP TABLE silver.crm_cust_info;
	    END IF;
	
	    -- Создаём новую таблицу
	    CREATE TABLE silver.crm_cust_info (
	        cst_id INT,
	        cst_key VARCHAR(50),
	        cst_firstname VARCHAR(50),
	        cst_lastname VARCHAR(50),
	        cst_marital_status VARCHAR(50),
	        cst_gndr VARCHAR(50),
	        cst_create_date DATE,
			dwh_create_date TIMESTAMP default CURRENT_TIMESTAMP
	    );
	END $$;
	
	do $$ 
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables
					WHERE table_schema='silver' and table_name='crm_prd_info') THEN
			DROP TABLE silver.crm_prd_info;
		END IF;
CREATE TABLE silver.crm_prd_info (
			prd_id INT,
			cat_id VARCHAR(50),
			prd_key VARCHAR(50),
			prd_nm VARCHAR(50),
			prd_cost INT,
			prd_line VARCHAR(50),
			prd_start_dt DATE,
			prd_end_dt DATE,
			dwh_create_date TIMESTAMP default CURRENT_TIMESTAMP
	);

END $$;
	
	
	do $$
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables
					WHERE table_schema='silver' and table_name='crm_sales_details_info') THEN
			DROP TABLE silver.crm_sales_details_info;
		END IF;
		
		create table silver.crm_sales_details_info (
			sls_ord_num VARCHAR(50),
			sls_prd_key VARCHAR(50),
			sls_cust_id INT,
			sls_order_dt DATE,
			sls_ship_dt DATE,
			sls_due_dt DATE,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT,
			dwh_create_date TIMESTAMP default CURRENT_TIMESTAMP
		);
	END $$;
	
	do $$ 
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables
					WHERE table_schema='silver' and table_name='erp_cust_az12') THEN
			DROP TABLE silver.erp_cust_az12;
		END IF;
		
		create table silver.erp_cust_az12 (
			CID VARCHAR(50),
			BDATE DATE,
			GEN VARCHAR(50),
			dwh_create_date TIMESTAMP default CURRENT_TIMESTAMP
		);
	END $$;
	
	DO $$ 
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables
					WHERE table_schema='silver' and table_name='erp_loc_a101') THEN
			DROP TABLE silver.erp_loc_a101;
		END IF;
		
		create table silver.erp_loc_a101 (
		CID VARCHAR(50),
		CNTRY VARCHAR(50),
		dwh_create_date TIMESTAMP default CURRENT_TIMESTAMP
	);
	end $$;
	
	do $$ 
	BEGIN
		IF EXISTS (SELECT 1 FROM information_schema.tables
					WHERE table_schema='silver' and table_name='erp_px_cat_g1v2') THEN
			DROP TABLE silver.erp_px_cat_g1v2;
		END IF;
		
		create table silver.erp_px_cat_g1v2 (
			ID VARCHAR(50),
			CAT VARCHAR(50),
			SUBCAT VARCHAR(50),
			MAINTENANCE VARCHAR(50),
			dwh_create_date TIMESTAMP default CURRENT_TIMESTAMP
	);
	end $$;
