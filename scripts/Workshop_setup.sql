/*
  ____ _____ _____ ____    _       ____       _     _____            _                                      _    
 / ___|_   _| ____|  _ \  / |  _  / ___|  ___| |_  | ____|_ ____   _(_)_ __ ___  _ __  _ __ ___   ___ _ __ | |_  
 \___ \ | | |  _| | |_) | | | (_) \___ \ / _ \ __| |  _| | '_ \ \ / / | '__/ _ \| '_ \| '_ ` _ \ / _ \ '_ \| __| 
  ___) || | | |___|  __/  | |  _   ___) |  __/ |_  | |___| | | \ V /| | | | (_) | | | | | | | | |  __/ | | | |_  
 |____/ |_| |_____|_|     |_| (_) |____/ \___|\__| |_____|_| |_|\_/ |_|_|  \___/|_| |_|_| |_| |_|\___|_| |_|\__| 
                                                                                                                 
*/                                                                                                                 

-- Create the workshop database and schema
CREATE DATABASE IF NOT EXISTS TRUCK_WARRANTY_WORKSHOP;  --Change if needed
CREATE SCHEMA IF NOT EXISTS TRUCK_WARRANTY_WORKSHOP.ANALYTICS;  --Change if needed
USE DATABASE TRUCK_WARRANTY_WORKSHOP;  --Change if needed
USE SCHEMA ANALYTICS;  --Change if needed

-- Create compute warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH --Change if needed
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;  --Change if needed

-- Create stage for CSV data files with directory enabled and encryption
CREATE OR REPLACE STAGE DATA_FILES
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Create stage for PDF documents with directory enabled and encryption
CREATE OR REPLACE STAGE DOCUMENTS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Verify stages created
SHOW STAGES;

/*
  ____ _____ _____ ____    ____        __  __       _          _____     _     _            
 / ___|_   _| ____|  _ \  |___ \   _  |  \/  | __ _| | _____  |_   _|_ _| |__ | | ___  ___  
 \___ \ | | |  _| | |_) |   __) | (_) | |\/| |/ _` | |/ / _ \   | |/ _` | '_ \| |/ _ \/ __| 
  ___) || | | |___|  __/   / __/   _  | |  | | (_| |   <  __/   | | (_| | |_) | |  __/\__ \ 
 |____/ |_| |_____|_|     |_____| (_) |_|  |_|\__,_|_|\_\___|   |_|\__,_|_.__/|_|\___||___/ 
                                                                                            
*/

-- Dimension: Dealers (50 locations)
CREATE OR REPLACE TABLE DEALERS (
    DEALER_ID VARCHAR(20) PRIMARY KEY,
    DEALER_NAME VARCHAR(200),
    REGION VARCHAR(50)
);

-- Dimension: Vehicles (5,000 trucks across 5 models)
CREATE OR REPLACE TABLE VEHICLES (
    CHASSIS_NUMBER VARCHAR(20) PRIMARY KEY,
    MODEL VARCHAR(20),
    YEAR INTEGER,
    ENGINE_TYPE VARCHAR(50),
    MSRP DECIMAL(10,2),
    RELIABILITY_RATING INTEGER
);

-- Dimension: Technicians (200 service techs)
CREATE OR REPLACE TABLE TECHNICIANS (
    TECHNICIAN_ID VARCHAR(20) PRIMARY KEY,
    TECHNICIAN_NAME VARCHAR(100),
    CURRENT_DEALER_ID VARCHAR(20),
    HIRE_DATE DATE,
    SPECIALIZATION VARCHAR(100),
    CERTIFICATION_LEVEL VARCHAR(50),
    YEARS_EXPERIENCE INTEGER,
    PERFORMANCE_RATING INTEGER,
    FOREIGN KEY (CURRENT_DEALER_ID) REFERENCES DEALERS(DEALER_ID)
);

-- Fact: Sales (5,000 vehicle sales)
CREATE OR REPLACE TABLE SALES (
    CHASSIS_NUMBER VARCHAR(20),
    SALES_DEALER_ID VARCHAR(20),
    SALE_DATE DATE,
    SALE_PRICE DECIMAL(10,2),
    FOREIGN KEY (CHASSIS_NUMBER) REFERENCES VEHICLES(CHASSIS_NUMBER),
    FOREIGN KEY (SALES_DEALER_ID) REFERENCES DEALERS(DEALER_ID)
);

-- Fact: Service Records (~25,000 service events - warranty, maintenance, repair)
CREATE OR REPLACE TABLE SERVICE (
    SERVICE_ID INTEGER PRIMARY KEY,
    CHASSIS_NUMBER VARCHAR(20),
    SERVICE_DEALER_ID VARCHAR(20),
    TECHNICIAN_ID VARCHAR(20),
    SERVICE_DATE DATE,
    SERVICE_TYPE VARCHAR(20),
    SERVICE_AMOUNT DECIMAL(10,2),
    PARTS_REPLACED VARCHAR(500),
    TECHNICIAN_NOTES TEXT,
    FAULT_CODE VARCHAR(20),
    CLAIM_STATUS VARCHAR(20),
    WARRANTY_TYPE VARCHAR(50),
    CUSTOMER_PAY CHAR(1),
    MILEAGE INTEGER,
    FOREIGN KEY (CHASSIS_NUMBER) REFERENCES VEHICLES(CHASSIS_NUMBER),
    FOREIGN KEY (SERVICE_DEALER_ID) REFERENCES DEALERS(DEALER_ID),
    FOREIGN KEY (TECHNICIAN_ID) REFERENCES TECHNICIANS(TECHNICIAN_ID)
);

/*
  ____ _____ _____ ____    _____       _                    _   _____ _ _           
 / ___|_   _| ____|  _ \  |___ /   _  | |    ___   __ _  __| | |  ___(_) | ___  ___ 
 \___ \ | | |  _| | |_) |   |_ \  (_) | |   / _ \ / _` |/ _` | | |_  | | |/ _ \/ __|
  ___) || | | |___|  __/   ___) |  _  | |__| (_) | (_| | (_| | |  _| | | |  __/\__ \
 |____/ |_| |_____|_|     |____/  (_) |_____\___/ \__,_|\__,_| |_|   |_|_|\___||___/
                                                                                    
*/

--Load .csv files into DATA_FILES Stage
--Load .pdf files into DOCUMENTS Stage

LIST @DATA_FILES;
LIST @DOCUMENTS;

/*
  ____ _____ _____ ____    _  _         _                    _   _____     _     _           
 / ___|_   _| ____|  _ \  | || |    _  | |    ___   __ _  __| | |_   _|_ _| |__ | | ___  ___ 
 \___ \ | | |  _| | |_) | | || |_  (_) | |   / _ \ / _` |/ _` |   | |/ _` | '_ \| |/ _ \/ __|
  ___) || | | |___|  __/  |__   _|  _  | |__| (_) | (_| | (_| |   | | (_| | |_) | |  __/\__ \
 |____/ |_| |_____|_|        |_|   (_) |_____\___/ \__,_|\__,_|   |_|\__,_|_.__/|_|\___||___/
                                                                                             
*/

-- Load dimension tables
COPY INTO DEALERS
FROM @DATA_FILES/dealers.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO VEHICLES
FROM @DATA_FILES/vehicles.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO TECHNICIANS
FROM @DATA_FILES/technicians.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load fact tables
COPY INTO SALES
FROM @DATA_FILES/sales.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO SERVICE
FROM @DATA_FILES/service.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

/*
  ____ _____ _____ ____    ____       __     __        _  __         ____        _        
 / ___|_   _| ____|  _ \  | ___|   _  \ \   / /__ _ __(_)/ _|_   _  |  _ \  __ _| |_ __ _ 
 \___ \ | | |  _| | |_) | |___ \  (_)  \ \ / / _ \ '__| | |_| | | | | | | |/ _` | __/ _` |
  ___) || | | |___|  __/   ___) |  _    \ V /  __/ |  | |  _| |_| | | |_| | (_| | || (_| |
 |____/ |_| |_____|_|     |____/  (_)    \_/ \___|_|  |_|_|  \__, | |____/ \__,_|\__\__,_|
                                                             |___/                        
*/

-- Check row counts
SELECT 'DEALERS' as TABLE_NAME, COUNT(*) as ROW_COUNT FROM DEALERS
UNION ALL
SELECT 'VEHICLES', COUNT(*) FROM VEHICLES
UNION ALL
SELECT 'TECHNICIANS', COUNT(*) FROM TECHNICIANS
UNION ALL
SELECT 'SALES', COUNT(*) FROM SALES
UNION ALL
SELECT 'SERVICE', COUNT(*) FROM SERVICE;

--EXPECTED RESULTS:
--Dealers: 50
--Vehicles:  5000
--Technicians: 200
--Sales:  5000
--Service:  24560

-- Check warranty revenue percentages by dealer
WITH sales_by_dealer AS (
    SELECT 
        SALES_DEALER_ID,
        SUM(SALE_PRICE) as total_sales
    FROM SALES
    GROUP BY SALES_DEALER_ID
),
service_by_dealer AS (
    SELECT 
        SERVICE_DEALER_ID,
        SUM(CASE WHEN SERVICE_TYPE = 'WARRANTY' THEN SERVICE_AMOUNT ELSE 0 END) as warranty_revenue,
        SUM(CASE WHEN SERVICE_TYPE = 'MAINTENANCE' THEN SERVICE_AMOUNT ELSE 0 END) as maintenance_revenue,
        SUM(CASE WHEN SERVICE_TYPE = 'REPAIR' THEN SERVICE_AMOUNT ELSE 0 END) as repair_revenue
    FROM SERVICE
    GROUP BY SERVICE_DEALER_ID
)
SELECT 
    d.DEALER_NAME,
    d.REGION,
    COALESCE(s.total_sales, 0) as sales_revenue,
    COALESCE(sv.warranty_revenue, 0) as warranty_revenue,
    COALESCE(sv.maintenance_revenue, 0) as maintenance_revenue,
    COALESCE(sv.repair_revenue, 0) as repair_revenue,
    (COALESCE(s.total_sales, 0) + COALESCE(sv.warranty_revenue, 0) + 
     COALESCE(sv.maintenance_revenue, 0) + COALESCE(sv.repair_revenue, 0)) as total_revenue,
    ROUND(COALESCE(sv.warranty_revenue, 0) / 
          NULLIF((COALESCE(s.total_sales, 0) + COALESCE(sv.warranty_revenue, 0) + 
                  COALESCE(sv.maintenance_revenue, 0) + COALESCE(sv.repair_revenue, 0)), 0) * 100, 2) 
          as warranty_pct,
    CASE 
        WHEN ROUND(COALESCE(sv.warranty_revenue, 0) / 
             NULLIF((COALESCE(s.total_sales, 0) + COALESCE(sv.warranty_revenue, 0) + 
                     COALESCE(sv.maintenance_revenue, 0) + COALESCE(sv.repair_revenue, 0)), 0) * 100, 2) >= 6.0 
        THEN '🚨 SUSPICIOUS'
        ELSE 'Normal'
    END as risk_flag
FROM DEALERS d
LEFT JOIN sales_by_dealer s ON d.DEALER_ID = s.SALES_DEALER_ID
LEFT JOIN service_by_dealer sv ON d.DEALER_ID = sv.SERVICE_DEALER_ID
WHERE (COALESCE(s.total_sales, 0) + COALESCE(sv.warranty_revenue, 0) + 
       COALESCE(sv.maintenance_revenue, 0) + COALESCE(sv.repair_revenue, 0)) > 0
ORDER BY warranty_pct DESC
LIMIT 10;

--EXPECTED RESULTS:
--Lewis Commercial Sales | 8.08 | SUSPICIOUS
--Anderson Truck Center | 6.83 | SUSPICIOUS
--(other dealers) | 2-4% | NORMAL
