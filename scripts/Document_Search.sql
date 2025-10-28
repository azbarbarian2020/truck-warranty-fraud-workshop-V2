USE DATABASE TRUCK_WARRANTY_WORKSHOP;
USE SCHEMA ANALYTICS;

LIST @DOCUMENTS;

/*
  ____ _____ _____ ____    _       _____      _                  _     _____         _    
 / ___|_   _| ____|  _ \  / |  _  | ____|_  _| |_ _ __ __ _  ___| |_  |_   _|____  _| |_  
 \___ \ | | |  _| | |_) | | | (_) |  _| \ \/ / __| '__/ _` |/ __| __|   | |/ _ \ \/ / __| 
  ___) || | | |___|  __/  | |  _  | |___ >  <| |_| | | (_| | (__| |_    | |  __/>  <| |_  
 |____/ |_| |_____|_|     |_| (_) |_____/_/\_\\__|_|  \__,_|\___|\__|   |_|\___/_/\_\\__|                                                           */

-- Extract text from all PDF documents
CREATE OR REPLACE TABLE RAW_TEXT AS
SELECT 
    RELATIVE_PATH,
    CASE 
        WHEN RELATIVE_PATH LIKE '%WARRANTY_POLICY%' THEN 'WARRANTY_POLICY'
        ELSE 'SERVICE_GUIDE'
    END AS DOCUMENT_TYPE,
    CASE 
        WHEN RELATIVE_PATH LIKE '%WARRANTY_POLICY%' THEN 'Warranty Policy'
        ELSE REPLACE(REPLACE(RELATIVE_PATH, '_Service_Guide.pdf', ''), 'MODEL-', 'Model ') || ' Service Guide'
    END AS DOCUMENT_TITLE,
    CASE 
        WHEN RELATIVE_PATH LIKE '%T2000%' THEN 'T2000'
        WHEN RELATIVE_PATH LIKE '%T4000%' THEN 'T4000'
        WHEN RELATIVE_PATH LIKE '%T6000%' THEN 'T6000'
        WHEN RELATIVE_PATH LIKE '%T8000%' THEN 'T8000'
        WHEN RELATIVE_PATH LIKE '%T9000%' THEN 'T9000'
        ELSE NULL
    END AS MODEL_NUMBER,
    TO_VARCHAR(
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
            '@TRUCK_WARRANTY_WORKSHOP.ANALYTICS.DOCUMENTS',
            RELATIVE_PATH,
            {'mode': 'LAYOUT'}
        ):content
    ) AS EXTRACTED_TEXT
FROM DIRECTORY(@TRUCK_WARRANTY_WORKSHOP.ANALYTICS.DOCUMENTS)
WHERE RELATIVE_PATH LIKE '%.pdf';


--Verify RAW_TEXT table populated
SELECT 
    DOCUMENT_TYPE,
    DOCUMENT_TITLE,
    MODEL_NUMBER,
    LENGTH(EXTRACTED_TEXT) as text_length
FROM RAW_TEXT
ORDER BY DOCUMENT_TYPE, MODEL_NUMBER;

/*
  ____ _____ _____ ____    ____         ____ _                 _      _____         _    
 / ___|_   _| ____|  _ \  |___ \   _   / ___| |__  _   _ _ __ | | __ |_   _|____  _| |_  
 \___ \ | | |  _| | |_) |   __) | (_) | |   | '_ \| | | | '_ \| |/ /   | |/ _ \ \/ / __| 
  ___) || | | |___|  __/   / __/   _  | |___| | | | |_| | | | |   <    | |  __/>  <| |_  
 |____/ |_| |_____|_|     |_____| (_)  \____|_| |_|\__,_|_| |_|_|\_\   |_|\___/_/\_\\__| 
                                                                                         
*/

CREATE OR REPLACE TABLE CHUNKED_TEXT AS
SELECT
    RELATIVE_PATH,
    DOCUMENT_TYPE,
    DOCUMENT_TITLE,
    MODEL_NUMBER,
    c.INDEX::INTEGER AS CHUNK_INDEX,
    c.VALUE::TEXT AS CHUNK_TEXT
FROM RAW_TEXT,
LATERAL FLATTEN(
    input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        EXTRACTED_TEXT,
        'markdown',
        1500,  -- chunk size
        200    -- overlap for context
    )
) c;

--Verify CHUNKED_TEXT table is populated
select * from chunked_text;

/*
  ____ _____ _____ ____    _____        ____                _         ____                      _       ____                  _           
 / ___|_   _| ____|  _ \  |___ /   _   / ___|_ __ ___  __ _| |_ ___  / ___|  ___  __ _ _ __ ___| |__   / ___|  ___ _ ____   _(_) ___ ___  
 \___ \ | | |  _| | |_) |   |_ \  (_) | |   | '__/ _ \/ _` | __/ _ \ \___ \ / _ \/ _` | '__/ __| '_ \  \___ \ / _ \ '__\ \ / / |/ __/ _ \ 
  ___) || | | |___|  __/   ___) |  _  | |___| | |  __/ (_| | ||  __/  ___) |  __/ (_| | | | (__| | | |  ___) |  __/ |   \ V /| | (_|  __/ 
 |____/ |_| |_____|_|     |____/  (_)  \____|_|  \___|\__,_|\__\___| |____/ \___|\__,_|_|  \___|_| |_| |____/ \___|_|    \_/ |_|\___\___| 
                                                                                                                                          
*/

CREATE OR REPLACE CORTEX SEARCH SERVICE DOCUMENTATION_SEARCH
ON CHUNK_TEXT
ATTRIBUTES RELATIVE_PATH, DOCUMENT_TYPE, DOCUMENT_TITLE, CHUNK_INDEX, MODEL_NUMBER
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT
        CHUNK_TEXT,
        RELATIVE_PATH,
        DOCUMENT_TYPE,
        DOCUMENT_TITLE,
        CHUNK_INDEX,
        MODEL_NUMBER
    FROM CHUNKED_TEXT
);

-- Test 1: Search for P0420 diagnostic procedure
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'TRUCK_WARRANTY_WORKSHOP.ANALYTICS.DOCUMENTATION_SEARCH',
        '{
            "query": "P0420 diagnostic procedure oxygen sensor test",
            "columns": ["chunk_text", "document_title", "model_number"],
            "limit": 3
        }'
    )
)['results'] as results;
