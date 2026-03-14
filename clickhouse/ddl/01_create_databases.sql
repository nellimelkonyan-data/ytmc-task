CREATE DATABASE IF NOT EXISTS crm_raw COMMENT 'Bronze layer - raw ingested CRM data';

CREATE DATABASE IF NOT EXISTS crm_staging COMMENT 'Silver layer - cleaned and standardized data';

CREATE DATABASE IF NOT EXISTS crm_marts COMMENT 'Gold layer - analytics ready models';

CREATE DATABASE IF NOT EXISTS crm_internal COMMENT 'Utility';