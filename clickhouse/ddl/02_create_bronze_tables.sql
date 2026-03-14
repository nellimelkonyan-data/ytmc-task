-- Drop tables if they already exist
DROP TABLE IF EXISTS crm_raw.activity_types;
DROP TABLE IF EXISTS crm_raw.activity;
DROP TABLE IF EXISTS crm_raw.deal_changes;
DROP TABLE IF EXISTS crm_raw.fields;
DROP TABLE IF EXISTS crm_raw.stages;
DROP TABLE IF EXISTS crm_raw.users;

-- Bronze Layer (Raw CRM Data)
-- Database: crm_raw


-- --------------------------------
-- Activity Types
-- --------------------------------
CREATE TABLE IF NOT EXISTS crm_raw.activity_types
(
    id UInt32,
    name String,
    active String,
    type String
)
ENGINE = MergeTree
ORDER BY id;


-- --------------------------------
-- Activities
-- --------------------------------
CREATE TABLE IF NOT EXISTS crm_raw.activity
(
    activity_id UInt64,
    type String,
    assigned_to_user UInt32,
    deal_id UInt64,
    done UInt8,
    due_to DateTime
)
ENGINE = MergeTree
ORDER BY activity_id;


-- --------------------------------
-- Deal Changes
-- --------------------------------
CREATE TABLE IF NOT EXISTS crm_raw.deal_changes
(
    deal_id UInt64,
    change_time DateTime,
    changed_field_key String,
    new_value String
)
ENGINE = MergeTree
ORDER BY (deal_id, change_time);


-- --------------------------------
-- Fields
-- --------------------------------
CREATE TABLE IF NOT EXISTS crm_raw.fields
(
    id UInt32,
    field_key String,
    name String,
    field_value_options Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


-- --------------------------------
-- Stages
-- --------------------------------
CREATE TABLE IF NOT EXISTS crm_raw.stages
(
    stage_id UInt32,
    stage_name String
)
ENGINE = MergeTree
ORDER BY stage_id;


-- --------------------------------
-- Users
-- --------------------------------
CREATE TABLE IF NOT EXISTS crm_raw.users
(
    id UInt32,
    name String,
    email String,
    modified DateTime
)
ENGINE = MergeTree
ORDER BY id;