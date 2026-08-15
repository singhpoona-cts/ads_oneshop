-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Native Merchant API v1 omnichannel settings. FLAT shape -- one row per
-- (sub-/standalone) account with a repeated per-region `omnichannel_settings`
-- list (the old {settings, children[]} envelope is gone). Mirrors the proto
-- `OmnichannelLiaSettings` (acit/api/v0/storage/schema.proto); the production
-- table is created from the generated `liasettings.schema`, so this fallback DDL
-- only needs to match column names/types.
CREATE TABLE IF NOT EXISTS ${PROJECT_NAME}.${DATASET_NAME}.liasettings (
    account_id INT64,
    omnichannel_settings ARRAY<
        STRUCT<
            name STRING,
            region_code STRING,
            lsf_type STRING,
            in_stock STRUCT<uri STRING, state STRING>,
            pickup STRUCT<uri STRING, state STRING>,
            lfp_link STRUCT<
                lfp_provider STRING,
                external_account_id STRING,
                state STRING
            >,
            odo STRUCT<uri STRING, state STRING>,
            about STRUCT<uri STRING, state STRING>,
            inventory_verification STRUCT<
                state STRING,
                contact STRING,
                contact_email STRING,
                contact_state STRING
            >
        >
    >
);

CREATE TABLE IF NOT EXISTS ${PROJECT_NAME}.${DATASET_NAME}.shippingsettings (
    account_id INT64,
    services ARRAY<
        STRUCT<
            service_name STRING,
            active BOOL,
            delivery_countries ARRAY<STRING>,
            currency_code STRING,
            shipment_type STRING,
            delivery_time STRUCT<
                min_transit_days INT64,
                max_transit_days INT64,
                min_handling_days INT64,
                max_handling_days INT64,
                cutoff_time STRUCT<hour INT64, minute INT64, time_zone STRING>,
                handling_business_day_config STRUCT<business_days ARRAY<STRING>>
            >,
            rate_groups ARRAY<
                STRUCT<
                    applicable_shipping_labels ARRAY<STRING>,
                    name STRING,
                    single_value STRUCT<
                        flat_rate STRUCT<amount_micros INT64, currency_code STRING>
                    >,
                    main_table STRUCT<
                        name STRING,
                        `rows` ARRAY<
                            STRUCT<
                                cells ARRAY<
                                    STRUCT<
                                        flat_rate STRUCT<amount_micros INT64, currency_code STRING>
                                    >
                                >
                            >
                        >,
                        row_headers STRUCT<
                            prices ARRAY<STRUCT<amount_micros INT64, currency_code STRING>>
                        >,
                        column_headers STRUCT<
                            prices ARRAY<STRUCT<amount_micros INT64, currency_code STRING>>
                        >
                    >
                >
            >
        >
    >
);