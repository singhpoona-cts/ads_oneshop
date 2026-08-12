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

CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_Account_List
  PARTITION BY
    extraction_date
  OPTIONS (
    partition_expiration_days = 90)
AS
WITH
  -- Native Merchant API v1 omnichannel settings. The `liasettings` table
  -- is now FLAT -- one row per account with a repeated per-region
  -- `omnichannel_settings` list (the old {settings, children[]} envelope is gone;
  -- v1 lists settings per sub-/standalone account directly). Status strings became
  -- enum NAME strings ('active' -> 'ACTIVE'); hostedLocalStorefront + mHLSF folded
  -- into the single `lsf_type` enum.
  Lia AS (
    SELECT DISTINCT
      L.account_id AS merchant_id,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE
          in_stock.state = 'ACTIVE'
          AND inventory_verification.contact_state = 'ACTIVE'
          AND about.state = 'ACTIVE'
      ) AS lia_has_lia_implemented,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE lsf_type IN ('GHLSF', 'MHLSF_BASIC', 'MHLSF_FULL')
      ) AS lia_has_mhlsf_implemented,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE pickup.state = 'ACTIVE'
      ) AS lia_has_store_pickup_implemented,
      EXISTS(
        SELECT 1
        FROM L.omnichannel_settings
        WHERE odo.state = 'ACTIVE'
      ) AS lia_has_odo_implemented
    FROM ${PROJECT_NAME}.${DATASET_NAME}.liasettings AS L
  ),
  AllShippingData AS (
    -- Native Merchant API v1 flat per-account shipping settings. The
    -- old {settings, children[]} envelope is gone; each row is one account.
    SELECT
      account_id AS accountId,
      services
    FROM ${PROJECT_NAME}.${DATASET_NAME}.shippingsettings
  ),
  AccountLevelShipping AS (
    SELECT DISTINCT
      accountId AS merchant_id,
      ARRAY_LENGTH(services) > 0 AS has_account_level_shipping,
      EXISTS(
        SELECT *
        FROM SS.services
        WHERE
          delivery_time.max_transit_days IS NOT NULL
          AND delivery_time.min_transit_days IS NOT NULL
          AND delivery_time.min_handling_days IS NOT NULL
          AND delivery_time.max_handling_days IS NOT NULL
      ) AS has_account_level_shipping_speed,
      EXISTS(
        SELECT *
        FROM SS.services
        WHERE
          delivery_time.max_transit_days IS NOT NULL
          AND delivery_time.max_handling_days IS NOT NULL
          AND delivery_time.max_transit_days + delivery_time.max_handling_days <= 3
      ) AS has_account_level_fast_shipping,
      EXISTS(
        SELECT *
        FROM
          SS.services AS S,
          S.rate_groups AS RG,
          RG.main_table.rows AS RS,
          RS.cells AS C
        WHERE
          C.flat_rate.amount_micros = 0
      )
        OR EXISTS(
          SELECT *
          FROM
            SS.services AS S,
            S.rate_groups AS RG
          WHERE RG.single_value.flat_rate.amount_micros = 0
        ) AS has_account_level_free_shipping
    FROM AllShippingData AS SS
  ),
  EnabledDestinations AS (
    SELECT DISTINCT
      CAST(account_id AS INT64) AS merchant_id,
      EXISTS(
        SELECT 1
        FROM P.status.destination_statuses
        WHERE reporting_context = 'FREE_LISTINGS'
      ) AS has_free_listings_enabled,
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.products AS P,
      P.status.destination_statuses AS DS
  ),
  AllAccounts AS (
    -- Flat Merchant API v1 accounts: one row per (leaf) account. Advanced/MCA
    -- accounts are excluded as merchants (is_advanced); the parent account is
    -- self-joined for the aggregator name and to roll down account-level
    -- automatic-improvements settings to sub-accounts.
    SELECT
      A.account_id AS merchant_id,
      A.account_name AS merchant_name,
      IFNULL(A.parent_account, 0) AS aggregator_id,
      P.account_name AS aggregator_name,
      COALESCE(
        A.automatic_improvements.image_improvements.effective_allow_automatic_image_improvements,
        P.automatic_improvements.image_improvements.effective_allow_automatic_image_improvements,
        FALSE)
        AS has_image_aiu_enabled,
      COALESCE(
        (
          A.automatic_improvements.item_updates.effective_allow_strict_availability_updates
          OR A.automatic_improvements.item_updates.effective_allow_availability_updates),
        (
          P.automatic_improvements.item_updates.effective_allow_strict_availability_updates
          OR P.automatic_improvements.item_updates.effective_allow_availability_updates),
        FALSE)
        AS has_availability_aiu_enabled,
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A
    LEFT JOIN ${PROJECT_NAME}.${DATASET_NAME}.accounts AS P
      ON A.parent_account = P.account_id
    WHERE NOT A.is_advanced
  ),
  AccountNames AS (
    SELECT DISTINCT
      A.merchant_id,
      A.merchant_name,
      A.aggregator_id,
      A.aggregator_name,
      CONCAT(A.merchant_name, ' (', A.merchant_id, ')') AS merchant_name_with_id
    FROM AllAccounts AS A
  ),
  Account AS (
    SELECT DISTINCT
      A.merchant_id,
      A.aggregator_id,
      IFNULL(A.has_image_aiu_enabled, FALSE) AS has_image_aiu_enabled,
      IFNULL(A.has_availability_aiu_enabled, FALSE) AS has_availability_aiu_enabled,
      IFNULL(L.lia_has_lia_implemented, FALSE) AS lia_has_lia_implemented,
      IFNULL(L.lia_has_mhlsf_implemented, FALSE) AS lia_has_mhlsf_implemented,
      IFNULL(L.lia_has_store_pickup_implemented, FALSE) AS lia_has_store_pickup_implemented,
      IFNULL(L.lia_has_odo_implemented, FALSE) AS lia_has_odo_implemented,
      IFNULL(ALS.has_account_level_shipping, FALSE) AS has_account_level_shipping,
      IFNULL(ED.has_free_listings_enabled, FALSE) AS has_free_listings_enabled,
      IFNULL(ALS.has_account_level_shipping_speed, FALSE) AS has_account_level_shipping_speed,
      IFNULL(ALS.has_account_level_fast_shipping, FALSE) AS has_account_level_fast_shipping,
      IFNULL(ALS.has_account_level_free_shipping, FALSE) AS has_account_level_free_shipping,
    FROM
      AllAccounts AS A
    LEFT JOIN Lia AS L
      ON L.merchant_id = A.merchant_id
    LEFT JOIN AccountLevelShipping AS ALS
      ON ALS.merchant_id = A.merchant_id
    LEFT JOIN EnabledDestinations AS ED
      ON ED.merchant_id = A.merchant_id
  ),
  HasFreeListings AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'has Free Listings enabled' AS metric_name,
      'free listings not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_free_listings_enabled
  ),
  ImageAiu AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'image AIU enabled' AS metric_name,
      'image AIU not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_image_aiu_enabled
  ),
  AvailabilityAiu AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'availability AIU enabled' AS metric_name,
      'availability AIU not enabled' AS data_quality_flag,
    FROM Account
    WHERE NOT has_availability_aiu_enabled
  ),
  LiaImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'has LIA implemented' AS metric_name,
      'lia not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_lia_implemented
  ),
  MhlsfImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'LIA: has MHLSF implemented' AS metric_name,
      'mhlsf not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_mhlsf_implemented
  ),
  StorePickupImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'LIA: has Store Pickup implemented' AS metric_name,
      'store pickup not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_store_pickup_implemented
  ),
  OnDisplayToOrderImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'LIA: On display to order implemented' AS metric_name,
      'on display to order not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT lia_has_odo_implemented
  ),
  AccountLevelShippingImplemented AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'uses account-level shipping settings' AS metric_name,
      'account-level shipping not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT has_account_level_shipping
  ),
  AccountLevelShippingSpeed AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'uses account-level shipping speed' AS metric_name,
      'account-level shipping speed not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT has_account_level_shipping_speed
  ),
  AccountLevelFreeShipping AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'uses account-level free shipping' AS metric_name,
      'account-level free shipping not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT has_account_level_free_shipping
  ),
  AccountLevelFastShipping AS (
    SELECT DISTINCT
      merchant_id,
      aggregator_id,
      'uses account-level fast shipping' AS metric_name,
      'account-level fast shipping not implemented' AS data_quality_flag,
    FROM Account
    WHERE NOT has_account_level_fast_shipping
  ),
  AllMetrics AS (
    SELECT * FROM HasFreeListings
    UNION ALL
    SELECT * FROM ImageAiu
    UNION ALL
    SELECT * FROM AvailabilityAiu
    UNION ALL
    SELECT * FROM LiaImplemented
    UNION ALL
    SELECT * FROM MhlsfImplemented
    UNION ALL
    SELECT * FROM StorePickupImplemented
    UNION ALL
    SELECT * FROM AccountLevelShippingImplemented
    UNION ALL
    SELECT * FROM AccountLevelShippingSpeed
    UNION ALL
    SELECT * FROM AccountLevelFreeShipping
    UNION ALL
    SELECT * FROM AccountLevelFastShipping
    UNION ALL
    SELECT * FROM OnDisplayToOrderImplemented
  )
SELECT DISTINCT
  CURRENT_DATE('UTC') AS extraction_date,
  CAST(AM.merchant_id AS STRING) AS merchant_id,
  AN.merchant_name,
  CAST(AM.aggregator_id AS STRING) AS aggregator_id,
  AN.aggregator_name,
  AN.merchant_name_with_id,
  metric_name,
  data_quality_flag
FROM AllMetrics AS AM
LEFT JOIN AccountNames AS AN
  USING (merchant_id);
