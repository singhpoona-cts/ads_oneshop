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
  ProductFeatures AS (
    SELECT
      CAST(account_id AS INT64) AS merchant_id,
      LOGICAL_OR(JSON_VALUE(TO_JSON(product.product_attributes), '$.ads_redirect') IS NOT NULL AND JSON_VALUE(TO_JSON(product.product_attributes), '$.ads_redirect') != '') AS has_ads_redirect,
      LOGICAL_OR(JSON_VALUE(TO_JSON(product.product_attributes), '$.video_link') IS NOT NULL AND JSON_VALUE(TO_JSON(product.product_attributes), '$.video_link') != '') AS has_valid_video_link,
      LOGICAL_OR(ARRAY_LENGTH(JSON_QUERY_ARRAY(TO_JSON(product.product_attributes), '$.loyalty_programs')) > 0) AS has_loyalty_member_pricing_activated
    FROM ${PROJECT_NAME}.${DATASET_NAME}.products
    GROUP BY 1
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
  AccountPrograms AS (
    SELECT
      CAST(account_id AS INT64) AS merchant_id,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('local-inventory-ads', 'local_inventory_ads')
        AND prog.state IN ('ENABLED', 'ELIGIBLE')
      ) AS is_lia_ready_or_started,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('local-inventory-ads', 'local_inventory_ads')
        AND prog.state = 'ENABLED'
      ) AS is_lia_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('promotions')
        AND prog.state = 'ENABLED'
      ) AS has_merchant_promotions_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('product-reviews', 'product_reviews')
        AND prog.state = 'ENABLED'
      ) AS has_product_reviews_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('checkout', 'shopping-actions', 'shopping_actions')
        AND prog.state = 'ENABLED'
      ) AS has_checkout_on_merchant_enabled,
      LOGICAL_OR(
        SPLIT(prog.name, '/')[SAFE_OFFSET(3)] IN ('checkout', 'shopping-actions', 'shopping_actions')
        AND (ARRAY_LENGTH(IFNULL(JSON_QUERY_ARRAY(TO_JSON(prog), '$.unmet_requirements'), [])) = 0 OR prog.state = 'ENABLED')
      ) AS has_cwcd_implemented
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS A
    LEFT JOIN UNNEST(A.programs) AS prog
    GROUP BY 1
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
      IFNULL(PF.has_ads_redirect, FALSE) AS has_ads_redirect,
      IFNULL(PF.has_valid_video_link, FALSE) AS has_valid_video_link,
      IFNULL(PF.has_loyalty_member_pricing_activated, FALSE) AS has_loyalty_member_pricing_activated,
      IFNULL(AP.is_lia_ready_or_started, FALSE) AS is_lia_ready_or_started,
      IFNULL(AP.is_lia_enabled, FALSE) AS is_lia_enabled,
      IFNULL(AP.has_merchant_promotions_enabled, FALSE) AS has_merchant_promotions_enabled,
      IFNULL(AP.has_product_reviews_enabled, FALSE) AS has_product_reviews_enabled,
      IFNULL(AP.has_checkout_on_merchant_enabled, FALSE) AS has_checkout_on_merchant_enabled,
      IFNULL(AP.has_cwcd_implemented, FALSE) AS has_cwcd_implemented
    FROM
      AllAccounts AS A
    LEFT JOIN Lia AS L
      ON L.merchant_id = A.merchant_id
    LEFT JOIN AccountLevelShipping AS ALS
      ON ALS.merchant_id = A.merchant_id
    LEFT JOIN EnabledDestinations AS ED
      ON ED.merchant_id = A.merchant_id
    LEFT JOIN ProductFeatures AS PF
      ON PF.merchant_id = A.merchant_id
    LEFT JOIN AccountPrograms AS AP
      ON AP.merchant_id = A.merchant_id
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
  HasAccountLevelShipping AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has account level shipping' AS metric_name,
      'no account level shipping' AS data_quality_flag
    FROM Account
    WHERE NOT has_account_level_shipping
  ),
  HasMcaMarketplaceStructure AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has MCA marketplace structure' AS metric_name,
      'no MCA marketplace structure' AS data_quality_flag
    FROM Account
    WHERE aggregator_id = 0
  ),
  HasAdsRedirect AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has ads_redirect' AS metric_name,
      'no ads_redirect' AS data_quality_flag
    FROM Account
    WHERE NOT has_ads_redirect
  ),
  HasValidVideoLink AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has valid video_link' AS metric_name,
      'no video_link' AS data_quality_flag
    FROM Account
    WHERE NOT has_valid_video_link
  ),
  HasLoyaltyPricing AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has loyalty member pricing activated' AS metric_name,
      'no loyalty member pricing' AS data_quality_flag
    FROM Account
    WHERE NOT has_loyalty_member_pricing_activated
  ),
  IsLiaReadyOrStarted AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'is LIA ready or started' AS metric_name,
      'LIA not ready or started' AS data_quality_flag
    FROM Account
    WHERE NOT is_lia_ready_or_started
  ),
  IsLiaEnabled AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'is LIA enabled' AS metric_name,
      'LIA not enabled' AS data_quality_flag
    FROM Account
    WHERE NOT is_lia_enabled
  ),
  HasMerchantPromotionsEnabled AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has merchant promotions enabled' AS metric_name,
      'merchant promotions not enabled' AS data_quality_flag
    FROM Account
    WHERE NOT has_merchant_promotions_enabled
  ),
  HasProductReviewsEnabled AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has product reviews enabled' AS metric_name,
      'product reviews not enabled' AS data_quality_flag
    FROM Account
    WHERE NOT has_product_reviews_enabled
  ),
  HasCheckoutOnMerchantEnabled AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has checkout on merchant enabled' AS metric_name,
      'checkout not enabled' AS data_quality_flag
    FROM Account
    WHERE NOT has_checkout_on_merchant_enabled
  ),
  HasCwcdImplemented AS (
    SELECT DISTINCT
      merchant_id, aggregator_id,
      'has CWCD implemented' AS metric_name,
      'CWCD not implemented' AS data_quality_flag
    FROM Account
    WHERE NOT has_cwcd_implemented
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
    SELECT * FROM AccountLevelShippingSpeed
    UNION ALL
    SELECT * FROM AccountLevelFreeShipping
    UNION ALL
    SELECT * FROM AccountLevelFastShipping
    UNION ALL
    SELECT * FROM OnDisplayToOrderImplemented
    UNION ALL
    SELECT * FROM HasAccountLevelShipping
    UNION ALL
    SELECT * FROM HasMcaMarketplaceStructure
    UNION ALL
    SELECT * FROM HasAdsRedirect
    UNION ALL
    SELECT * FROM HasValidVideoLink
    UNION ALL
    SELECT * FROM HasLoyaltyPricing
    UNION ALL
    SELECT * FROM IsLiaReadyOrStarted
    UNION ALL
    SELECT * FROM IsLiaEnabled
    UNION ALL
    SELECT * FROM HasMerchantPromotionsEnabled
    UNION ALL
    SELECT * FROM HasProductReviewsEnabled
    UNION ALL
    SELECT * FROM HasCheckoutOnMerchantEnabled
    UNION ALL
    SELECT * FROM HasCwcdImplemented
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
