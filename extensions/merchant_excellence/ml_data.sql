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

CREATE OR REPLACE TABLE ${PROJECT_NAME}.${DATASET_NAME}.MEX_ML_Data
  PARTITION BY
    extraction_date
  OPTIONS (
    partition_expiration_days = 60)
AS
WITH
  AccountNames AS (
    -- Flat Merchant API v1 accounts. The INNER JOIN to the parent restricts to
    -- sub-accounts of advanced/MCA accounts, matching the original behavior of
    -- cross-joining only `accounts.children`. C = sub-account, P = aggregator.
    SELECT DISTINCT
      C.account_id AS merchant_id,
      C.account_name AS merchant_name,
      P.account_id AS aggregator_id,
      P.account_name AS aggregator_name,
      CONCAT(C.account_name, ' (', CAST(C.account_id AS STRING), ')') AS merchant_name_with_id
    FROM ${PROJECT_NAME}.${DATASET_NAME}.accounts AS C
    JOIN ${PROJECT_NAME}.${DATASET_NAME}.accounts AS P
      ON C.parent_account = P.account_id
  ),
  -- Phase 3: native Merchant API v1 omnichannel settings. The `liasettings` table
  -- is now FLAT -- one row per (sub-/standalone) account with a repeated per-region
  -- `omnichannel_settings` list. The old children-only roll-down (`L.children`) is
  -- gone; the downstream `Account` CTE already INNER JOINs to the parent so only
  -- MCA sub-accounts survive. Status strings became enum NAME strings; the
  -- hostedLocalStorefront + mHLSF signals folded into the single `lsf_type` enum.
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
  AccountLevelShipping AS (
  SELECT DISTINCT
    S.account_id AS merchant_id,
    ARRAY_LENGTH(S.services) > 0 AS has_account_level_shipping
  FROM ${PROJECT_NAME}.${DATASET_NAME}.shippingsettings AS S
  ),
  Account AS (
    -- C = sub-account, P = aggregator/parent (INNER JOIN keeps MCA children only).
    SELECT DISTINCT
      C.account_id AS merchant_id,
      P.account_id AS aggregator_id,
      ALS.has_account_level_shipping,
      IFNULL(
        C.automatic_improvements.image_improvements.effective_allow_automatic_image_improvements,
        P.automatic_improvements.image_improvements.effective_allow_automatic_image_improvements)
        AS has_image_aiu_enabled,
      IFNULL(
        (
          C.automatic_improvements.item_updates.effective_allow_strict_availability_updates
          OR C.automatic_improvements.item_updates.effective_allow_availability_updates),
        (
          P.automatic_improvements.item_updates.effective_allow_strict_availability_updates
          OR P.automatic_improvements.item_updates.effective_allow_availability_updates))
        AS has_availability_aiu_enabled,
      L.lia_has_lia_implemented,
      L.lia_has_mhlsf_implemented,
      L.lia_has_store_pickup_implemented,
      L.lia_has_odo_implemented
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.accounts AS C
    JOIN ${PROJECT_NAME}.${DATASET_NAME}.accounts AS P
      ON C.parent_account = P.account_id
    LEFT JOIN AccountLevelShipping AS ALS
      ON ALS.merchant_id = C.account_id
    LEFT JOIN Lia AS L
      ON L.merchant_id = C.account_id
  ),
  AdsStats AS (
    SELECT
      P.segments.productMerchantId AS merchant_id,
      CONCAT(
        LOWER(P.segments.productChannel),
        ":",
        L.languageConstant.code,
        ":",
        P.segments.productFeedLabel,
        ":",
        P.segments.productItemId) AS product_id,
      SUM(P.metrics.impressions) AS impressions_last30days,
      SUM(P.metrics.clicks) AS clicks_last30days,
      SUM(P.metrics.costMicros) / 1e6 AS cost_last30days,
      SUM(P.metrics.conversionsValue) AS revenue_last30days,
      SUM(P.metrics.conversions) AS conversions_last30days,
    FROM ${PROJECT_NAME}.${DATASET_NAME}.performance AS P
    LEFT JOIN ${PROJECT_NAME}.${DATASET_NAME}.language AS L
      ON P.segments.productLanguage = L.languageConstant.resourceName
    GROUP BY
      merchant_id,
      product_id
  ),
  EnabledDestinations AS (
    SELECT DISTINCT
      account_id AS merchant_id,
      offer_id AS product_id,
      EXISTS(
        SELECT 1
        FROM P.status.destination_statuses
        WHERE reporting_context = 'FREE_LISTINGS'
      ) AS has_free_listings_enabled,
      EXISTS(
        SELECT 1
        FROM P.status.destination_statuses
        WHERE reporting_context = 'DEMAND_GEN_ADS'
      ) AS has_dynamic_remarketing_enabled,
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.products AS P,
      P.status.destination_statuses AS DS
  ),
  ProductStatus AS (
    SELECT
      account_id AS merchant_id,
      P.product.channel,
      offer_id AS product_id,
      P.status.item_level_issues,
      ARRAY(
        SELECT DISTINCT x
        FROM
          UNNEST(
            ARRAY_CONCAT(DS.approved_countries, DS.pending_countries, DS.disapproved_countries))
            AS x
      ) AS targeted_countries,
      DS AS destination_statuses,
      ED.has_free_listings_enabled,
      ED.has_dynamic_remarketing_enabled
    FROM ${PROJECT_NAME}.${DATASET_NAME}.products AS P
    LEFT JOIN P.status.destination_statuses AS DS
    INNER JOIN EnabledDestinations AS ED
      ON ED.product_id = P.offer_id
    WHERE DS.reporting_context = 'SHOPPING_ADS'
  ),
  ItemIssues AS (
    SELECT
      P.account_id AS merchant_id,
      P.offer_id AS product_id,
      country,
      ARRAY_AGG(DISTINCT ILI.description) AS item_issues
    FROM
      ${PROJECT_NAME}.${DATASET_NAME}.products AS P,
      P.status.item_level_issues AS ILI,
      ILI.applicable_countries AS country
    WHERE ILI.reporting_context = 'SHOPPING_ADS'
    GROUP BY
      merchant_id,
      product_id,
      country
  ),
  ProductStatusCountry AS (
    SELECT
      CAST(PS.merchant_id AS INT64) AS merchant_id,
      PS.channel,
      PS.product_id,
      targeted_country,
      EXISTS(
        SELECT 1
        FROM PS.destination_statuses.disapproved_countries AS disapproved_country
        WHERE disapproved_country = targeted_country
      ) AS is_disapproved,
      II.item_issues,
      PS.has_free_listings_enabled,
      PS.has_dynamic_remarketing_enabled
    FROM ProductStatus AS PS, PS.targeted_countries AS targeted_country
    LEFT JOIN ItemIssues AS II
      ON
        II.product_id = PS.product_id
        AND II.country = targeted_country
  ),
  Products AS (
    SELECT
      IFNULL(AC.aggregator_id, 0) AS aggregator_id,
      PSC.merchant_id,
      PSC.channel,
      PSC.product_id,
      PSC.targeted_country,
      PSC.is_disapproved,
      PSC.item_issues,
      PSC.has_free_listings_enabled,
      PSC.has_dynamic_remarketing_enabled,
      P.product.offer_id AS item_id,
      P.product.content_language AS language,
      IFNULL(P.product.product_attributes.brand, '') AS brand,
      IFNULL(P.product.product_attributes.custom_label_0, '') AS custom_label_0,
      IFNULL(P.product.product_attributes.custom_label_1, '') AS custom_label_1,
      IFNULL(P.product.product_attributes.custom_label_2, '') AS custom_label_2,
      IFNULL(P.product.product_attributes.custom_label_3, '') AS custom_label_3,
      IFNULL(P.product.product_attributes.custom_label_4, '') AS custom_label_4,
      IFNULL(SPLIT(P.product.product_attributes.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(0)], '')
        AS product_type_lvl1,
      IFNULL(SPLIT(P.product.product_attributes.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(1)], '')
        AS product_type_lvl2,
      IFNULL(SPLIT(P.product.product_attributes.product_types[SAFE_OFFSET(0)], ' > ')[SAFE_OFFSET(2)], '')
        AS product_type_lvl3,
      AC.has_account_level_shipping,
      AC.has_image_aiu_enabled,
      AC.has_availability_aiu_enabled,
      AC.lia_has_lia_implemented,
      AC.lia_has_mhlsf_implemented,
      AC.lia_has_store_pickup_implemented,
      AC.lia_has_odo_implemented,
      P.product.product_attributes.gtins[SAFE_ORDINAL(1)] AS gtin,
      P.product.product_attributes.description,
      P.product.product_attributes.title,
      P.product.product_attributes.color,
      P.product.product_attributes.age_group,
      P.product.product_attributes.gender,
      P.product.product_attributes.size,
      P.product.product_attributes.additional_image_links,
      P.product.product_attributes.sale_price,
      P.product.product_attributes.item_group_id,
      P.product.product_attributes.product_types,
      P.product.product_attributes.product_highlights,
      P.product.product_attributes.shipping,
      (P.has_shopping_targeting OR P.has_performance_max_targeting) AS has_targeting,
      IFNULL(AD.impressions_last30days, 0) AS impressions,
      IFNULL(AD.clicks_last30days, 0) AS clicks,
      IFNULL(AD.cost_last30days, 0) AS cost,
      IFNULL(AD.revenue_last30days, 0) AS revenue,
      IFNULL(AD.conversions_last30days, 0) AS conversions
    FROM ProductStatusCountry AS PSC
    INNER JOIN ${PROJECT_NAME}.${DATASET_NAME}.products AS P
      ON
        CAST(P.account_id AS INT64) = PSC.merchant_id
        AND P.offer_id = PSC.product_id
    LEFT JOIN AdsStats AS AD
      ON
       CAST(AD.merchant_id AS INT64) = CAST(PSC.merchant_id AS INT64)
        AND LOWER(AD.product_id) = LOWER(PSC.product_id)
    LEFT JOIN Account AS AC
      ON AC.merchant_id = PSC.merchant_id
  )

SELECT
  CURRENT_DATE() AS extraction_date,
  P.aggregator_id,
  AN.aggregator_name,
  P.merchant_id,
  AN.merchant_name,
  P.targeted_country,
  P.item_id,
  P.channel,
  P.language,
  P.product_type_lvl1,
  P.product_type_lvl2,
  P.product_type_lvl3,
  P.title,
  P.description,
  P.has_dynamic_remarketing_enabled AS has_dynamic_remarketing,
  P.has_free_listings_enabled AS has_free_listings,
  EXISTS(
    SELECT 1
    FROM UNNEST(P.item_issues) AS e
    WHERE e LIKE '%Automatic item updates active%'
  ) AS has_price_availability_aiu,
  ARRAY_LENGTH(P.shipping) > 0 AS has_item_level_shipping,
  P.has_account_level_shipping,
  P.has_image_aiu_enabled AS has_image_aiu,
  P.has_availability_aiu_enabled AS has_availability_aiu,
  (IFNULL(custom_label_0, '') != ''
    OR IFNULL(custom_label_1, '') != ''
    OR IFNULL(custom_label_2, '') != ''
    OR IFNULL(custom_label_3, '') != ''
    OR IFNULL(custom_label_4, '') != '') AS has_custom_label,
  LENGTH(P.item_group_id) > 0 AS has_item_group_id,
  EXISTS(
      SELECT 1
      FROM UNNEST(P.product_types) AS e
      WHERE ARRAY_LENGTH(SPLIT(e, ' > ')) > 2
    ) AS has_good_product_type,
  IFNULL(brand, '') != '' AS has_brand,
  P.gtin IS NOT NULL AS has_gtin,
  LENGTH(P.description) >= 500 AS has_500_description,
  LENGTH(P.title) >= 30 AS has_30_title,
  ARRAY_LENGTH(P.product_highlights) > 0 AS has_product_highlight,
  P.color IS NOT NULL AS has_color,
  P.age_group IS NOT NULL AS has_age_group,
  P.gender IS NOT NULL AS has_gender,
  IFNULL(P.size, '') != '' AS has_size,
  -- Accounts with no row in the (flat, native-v1) liasettings table get NULL from
  -- the LEFT JOIN; coalesce to FALSE to preserve the pre-migration explicit-False
  -- semantics ("not implemented") for these ML features.
  IFNULL(P.lia_has_mhlsf_implemented, FALSE) AS has_mhlsf_implemented,
  IFNULL(P.lia_has_store_pickup_implemented, FALSE) AS has_store_pickup_implemented,
  IFNULL(P.lia_has_odo_implemented, FALSE) AS has_odo_implemented,
  IFNULL(P.sale_price.amount_micros, 0) > 0 AS has_sale_price,
  ARRAY_LENGTH(P.additional_image_links) > 0 AS has_additional_images,
  P.impressions AS impressions_30days,
  P.clicks AS clicks_30days,
  P.cost AS cost_30days,
  P.revenue AS revenue_30days,
  P.conversions AS conversions_30days
FROM Products AS P
INNER JOIN
  AccountNames AS AN USING (merchant_id)
