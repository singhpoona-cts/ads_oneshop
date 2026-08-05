# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Create base table files for Merchant Center products in Google Ads.

Aggregates the following data:

 - Merchant Center:
   - Account-level data, such as:
     - Links to Google Ads
     - Shipping settings
     - Local inventory settings
   - Products and their statuses within Merchant Center
 - Google Ads
   - All targeting settings for campaigns driven by Merchant Center feeds
   - The advertising performance of those products
"""

from absl import app
from absl import flags

import copy
import json

from typing import Any

from acit import performance_max
from acit import shopping
from acit import product
from acit.utils import METADATA_KEY
from acit.api.v0.storage import schema_pb2

from google.protobuf import json_format

import apache_beam as beam
from apache_beam import pipeline
from apache_beam.io import textio
from apache_beam.io import fileio
from apache_beam.options import pipeline_options
from apache_beam import pvalue

# Omit variable declaration so we can pickle __main__.
flags.DEFINE_string(
    'source_dir', '/tmp/acit/*', 'The root path for all source files.'
)

flags.DEFINE_string(
    'products_output', 'out.jsonlines', 'The file path to output products to'
)

flags.DEFINE_string(
    'liasettings_output',
    'liasettings.json',
    'The Local Inventory Ads settings output file.',
)

flags.DEFINE_string(
    'accounts_output',
    'accounts.jsonlines',
    'The Merchant Center accounts output file.',
)

flags.DEFINE_string(
    'shippingsettings_output',
    'shippingsettings.jsonlines',
    'The Merchant Center shipping settings output file.',
)


def _ReadGoogleAdsRows(description: str, path: str) -> beam.ParDo:
  """Simple textio wrapper, can be used to swap in Ads protos later."""
  return textio.ReadFromText(
      path
  ) | f'{description} to Google Ads Row' >> beam.Map(json.loads)


def combine_campaign_settings(
    campaign_settings: pvalue.PCollection,
    languages_by_campaign_id: pvalue.PCollection,
    listing_scopes_by_campaign_id: pvalue.PCollection,
) -> pvalue.PCollection:
  """Creates a single record.

  For each campaign with its targeted languages and listing scopes.

  Args:
    campaign_settings: The PCollection of all campaign settings.
    languages_by_campaign_id: The PTable of campaign ID and language
      targeting information.
    listing_scopes_by_campaign_id: The PTable of campaign ID and (at most
      one) root listing scope.

  Returns:
    A combined campaign settings PCollection.
  """

  # TODO: https://github.com/apache/beam/issues/20825
  # Remove pyright ignore annotation.
  return (  # pyright: ignore [reportReturnType]
      {
          'campaigns':
          campaign_settings | beam.Map(lambda c: (c['campaign']['id'], c)),
          'languages':
          languages_by_campaign_id,
          'inventory_filter_dimensions':
          listing_scopes_by_campaign_id,
      }
      | beam.CoGroupByKey()
      | beam.Filter(lambda kv: len(kv[1]['campaigns']) > 0)
      | beam.FlatMapTuple(lambda _, v: product.build_campaign(
          # Must only be one
          v['campaigns'][0],
          # Array
          v['languages'],
          # At-most one
          next(iter(v['inventory_filter_dimensions']), []),
      )))


# Flags after `--` can get passed directly to Beam
def main(argv):
  opts = pipeline_options.PipelineOptions(argv[1:])
  opts.view_as(pipeline_options.SetupOptions).save_main_session = True

  source_dir = flags.FLAGS.source_dir

  with pipeline.Pipeline(options=opts) as p:
    # Ads Data
    asset_group_listing_filters = (
        p
        | 'Read Asset Group Listing Filters' >> _ReadGoogleAdsRows(
            'Asset Group Listing Filters',
            f'{source_dir}/ads/*/asset_group_listing_filter/*.jsonlines',
        ))

    campaign_settings = p | 'Read Campaign Settings' >> _ReadGoogleAdsRows(
        'Campaign Settings',
        f'{source_dir}/ads/*/campaign/*.jsonlines',
    )

    campaign_criteria = p | 'Read Campaign Criteria' >> _ReadGoogleAdsRows(
        'Campaign Criteria',
        f'{source_dir}/ads/*/campaign_criterion/*.jsonlines',
    )

    ad_group_criteria = p | 'Read Ad Group Criteria' >> _ReadGoogleAdsRows(
        'Ad Group Criteria',
        f'{source_dir}/ads/*/ad_group_criterion/*.jsonlines',
    )

    category_names_by_id = (
        p
        | 'Read Product Categories' >> _ReadGoogleAdsRows(
            'Product Categories',
            f'{source_dir}/ads/*/product_category/*.jsonlines',
        )
        | 'Create Category Mapping' >> beam.Map(lambda row: (
            row['productCategoryConstant']['categoryId'],
            next(
                iter([
                    localization['value']
                    for localization in row['productCategoryConstant'][
                        'localizations'] if localization['regionCode'] ==
                    'US' and localization['languageCode'] == 'en'
                ])),
        )))

    language_codes_by_resource_name = (
        p
        | 'Read Language Codes' >> _ReadGoogleAdsRows(
            'Language Codes',
            f'{source_dir}/ads/*/language_constant/*.jsonlines',
        )
        | 'Create Language Mapping' >> beam.Map(lambda row: (
            row['languageConstant']['resourceName'],
            row['languageConstant']['code'],
        )))

    # Merchant Center data.
    #
    # As of the Merchant API migration for products, each line is a native v1
    # `Product` that already embeds its status (`product_status`).
    # There is no longer a separate `productstatuses` collection / join.
    products = (p
                | 'Read Products' >> textio.ReadFromText(
                    f'{source_dir}/merchant_center/*/products/*.jsonlines')
                | 'Products to JSON' >> beam.Map(json.loads))

    def convert_lia_settings(row):
      # Native Merchant API v1 omnichannel settings.
      # `merchant_lia` writes one FLAT record per account --
      #   {"account_id": <int>, "omnichannel_settings":
      # [<OmnichannelSetting>, ...]}
      # -- so there is no longer a {settings, children[]}
      # envelope to disambiguate.`ignore_unknown_fields` drops the stamped
      # `downloaderMetadata` (and any
      # not-yet-modeled v1 attribute) instead of failing the parse.
      msg = schema_pb2.OmnichannelLiaSettings()
      json_format.ParseDict(row, msg, ignore_unknown_fields=True)
      return json_format.MessageToDict(
          msg,
          always_print_fields_with_no_presence=True,
          preserving_proto_field_name=True,
      )

    def convert_accounts(row):
      """Parses one aggregated v1 account into the `accounts` table shape.

      `merchant_accounts.download_accounts` writes the native v1 shape; the row
      layout is defined once as `schema_pb2.Account`, which also generates the
      BigQuery schema. Parsing here rather than in the downloader keeps
      `schema_pb2` out of the process that loads the Merchant API GAPIC -- both
      declare package `google.shopping.merchant.accounts.v1`, so co-loading them
      makes protobuf register conflicting descriptors for the same symbols.

      `ignore_unknown_fields` drops any not-yet-modeled v1 attribute instead of
      failing the parse.

      Args:
        row: A native-shape account dict, as read from
          `merchant_center/*/accounts/*.jsonlines`.

      Returns:
        A dict matching the generated BigQuery `accounts` schema.
      """
      msg = schema_pb2.Account()
      json_format.ParseDict(row, msg, ignore_unknown_fields=True)
      return json_format.MessageToDict(
          msg,
          always_print_fields_with_no_presence=True,
          preserving_proto_field_name=True,
      )

    # Process accounts
    _ = (
        p
        | 'Glob Accounts files' >> fileio.MatchFiles(
            file_pattern=(
                f'{source_dir}/merchant_center/*/accounts/*.jsonlines'),
            empty_match_treatment=fileio.EmptyMatchTreatment.
            ALLOW_IF_WILDCARD,
        )
        | 'Read Accounts' >> textio.ReadAllFromText()
        | 'Accounts to JSON' >> beam.Map(json.loads)
        | 'Accounts to table format' >> beam.Map(convert_accounts)
        | 'Accounts back to JSON' >> beam.Map(json.dumps)
        | 'Output Accounts' >> textio.WriteToText(
            flags.FLAGS.accounts_output))

    def convert_shipping_settings(row):
      """Parses one account's v1 shipping settings into the table shape.

      `merchant_shipping.download_shipping_settings` writes the native v1 shape;
      the row layout is defined once as `schema_pb2.ShippingSettings`, which
      also generates the BigQuery schema. As with accounts, parsing here rather
      than in the downloader keeps `schema_pb2` out of the process that loads
      the Merchant API GAPIC.

      `ignore_unknown_fields` drops the stamped `downloaderMetadata` (and any
      not-yet-modeled v1 attribute) instead of failing the parse.

      Args:
        row: A native-shape shipping settings dict, as read from
          `merchant_center/*/shippingsettings/*.jsonlines`.

      Returns:
        A dict matching the generated BigQuery `shippingsettings` schema.
      """
      msg = schema_pb2.ShippingSettings()
      json_format.ParseDict(row, msg, ignore_unknown_fields=True)
      return json_format.MessageToDict(
          msg,
          always_print_fields_with_no_presence=True,
          preserving_proto_field_name=True,
      )

    # Process shipping settings
    _ = (
        p
        | 'Glob Shipping Settings files' >> fileio.MatchFiles(
            file_pattern=(
                f'{source_dir}/merchant_center/*/shippingsettings/'
                '*.jsonlines'),
            empty_match_treatment=fileio.EmptyMatchTreatment.
            ALLOW_IF_WILDCARD,
        )
        | 'Read Shipping Settings' >> textio.ReadAllFromText()
        | 'Shipping Settings to JSON' >> beam.Map(json.loads)
        | 'Shipping Settings to table format' >> beam.Map(
            convert_shipping_settings)
        | 'Shipping Settings back to JSON' >> beam.Map(json.dumps)
        | 'Output Shipping Settings' >> textio.WriteToText(
            flags.FLAGS.shippingsettings_output))

    # Process LIA settings
    _ = (
        p
        | 'Glob LIA Settings files' >> fileio.MatchFiles(
            file_pattern=(
                f'{source_dir}/merchant_center/*/liasettings/*.jsonlines'),
            empty_match_treatment=fileio.EmptyMatchTreatment.
            ALLOW_IF_WILDCARD,
        )
        | 'Read LIA Settings' >> textio.ReadAllFromText()
        | 'LIA Settings to JSON' >> beam.Map(json.loads)
        | 'LIA Settings to table format' >> beam.Map(convert_lia_settings)
        | 'LIA Settings back to JSON' >> beam.Map(json.dumps)
        | 'Output LIA settings' >> textio.WriteToText(
            flags.FLAGS.liasettings_output))

    languages_by_campaign_id = (
        campaign_criteria
        |
        beam.Filter(lambda c: c['campaignCriterion']['type'] == 'LANGUAGE')
        | beam.Map(lambda c: (
            c['campaign']['id'],
            {
                'language':
                c['campaignCriterion']['language']['languageConstant'],
                'is_targeted':
                not c['campaignCriterion']['negative'],
            },
        )))

    listing_scopes_by_campaign_id = (
        campaign_criteria
        | beam.Filter(
            lambda c: c['campaignCriterion']['type'] == 'LISTING_SCOPE')
        | beam.Map(lambda c: (
            c['campaign']['id'],
            c['campaignCriterion']['listingScope']['dimensions'],
        )))

    campaigns = combine_campaign_settings(
        campaign_settings,
        languages_by_campaign_id,
        listing_scopes_by_campaign_id,
    )

    shopping_campaigns_by_merchant_id = (
        campaigns
        | beam.Filter(lambda c: c['campaign_type'] == 'SHOPPING')
        | 'Group Shopping campaigns by Merchant ID' >>
        beam.GroupBy(lambda c: c['merchant_id']))

    # Precompute shopping targeting sideinput
    shopping_trees_by_campaign_id = (
        ad_group_criteria
        | 'Group filters by ad group' >> beam.GroupBy(lambda f: (
            f['campaign']['id'],
            f['adGroup']['id'],
        ))
        | beam.MapTuple(shopping.build_product_group_tree)
        | beam.MapTuple(lambda ids, tree: (ids[0], tree))
        | 'Group Shopping Listing Group Trees by Campaign ID' >>
        beam.GroupByKey())

    pmax_campaigns_by_merchant_id = (
        campaigns
        | beam.Filter(lambda c: c['campaign_type'] == 'PERFORMANCE_MAX')
        | 'Group PMax campaigns by Merchant ID' >>
        beam.GroupBy(lambda c: c['merchant_id']))

    # Precompute PMax targeting sideinput
    pmax_trees_by_campaign_id = (
        asset_group_listing_filters
        | 'Group filters by asset group' >> beam.GroupBy(lambda f: (
            f['campaign']['id'],
            f['assetGroup']['id'],
        ))
        | beam.MapTuple(performance_max.build_product_targeting_tree)
        | beam.MapTuple(lambda ids, tree: (ids[0], tree))
        | 'Group PMax Listing Group Trees by Campaign ID' >>
        beam.GroupByKey())

    def split_v1_product(p):
      """Splits a native v1 Product into a strongly-typed WideProduct message.

      In the v1 Product representation, we parse the raw dictionary into
      the official `schema_pb2.WideProduct` Protobuf compiled message class,
      entirely eliminating "type erasure" throughout the pipeline.

      Args:
          p: A native v1 `Product` dict, as read from
          `merchant_center/*/products/*.jsonlines`.

      Returns:
          A strongly-typed schema_pb2.WideProduct object.
      """
      account_id = p[METADATA_KEY]['accountId']
      status = p.pop('product_status', None) or {}
      channel = 'local' if p.get('legacy_local') else 'online'

      wide_row = {
          'accountId': account_id,
          'offerId': p.get('offer_id'),
          'channel': channel,
          'product': p,
          'status': status,
      }

      msg = schema_pb2.WideProduct()
      json_format.ParseDict(wide_row, msg, ignore_unknown_fields=True)
      return msg

    product_statuses = (
        products
        | 'Build wide product records' >> beam.Map(split_v1_product))

    def attach_pmax_campaign_ids(
        product_row: schema_pb2.WideProduct,
        campaign_ids: list[int],
    ) -> schema_pb2.WideProduct:
      """Records which Performance Max campaigns target this product.

      Args:
        product_row: The wide product record to annotate.
        campaign_ids: IDs of the PMax campaigns whose asset group listing
          filters matched this product. Empty if none matched.

      Returns:
        A copy of `product_row` with the PMax targeting fields populated.
      """
      annotated = copy.deepcopy(product_row)
      annotated.has_performance_max_targeting = (
          True if campaign_ids else False)
      del annotated.performance_max_campaign_ids[:]
      annotated.performance_max_campaign_ids.extend(
          [int(cid) for cid in campaign_ids])
      return annotated

    def attach_shopping_campaign_ids(
        product_row: schema_pb2.WideProduct,
        campaign_ids: list[int],
    ) -> schema_pb2.WideProduct:
      """Records which Shopping campaigns target this product.

      Args:
        product_row: The wide product record to annotate. Already carries its
          Performance Max targeting from `attach_pmax_campaign_ids`.
        campaign_ids: IDs of the Shopping campaigns whose listing group trees
          matched this product. Empty if none matched.

      Returns:
        A copy of `product_row` with the Shopping targeting fields populated.
      """
      annotated = copy.deepcopy(product_row)
      annotated.has_shopping_targeting = True if campaign_ids else False
      del annotated.shopping_campaign_ids[:]
      annotated.shopping_campaign_ids.extend(
          [int(cid) for cid in campaign_ids])
      return annotated

    def to_products_table_row(
        product_row: schema_pb2.WideProduct,
    ) -> dict[str, Any]:
      """Converts a fully annotated WideProduct into a `products` table row.

      Args:
        product_row: The wide product record, carrying Merchant Center data,
          the derived status flags, and both Shopping and PMax targeting.

      Returns:
        A JSON-serializable dict matching the generated BigQuery `products`
        schema (snake_case field names).
      """
      return json_format.MessageToDict(
          product_row, preserving_proto_field_name=True)

    # `attach_pmax_campaign_ids` runs between the PMax and Shopping targeting
    # stages so the Shopping stage receives a WideProduct message rather than
    # the (product, trees) tuple that `get_campaign_targeting` emits.
    all_products_pmax = (
        product_statuses
        | 'Approved' >> beam.Map(product.set_product_approved)
        | 'In Stock' >> beam.Map(product.set_product_in_stock)
        | 'Get PMax targeting' >> beam.FlatMap(
            product.get_campaign_targeting,
            pvalue.AsDict(pmax_trees_by_campaign_id),
            pvalue.AsDict(pmax_campaigns_by_merchant_id),
            pvalue.AsDict(category_names_by_id),
            pvalue.AsDict(language_codes_by_resource_name),
        )
        | 'Combine PMax targeting raw' >> beam.MapTuple(
            lambda product, trees: (
                product,
                sorted(list(set([int(t['campaign_id']) for t in trees])))
            ))
        | 'Attach PMax campaign IDs' >> beam.MapTuple(
            attach_pmax_campaign_ids)
        | 'Get Shopping targeting' >> beam.FlatMap(
            product.get_campaign_targeting,
            pvalue.AsDict(shopping_trees_by_campaign_id),
            pvalue.AsDict(shopping_campaigns_by_merchant_id),
            pvalue.AsDict(category_names_by_id),
            pvalue.AsDict(language_codes_by_resource_name),
        )
        | 'Combine Shopping targeting raw' >> beam.MapTuple(
            lambda product, trees: (
                product,
                sorted(list(set([int(t['campaign_id']) for t in trees])))
            ))
    )

    _ = (all_products_pmax
         | 'Attach Shopping campaign IDs' >> beam.MapTuple(
             attach_shopping_campaign_ids)
         | 'Convert to products table row' >> beam.Map(to_products_table_row)
         | 'JSON' >> beam.Map(json.dumps)
         | textio.WriteToText(flags.FLAGS.products_output))


if __name__ == '__main__':
  app.run(main)
