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

import json


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

    def products_table_row(tup):
      """Prepare data for JSON serialization."""
      # Tup is (product, shopping_campaign_ids) where product was already
      # processed by PMax Combine.
      product_obj, shopping_campaign_ids = tup
      import copy
      product_obj_copy = copy.deepcopy(product_obj)
      product_obj_copy.has_shopping_targeting = (
          True if shopping_campaign_ids else False)
      del product_obj_copy.shopping_campaign_ids[:]
      product_obj_copy.shopping_campaign_ids.extend(
          [int(cid) for cid in shopping_campaign_ids])

      return json_format.MessageToDict(product_obj_copy,
                                       preserving_proto_field_name=True)

    def pmax_combine_row(tup):
      # Tup is (product, pmax_campaign_ids)
      product_obj, pmax_campaign_ids = tup
      import copy
      product_obj_copy = copy.deepcopy(product_obj)
      product_obj_copy.has_performance_max_targeting = (
          True if pmax_campaign_ids else False)
      del product_obj_copy.performance_max_campaign_ids[:]
      product_obj_copy.performance_max_campaign_ids.extend(
          [int(cid) for cid in pmax_campaign_ids])
      return product_obj_copy

    # Hook pmax_combine_row between the PMax and Shopping
    # targeting stages to pass proto correctly
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
        | 'Combine PMax targeting to proto' >> beam.Map(pmax_combine_row)
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
         | 'Convert to final products table output' >>
         beam.Map(products_table_row)
         | 'JSON' >> beam.Map(json.dumps)
         | textio.WriteToText(flags.FLAGS.products_output))


if __name__ == '__main__':
  app.run(main)
