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
"""Merchant Center product data helpers (strongly typed via Protobuf)."""

import copy

from typing import Optional, Tuple, List, Callable, Any, TypedDict, Iterable
from acit.api.v0.storage import schema_pb2
from google.shopping import merchant_products_v1 as mp
from google.shopping import type as gst

_SHOPPING_ADS = gst.ReportingContext.ReportingContextEnum.SHOPPING_ADS

# 1-indexed dimension levels
_PRODUCT_DIMENSION_LEVELS = [
    'LEVEL1',
    'LEVEL2',
    'LEVEL3',
    'LEVEL4',
    'LEVEL5',
]

# 0-indexed dimension indices
_PRODUCT_DIMENSION_INDICES = [
    'INDEX0',
    'INDEX1',
    'INDEX2',
    'INDEX3',
    'INDEX4',
]


class TargetedLanguage(TypedDict):
  language: str
  is_targeted: bool


class Campaign(TypedDict):
  customer_id: str
  campaign_id: str
  campaign_type: str
  merchant_id: str
  feed_label: str
  enable_local: bool
  languages: list[TargetedLanguage]
  inventory_filter_dimensions: list[Any]


def build_campaign(
    campaign: dict[str, Any],
    languages: list[TargetedLanguage],
    inventory_filter_dimensions: list[Any],
) -> Iterable[Campaign]:
  shopping_settings = campaign['campaign'].get('shoppingSetting')
  if shopping_settings:
    return [
        Campaign(
            customer_id=campaign['customer']['id'],
            campaign_id=campaign['campaign']['id'],
            campaign_type=campaign['campaign']['advertisingChannelType'],
            merchant_id=shopping_settings['merchantId'],
            feed_label=shopping_settings.get('feedLabel', ''),
            enable_local=shopping_settings.get('enableLocal', False),
            languages=languages,
            inventory_filter_dimensions=inventory_filter_dimensions,
        )
    ]
  return []


def _get_attributes(
    product: schema_pb2.WideProduct,
) -> mp.ProductAttributes:
  """Returns the offer attributes embedded in a WideProduct.

  Args:
    product: The wide product record to read from.

  Returns:
    The `product_attributes` submessage. Proto3 yields an empty message rather
    than None when it was never set.
  """
  return product.product.product_attributes


def set_product_in_stock(
        product: schema_pb2.WideProduct) -> schema_pb2.WideProduct:
  """Sets a key on the composite product status for product availability."""
  product_copy = copy.deepcopy(product)
  attributes = _get_attributes(product_copy)
  product_copy.in_stock = (
      attributes.availability == mp.Availability.IN_STOCK)
  return product_copy


def set_product_approved(
        product: schema_pb2.WideProduct) -> schema_pb2.WideProduct:
  """Sets a key depending on whether the offer is approved in all locations."""
  product_copy = copy.deepcopy(product)
  destinations = [
      d
      for d in product_copy.status.destination_statuses
      if d.reporting_context == _SHOPPING_ADS
  ]
  for destination in destinations:
    del product_copy.approved_countries[:]
    del product_copy.pending_countries[:]
    del product_copy.disapproved_countries[:]
    product_copy.approved_countries.extend(destination.approved_countries)
    product_copy.pending_countries.extend(destination.pending_countries)
    product_copy.disapproved_countries.extend(
        destination.disapproved_countries)
    break
  return product_copy


def taxonomy_matches_dimension(
    product_taxonomy: str,
    dimension: Any,
    dimension_key: str,
    depth_key: str,
    depth_names: List[str],
    test: Callable[[Any, str], bool],
) -> bool:
  """Whether the given taxonomy string matches the input dimension.

  Taxonomy strings are used for Google Product Categories and
  user-provided types. They are of the form "A > B > C".

  This method looks at the expected level of the dimension, and truncates (as
  necessary) the taxonomy string to only that many levels, useful when recursing
  through multiple dimension path levels.

  Args:
    product_taxonomy: The taxonomy string from the product.
    dimension: The ListingGroupDimension object to compare against.
    dimension_key: The `oneof` field name for this object.
    depth_key: The "level" key name within the object.
    depth_names: A lookup table for valid key values.
    test: A callable to test whether dimension[dimension_key] matches the
            depth-truncated taxonomy string.
  Returns:
    The result of the test
  """
  if dimension_key in dimension:
    info = dimension[dimension_key]
    depth = depth_names.index(info[depth_key])
    taxonomy_tokens = product_taxonomy.split(' > ')
    if depth >= len(taxonomy_tokens):
      # Ad criteria is too granular
      return False
    # We only want to match up to the depth specified in the dimension
    product_taxonomy = ' > '.join(taxonomy_tokens[: depth + 1])
    return test(info, product_taxonomy)
  return False


_WILDCARD_DIMENSION_PATHS = {
    'productCategory': 'categoryId',
    'productBrand': 'value',
    'productChannel': 'channel',
    'productChannelExclusivity': 'channelExclusivity',
    'productCondition': 'condition',
    'productCustomAttribute': 'value',
    'productItemId': 'value',
    'productType': 'value',
}


def dimension_is_wildcard(dimension: dict[str, Any]) -> bool:
  for key, sub_key in _WILDCARD_DIMENSION_PATHS.items():
    if key in dimension:
      return sub_key not in dimension[key]
  return False


def dimension_matches_product(
    product: schema_pb2.WideProduct,
    dimension: dict[str, Any],
    category_names_by_id: dict[str, str],
) -> bool:
  """Whether the provided dimension matches the product.

  Args:
    product: The wide product record to test.
    dimension: One `ListingGroupDimension` from a Google Ads listing group or
      asset group filter. Google Ads JSON, so the keys are camelCase.
    category_names_by_id: Google product category ID to US English name.

  Returns:
    Whether this dimension matches the product.
  """
  if dimension_is_wildcard(dimension):
    return True

  attributes = _get_attributes(product)

  if 'productCategory' in dimension:
    level = dimension['productCategory']['level']
    id_ = dimension['productCategory']['categoryId']

    taxonomy_index = _PRODUCT_DIMENSION_LEVELS.index(level)
    google_product_category = attributes.google_product_category
    taxonomy_tokens = google_product_category.split(' > ')
    if not taxonomy_index < len(taxonomy_tokens):
      # Ad criteria is too granular
      return False
    category_to_match = taxonomy_tokens[taxonomy_index]
    # Campaigns before 2019 may have obsolete category IDs
    if id_ not in category_names_by_id:
      return False
    category = category_names_by_id[id_]
    return bool(category_to_match == category)

  # All string comparisons should be case-insensitive
  # Ads and MC have inconsistent case processing.

  if 'productBrand' in dimension:
    brand = attributes.brand
    return bool(
        dimension['productBrand']['value'].lower()
        == brand.lower()
    )
  if 'productChannel' in dimension:
    # `channel` is derived (online/local) from the v1 `legacy_local` flag and
    # set at the root of the WideProduct in the Beam stage.
    return bool(
        dimension['productChannel']['channel'].lower()
        == product.channel.lower()
    )
  if 'productChannelExclusivity' in dimension:
    # Google API Product does not expose channel exclusivity anymore, assume
    # MULTI_CHANNEL.
    return bool(
        dimension['productChannelExclusivity']['channelExclusivity'].lower()
        == 'multi_channel'
    )
  if 'productCondition' in dimension:
    # v1 `condition` is an enum NAME string (NEW/USED/REFURBISHED); lower-cased
    # it matches the Ads dimension value (new/used/refurbished).
    condition = mp.Condition(attributes.condition).name
    return bool(
        dimension['productCondition']['condition'].lower()
        == condition.lower()
    )
  if 'productCustomAttribute' in dimension:
    info = dimension['productCustomAttribute']
    depth = _PRODUCT_DIMENSION_INDICES.index(info['index'])
    product_label = getattr(attributes, f'custom_label_{depth}', '')
    return bool(info['value'].lower() == product_label.lower())
  if 'productItemId' in dimension:
    # `split_v1_product` lifts the offer ID to the root of the WideProduct;
    # fall back to the embedded Product for records written before that.
    offer_id = product.offer_id or product.product.offer_id
    return bool(
        dimension['productItemId']['value'].lower()
        == offer_id.lower()
    )
  if 'productType' in dimension:
    product_types = list(attributes.product_types)
    first_type = product_types[0] if product_types else ''
    return taxonomy_matches_dimension(
        first_type,
        dimension,
        'productType',
        'level',
        _PRODUCT_DIMENSION_LEVELS,
        lambda dimension, taxonomy: dimension['value'].lower()
        == taxonomy.split(' > ')[-1].lower()
        if taxonomy
        else False,
    )

  return False


class ProductTargetingNode(TypedDict):
  """Recursive Node for Product Targeting.

  Attributes:
    children: Any child nodes
    dimension: Ads targeting info. Empty on the root.
    is_targeted: Whether this branch is targeted. Leaf only.
  """

  children: List['ProductTargetingNode']
  dimension: Any
  # JSON field format
  isTargeted: Optional[bool]


class ProductTargetingTree(TypedDict):
  """Represents Ads targeting for a Product.

  Attributes:
    customer_id: The Customer ID
    campaign_id: The Campaign ID
    tree_parent_id: For Shopping Campaigns, the ad_group_id;
    for Performance Max, the asset_group_id.
  """

  customer_id: str
  campaign_id: str
  tree_parent_id: str
  node: ProductTargetingNode


def product_targeted_by_tree(
    product: schema_pb2.WideProduct,
    node: ProductTargetingNode,
    category_names_by_id: dict[str, str]
) -> bool:
  """Recursively checks whether a product is targeted by a tree.

  Args:
    product: The wide product record to test.
    node: The tree node to descend from; the root on the first call.
    category_names_by_id: Google product category ID to US English name.

  Returns:
    Whether the product is targeted by the leaf this tree resolves to.
  """
  is_targeted = node.get('isTargeted')
  if is_targeted is not None:
    return is_targeted
  matcher = None
  wildcard = None
  for child in node['children']:
    dimension = child['dimension']
    if not dimension:
      # TODO: The Ads API does not support Collections.
      #       Since dimension can only be empty for the root node, if we
      #       hit here, we've encountered a second empty dimension. So,
      #       we assume conservatively that this product is not targeted.
      #       Besides, collections have fundamental differences with
      #       normal PMax shopping campaigns.
      return False
    if dimension_is_wildcard(dimension):
      wildcard = child
    elif dimension_matches_product(product, dimension, category_names_by_id):
      matcher = child
  first_match = matcher or wildcard
  if not first_match:
    # NOTE: This is a bad data scenario.
    #
    # We don't actually know whether this product is targeted or not. But we
    # should err on the side of caution and report the product as untargeted.
    #
    # Unfortunately, if a wildcard exists, we will never hit this flow, and
    # will have to return the targeting value of the wildcard.
    return False

  return product_targeted_by_tree(product, first_match, category_names_by_id)


def build_product_group_tree(
    dimensions: List[Any], node: ProductTargetingNode, is_targeted: bool
) -> None:
  """Recusively builds a ProductTargeting Tree.

  A Product Targeting tree is either a Listing Group for shopping ads,
  or an Asset Group Listing Group Filter for Performance Max.

  Presumes an empty top-level node at root.

  Args:
    dimensions: The array of dimensions from the campaign resource.
    node: The node of the tree to populate.
    is_targeted: Whether the leaf we're building to should be targeted.
  """

  if not dimensions:
    return
  assert node.get('isTargeted') is None
  head, rest = dimensions[0], dimensions[1:]
  # Does the child exist at this level?
  matches = [c for c in node['children'] if c['dimension'] == head]
  if matches:
    child = matches[0]
  else:
    child = ProductTargetingNode(
        children=[], dimension=head, isTargeted=None if rest else is_targeted
    )
    node['children'].append(child)
  build_product_group_tree(rest, child, is_targeted)


def campaign_matches_product_status(
    campaign: Campaign,
    product_status: schema_pb2.WideProduct,
    category_names_by_id: dict[str, str],
    language_codes_by_resource_name: dict[str, str],
) -> bool:
    """Determines if a campaign's targeting criteria admit the given product.

    Args:
      campaign: The campaign whose targeting settings to test.
      product_status: The wide product record to test.
      category_names_by_id: Google product category ID to US English name.
      language_codes_by_resource_name: Ads language resource name to language
        code.

    Returns:
      Whether the campaign could target this product at all.
    """

    product = product_status.product
    if campaign['merchant_id'] != product_status.account_id:
      return False
    campaign_label = campaign['feed_label'].lower()
    if campaign_label and campaign_label != product.feed_label.lower():
      return False
    if not campaign['enable_local'] and product_status.channel == 'local':
      return False
    for dimension in campaign['inventory_filter_dimensions']:
      if not dimension_matches_product(
          product_status, dimension, category_names_by_id):
        return False

    # Language targeting
    positive_languages = [
        language_codes_by_resource_name[lang['language']]
        for lang in campaign['languages']
        if lang['is_targeted']
    ]
    negative_languages = [
        language_codes_by_resource_name[lang['language']]
        for lang in campaign['languages']
        if not lang['is_targeted']
    ]

    content_language = product.content_language
    if (
        positive_languages
        and content_language not in positive_languages
    ):
      return False
    if content_language in negative_languages:
      return False
    return True


def get_campaign_targeting(
    product: schema_pb2.WideProduct,
    trees_by_campaign_id: dict[str, list[ProductTargetingTree]],
    campaigns_by_merchant_id: dict[str, list[Campaign]],
    category_names_by_id: dict[str, str],
    language_codes_by_resource_name: dict[str, str],
) -> Iterable[Tuple[schema_pb2.WideProduct, list[ProductTargetingTree]]]:
  """Determines whether a product is targeted by any visible campaigns.

  Conforms to Beam ParDo function return requirements.

  Args:
    product: The wide product record to compare against.
    trees_by_campaign_id: A multimap of campaign ID to the unique listing group
      tree for each campaign. Works for PMax asset groups as well as Shopping
      ad groups.
    campaigns_by_merchant_id: A multimap of Merchant Center account ID to the
      campaigns fed by it.
    category_names_by_id: Google product category ID to US English name.
    language_codes_by_resource_name: Ads language resource name to language
      code.

  Returns:
    An iterable Tuple of the product and all trees that match it.
  """

  matched_trees = []
  account_id = product.account_id
  for campaign in campaigns_by_merchant_id.get(account_id, []):
    if not campaign_matches_product_status(
        campaign, product, category_names_by_id, language_codes_by_resource_name
    ):
      continue
    for tree in trees_by_campaign_id.get(campaign['campaign_id'], []):
      if product_targeted_by_tree(
          product, tree['node'], category_names_by_id
      ):
        matched_trees.append(tree)

  return [(product, matched_trees)]
