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

from typing import Optional, Dict, Tuple, List, Callable, Any, TypedDict, Iterable
from acit.api.v0.storage import schema_pb2

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
    campaign,
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


def _val(obj: Any, key: str, default: Any = None) -> Any:
  """Polymorphic helper to get a field value from either a Protobuf or a dict."""
  if isinstance(obj, dict):
    return obj.get(key, default)
  
  # For Protobuf messages, check if the field is an enum and convert to its name string
  if hasattr(obj, 'DESCRIPTOR'):
    field_desc = obj.DESCRIPTOR.fields_by_name.get(key)
    if field_desc and field_desc.type == field_desc.TYPE_ENUM:
      val_int = getattr(obj, key)
      # Translate integer enum to its string name
      return field_desc.enum_type.values_by_number[val_int].name

  return getattr(obj, key, default)


def _get_attributes(product: Any) -> Any:
  """Safely gets product attributes from either a WideProduct proto or a dictionary."""
  # Case 1: product is a WideProduct proto
  if hasattr(product, 'product'):
    inner_prod = product.product
    if hasattr(inner_prod, 'product_attributes'):
      return inner_prod.product_attributes
  # Case 2: product is a dict representing WideProduct
  if isinstance(product, dict):
    inner_prod = product.get('product')
    if isinstance(inner_prod, dict):
      return inner_prod.get('product_attributes') or {}
    # Fallback Case 3: product itself is the attributes or inner product dict (used in some unit tests)
    attrs = product.get('product_attributes')
    if attrs is not None:
      return attrs
    return product
  return {}


def set_product_in_stock(product: Any) -> Any:
  """Sets a key on the composite product status for product availability."""
  attributes = _get_attributes(product)
  availability = _val(attributes, 'availability', '')
  in_stock = 'IN_STOCK' == availability

  if isinstance(product, dict):
    product['inStock'] = in_stock
  else:
    product.in_stock = in_stock
  return product


def set_product_approved(product: Any) -> Any:
  """Sets a key depending on whether the offer is approved in all locations."""
  if isinstance(product, dict):
    status = product.get('status', {})
    destinations = [
        d
        for d in status.get('destination_statuses', [])
        if d.get('reporting_context') == 'SHOPPING_ADS'
    ]
    for destination in destinations:
      product['approvedCountries'] = destination.get('approved_countries', [])
      product['pendingCountries'] = destination.get('pending_countries', [])
      product['disapprovedCountries'] = destination.get('disapproved_countries', [])
      break
  else:
    destinations = [
        d
        for d in product.status.destination_statuses
        if _val(d, 'reporting_context') == 'SHOPPING_ADS'
    ]
    for destination in destinations:
      del product.approved_countries[:]
      del product.pending_countries[:]
      del product.disapproved_countries[:]
      product.approved_countries.extend(destination.approved_countries)
      product.pending_countries.extend(destination.pending_countries)
      product.disapproved_countries.extend(destination.disapproved_countries)
      break
  return product


def taxonomy_matches_dimension(
    product_taxonomy: str,
    dimension: Any,
    dimension_key: str,
    depth_key: str,
    depth_names: List[str],
    test: Callable[[Any, str], bool],
) -> bool:
  """Whether the given taxonomy string matches the input dimension.

  Taxonomy strings are used for Google Product Categories and user-provided types.
  They are of the form "A > B > C".

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


def dimension_is_wildcard(dimension) -> bool:
  for key, sub_key in _WILDCARD_DIMENSION_PATHS.items():
    if key in dimension:
      return sub_key not in dimension[key]
  return False


def dimension_matches_product(
    product: Any,
    dimension: Any,
    category_names_by_id: dict[str, str],
) -> bool:
  """Whether the provided dimension matches the product."""
  if dimension_is_wildcard(dimension):
    return True

  attributes = _get_attributes(product)

  if 'productCategory' in dimension:
    level = dimension['productCategory']['level']
    id_ = dimension['productCategory']['categoryId']

    taxonomy_index = _PRODUCT_DIMENSION_LEVELS.index(level)
    google_product_category = _val(attributes, 'google_product_category', '')
    taxonomy_tokens = google_product_category.split(' > ')
    if not taxonomy_index < len(taxonomy_tokens):
      # Ad criteria is too granular
      return False
    category_to_match = taxonomy_tokens[taxonomy_index]
    # Campaigns before 2019 may have obsolete category IDs
    if id_ not in category_names_by_id:
      return False
    category = category_names_by_id[id_]
    return category_to_match == category

  # All string comparisons should be case-insensitive
  # Ads and MC have inconsistent case processing.

  if 'productBrand' in dimension:
    brand = _val(attributes, 'brand', '')
    return (
        dimension['productBrand']['value'].lower()
        == brand.lower()
    )
  if 'productChannel' in dimension:
    # `channel` is derived (online/local) from the v1 `legacy_local` flag and
    # set at the root of the WideProduct in the Beam stage.
    channel = _val(product, 'channel', '')
    return (
        dimension['productChannel']['channel'].lower()
        == channel.lower()
    )
  if 'productChannelExclusivity' in dimension:
    # Google API Product does not expose channel exclusivity anymore, assume MULTI_CHANNEL.
    return (
        dimension['productChannelExclusivity']['channelExclusivity'].lower()
        == 'multi_channel'
    )
  if 'productCondition' in dimension:
    # v1 `condition` is an enum NAME string (NEW/USED/REFURBISHED); lower-cased
    # it matches the Ads dimension value (new/used/refurbished).
    condition = _val(attributes, 'condition', '')
    return (
        dimension['productCondition']['condition'].lower()
        == condition.lower()
    )
  if 'productCustomAttribute' in dimension:
    info = dimension['productCustomAttribute']
    depth = _PRODUCT_DIMENSION_INDICES.index(info['index'])
    product_label = _val(attributes, f'custom_label_{depth}', '')
    return info['value'].lower() == product_label.lower()
  if 'productItemId' in dimension:
    offer_id = _val(product, 'offerId', '') or _val(product, 'offer_id', '')
    if not offer_id and hasattr(product, 'product'):
      offer_id = product.product.offer_id
    if not offer_id and isinstance(product, dict) and 'product' in product:
      offer_id = product['product'].get('offer_id')
    return (
        dimension['productItemId']['value'].lower()
        == str(offer_id).lower()
    )
  if 'productType' in dimension:
    product_types = _val(attributes, 'product_types', []) or []
    product_types = list(product_types)
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
    tree_parent_id: For Shopping Campaigns, the ad_group_id; for Performance Max, the asset_group_id.
  """

  customer_id: str
  campaign_id: str
  tree_parent_id: str
  node: ProductTargetingNode


def product_targeted_by_tree(
    product: Any,
    node: ProductTargetingNode,
    category_names_by_id: dict[str, str]
) -> bool:
  """Recursively checks whether a product is targeted by a tree."""
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
):
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
    product_status: Any,
    category_names_by_id,
    language_codes_by_resource_name,
) -> bool:
  # TODO: unit test this
  product = _val(product_status, 'product')
  account_id = _val(product_status, 'accountId') or _val(product_status, 'account_id')
  if campaign['merchant_id'] != account_id:
    return False
  campaign_label = campaign['feed_label'].lower()
  feed_label = _val(product, 'feed_label', '')
  if campaign_label and campaign_label != feed_label.lower():
    return False
  channel = _val(product_status, 'channel', '')
  if not campaign['enable_local'] and channel == 'local':
    return False
  for dimension in campaign['inventory_filter_dimensions']:
    if not dimension_matches_product(product_status, dimension, category_names_by_id):
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

  content_language = _val(product, 'content_language', '')
  if (
      positive_languages
      and content_language not in positive_languages
  ):
    return False
  if content_language in negative_languages:
    return False
  return True


def get_campaign_targeting(
    product: Any,
    trees_by_campaign_id: dict[str, list[ProductTargetingTree]],
    campaigns_by_merchant_id: dict[str, list[Campaign]],
    category_names_by_id: dict[str, str],
    language_codes_by_resource_name: dict[str, str],
) -> Iterable[Tuple[Any, list[ProductTargetingTree]]]:
  """Determines whether a product is targeted by any visible campaigns.

  Conforms to Beam ParDo function return requirements.

  Args:
    product: The product to compare against
    mechant_to_cids: A lookup table of Merchant Center leaf IDs to Ads Customer IDs
    cid_to_ad_group_trees: A multimap of cids to the unique listing group tree for each
      campaign. Works for PMax asset groups as well as Shopping ad groups.

  Returns:
    An iterable Tuple of the product and all trees that match it.
  """

  matched_trees = []
  account_id = _val(product, 'accountId') or _val(product, 'account_id')
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
