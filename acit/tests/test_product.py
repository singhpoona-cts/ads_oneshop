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

from absl.testing import absltest
from absl.testing import parameterized

from acit import product as product_util
from acit.api.v0.storage import schema_pb2

_PRODUCT_CATEGORIES_BY_ID = {
    '1': 'Animals & Pet Supplies',
    '2': 'Other',
}


def _to_camel_case(snake_str):
  components = snake_str.split('_')
  return components[0] + ''.join(x.title() for x in components[1:])


def _convert_dict_to_camel_case(d):
  if isinstance(d, list):
    return [_convert_dict_to_camel_case(x) for x in d]
  if isinstance(d, dict):
    new_dict = {}
    for k, v in d.items():
      camel_key = _to_camel_case(k)
      # Standardize enum strings to uppercase so json_format.ParseDict maps them successfully
      if camel_key in ('condition', 'availability', 'reportingContext') and isinstance(v, str):
        v = v.upper()
      new_dict[camel_key] = _convert_dict_to_camel_case(v)
    return new_dict
  return d


class ProductTest(parameterized.TestCase):

  @parameterized.named_parameters(
      {
          'testcase_name': 'product_category_wildcard',
          'dimension': {
              'productCategory': {
                  'level': 'LEVEL5'
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_category_specific',
          'dimension': {
              'productCategory': {
                  'categoryId': '1',
                  'level': 'LEVEL5'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_brand_wildcard',
          'dimension': {
              'productBrand': {}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_brand_specific',
          'dimension': {
              'productBrand': {
                  'value': 'Brand Name'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_channel_wildcard',
          'dimension': {
              'productChannel': {}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_specific',
          'dimension': {
              'productChannel': {
                  'channel': 'ONLINE'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_channel_exclusivity_wildcard',
          'dimension': {
              'productChannelExclusivity': {}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_exclusivity_specific',
          'dimension': {
              'productChannelExclusivity': {
                  'channelExclusivity': 'MULTI_CHANNEL'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_condition_wildcard',
          'dimension': {
              'productCondition': {}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_condition_specific',
          'dimension': {
              'productCondition': {
                  'condition': 'NEW'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_custom_attribute_wildcard',
          'dimension': {
              'productCustomAttribute': {
                  'index': 'INDEX0'
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_custom_attribute_specific',
          'dimension': {
              'productCustomAttribute': {
                  'index': 'INDEX4',
                  'value': 'Some Custom Attribute',
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_item_id_wildcard',
          'dimension': {
              'productItemId': {}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_item_id_specific',
          'dimension': {
              'productItemId': {
                  'value': 'my_product_id'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_type_wildcard',
          'dimension': {
              'productType': {
                  'level': 'LEVEL1'
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_type_specific',
          'dimension': {
              'productType': {
                  'level': 'LEVEL5',
                  'value': 'My product type'
              }
          },
          'expected': False,
      },
  )
  def test_dimension_is_wildcard(self, dimension, expected):
    result = product_util.dimension_is_wildcard(dimension)
    self.assertEqual(result, expected)

  @parameterized.named_parameters(
      {
          'testcase_name': 'product_category_top_level_match',
          'product': {
              'product_attributes': {
                  'google_product_category': 'Animals & Pet Supplies'
              }
          },
          'dimension': {
              'productCategory': {
                  'categoryId': '1',
                  'level': 'LEVEL1'
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_category_top_level_mismatch',
          'product': {
              'product_attributes': {
                  'google_product_category': 'Animals & Pet Supplies'
              }
          },
          'dimension': {
              'productCategory': {
                  'categoryId': '2',
                  'level': 'LEVEL1'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_category_top_level_wildcard_match',
          'product': {
              'product_attributes': {
                  'google_product_category': 'Animals & Pet Supplies'
              }
          },
          'dimension': {
              'productCategory': {}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_brand_match',
          'product': {
              'product_attributes': {
                  'brand': 'Some Brand'
              }
          },
          'dimension': {
              'productBrand': {
                  'value': 'Some Brand'
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_match',
          'product': {
              'channel': 'online'
          },
          'dimension': {
              'productChannel': {
                  'channel': 'online'
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_condition_match',
          'product': {
              'product_attributes': {
                  'condition': 'new'
              }
          },
          'dimension': {
              'productCondition': {
                  'condition': 'new'
              }
          },
          'expected': True,
      },
      # TODO: custom attribute out of index
      {
          'testcase_name': 'product_custom_attribute_match',
          'product': {
              'product_attributes': {
                  'custom_label_0': 'first attribute',
                  'custom_label_1': 'some attribute',
                  'custom_label_4': 'ignored',
              }
          },
          'dimension': {
              # 0-indexed
              # Confusingly this refers to product labels, not attributes
              'productCustomAttribute': {
                  'value': 'some attribute',
                  'index': 'INDEX1',
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_item_match',
          'product': {
              'offer_id': 'asdf'
          },
          'dimension': {
              'productItemId': {
                  'value': 'asdf'
              }
          },
          'expected': True,
      },
      # TODO: Same test cases as above
      {
          'testcase_name': 'product_type_match',
          # Ads only uses the first product type, no matter what
          'product': {
              'product_attributes': {
                  'product_types': [
                      'First type > some type > Other type',
                      'ignored taxonomy',
                      'some bad > > > data',
                  ]
              }
          },
          'dimension': {
              # 1-indexed
              'productType': {
                  'value': 'some type',
                  'level': 'LEVEL2'
              }
          },
          'expected': True,
      },
  )
  def test_specific_dimension_matches_product(self, product, dimension,
                                              expected):
    """Tests whether a single targeting dimension match a given product."""
    from google.protobuf import json_format
    camel_product = _convert_dict_to_camel_case(product)
    wide_row = {}
    if 'productAttributes' in camel_product:
      wide_row['product'] = camel_product
    else:
      wide_row = camel_product
    msg = schema_pb2.WideProduct()
    json_format.ParseDict(wide_row, msg, ignore_unknown_fields=True)
    self.assertEqual(
        product_util.dimension_matches_product(msg, dimension,
                                               _PRODUCT_CATEGORIES_BY_ID),
        expected,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'catch_all',
          'product': {},
          'tree': {
              'isTargeted': True
          },
          'expected': True,
      },
      {
          'testcase_name': 'basic_path_match',
          'product': {
              'product_attributes': {
                  'google_product_category': 'Animals & Pet Supplies'
              }
          },
          'tree': {
              'children': [
                  {
                      'dimension': {
                          'productCategory': {
                              'level': 'LEVEL1',
                              'categoryId': '1',
                          }
                      },
                      'isTargeted': True,
                  },
                  {
                      'dimension': {
                          'productCategory': {}
                      },
                      'isTargeted': False,
                  },
              ],
              'dimension': {},
          },
          'expected': True,
      },
      {
          'testcase_name': 'basic_path_wildcard',
          'product': {
              'product_attributes': {
                  'google_product_category': 'Animals & Pet Supplies'
              }
          },
          'tree': {
              'children': [
                  {
                      'dimension': {
                          'productCategory': {
                              'level': 'LEVEL1',
                              'categoryId': '2',
                          }
                      },
                      'isTargeted': False,
                  },
                  {
                      'dimension': {
                          'productCategory': {}
                      },
                      'isTargeted': True,
                  },
              ],
              'dimension': {},
          },
          'expected': True,
      },
  )
  def test_product_targeted_by_tree(self, product, tree, expected):
    from google.protobuf import json_format
    camel_product = _convert_dict_to_camel_case(product)
    wide_row = {}
    if 'productAttributes' in camel_product:
      wide_row['product'] = camel_product
    else:
      wide_row = camel_product
    msg = schema_pb2.WideProduct()
    json_format.ParseDict(wide_row, msg, ignore_unknown_fields=True)
    self.assertEqual(
        product_util.product_targeted_by_tree(msg, tree,
                                              _PRODUCT_CATEGORIES_BY_ID),
        expected,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'explicit_match',
          'product': {
              'product_attributes': {
                  'product_types': ['my level1 type']
              }
          },
          'node': {
              'children': [
                  {
                      'children': [],
                      'dimension': {
                          'productType': {
                              'level': 'LEVEL1',
                              'value': 'my level1 type',
                          }
                      },
                      'isTargeted': True,
                  },
                  # Even though we don't have the full tree, we know that this
                  # product type is targeted
              ],
              'dimension': {},
              'isTargeted':
              None,
          },
          'expected': True,
      },
      {
          'testcase_name': 'no_match_no_wildcard',
          'product': {
              'product_attributes': {
                  'product_types': ['no_match_type']
              }
          },
          'node': {
              'children': [
                  {
                      'children': [],
                      'dimension': {
                          'productType': {
                              'level': 'LEVEL1',
                              'value': 'my level1 type',
                          }
                      },
                      'isTargeted': False,
                  },
                  # There may be a wildcard here which includes this product
                  # type, but we don't actually know
              ],
              'dimension': {},
              'isTargeted':
              None,
          },
          # We must assume (cautiously) that this product is not targeted.
          'expected': False,
      },
      {
          'testcase_name': 'match_with_wildcard',
          'product': {
              'product_attributes': {
                  'product_types': ['my level1 type']
              }
          },
          'node': {
              'children':
              [{
                  'children': [],
                  'dimension': {
                      'productType': {
                          'level': 'LEVEL1',
                          # wildcard, value is omitted
                      }
                  },
                  'isTargeted': False,
              }
               # We would expect a missing branch here which explicitly
               # *excludes* this product type.
              ],
              'dimension': {},
              'isTargeted':
              None,
          },
          # In this scenario, we would want to return `True`,
          # but we are forced to take the value of the wildcard.
          'expected': False,
      },
      {
          'testcase_name': 'no_match_with_wildcard',
          'product': {
              'product_attributes': {
                  'product_types': ['no_match_type']
              }
          },
          'node': {
              'children': [
                  {
                      'children': [],
                      'dimension': {
                          'productType': {
                              'level': 'LEVEL1',
                              # wildcard, value is omitted
                          }
                      },
                      'isTargeted': True,
                  },
                  # We would expect a missing branch here which explicitly
                  # *excludes* this product type.
              ],
              'dimension': {},
              'isTargeted':
              None,
          },
          # Since there is no explicit match and we're missing branches,
          # we want to be cautious and report the product as
          # untargeted (`False`), even though this may be wrong.
          # But we are forced to take the wildcard
          # targeting. This may result in overreported product targeting.
          'expected': True,
      },
  )
  def test_product_targeted_by_unbalanced_tree(self, product, node,
                                               expected):
    """Tests whether a product matches targeting in an unbalanced tree."""
    from google.protobuf import json_format
    camel_product = _convert_dict_to_camel_case(product)
    wide_row = {}
    if 'productAttributes' in camel_product:
      wide_row['product'] = camel_product
    else:
      wide_row = camel_product
    msg = schema_pb2.WideProduct()
    json_format.ParseDict(wide_row, msg, ignore_unknown_fields=True)
    actual = product_util.product_targeted_by_tree(
        msg, node, _PRODUCT_CATEGORIES_BY_ID)
    self.assertEqual(expected, actual)

  def test_build_product_group_tree_empty_root(self):
    dimensions = [
        {
            'productType': {
                'level': 'LEVEL1',
                'value': 'my level1 type',
            },
        },
        {
            'productType': {
                'level': 'LEVEL2',
                # This is a wildcard, so we omit `value`
            },
        },
    ]
    root: product_util.ProductTargetingNode = {
        'children': [],
        'dimension': {},
        'isTargeted': None,
    }
    is_targeted = True

    expected = {
        'children': [{
            'children': [{
                'children': [],
                'dimension': {
                    'productType': {
                        'level': 'LEVEL2'
                    }
                },
                'isTargeted': True,
            }],
            'dimension': {
                'productType': {
                    'level': 'LEVEL1',
                    'value': 'my level1 type',
                }
            },
            'isTargeted':
            None,
        }],
        'dimension': {},
        'isTargeted':
        None,
    }

    product_util.build_product_group_tree(dimensions, root, is_targeted)

    self.assertEqual(expected, root)

  def test_build_product_group_tree_with_matches(self):
    dimensions = [
        {
            'productType': {
                'level': 'LEVEL1',
                'value': 'my level1 type',
            },
        },
        {
            'productType': {
                'level': 'LEVEL2',
                'value': 'my level2 type',
            },
        },
    ]

    root: product_util.ProductTargetingNode = {
        'children': [{
            'children': [
                {
                    'children': [],
                    'dimension': {
                        'productType': {
                            'level': 'LEVEL2'
                        }
                    },
                    'isTargeted': True,
                },
            ],
            'dimension': {
                'productType': {
                    'level': 'LEVEL1',
                    'value': 'my level1 type',
                }
            },
            'isTargeted':
            None,
        }],
        'dimension': {},
        'isTargeted':
        None,
    }

    is_targeted = False

    expected: product_util.ProductTargetingNode = {
        'children': [{
            'children': [
                {
                    'children': [],
                    'dimension': {
                        'productType': {
                            'level': 'LEVEL2'
                        }
                    },
                    'isTargeted': True,
                },
                {
                    'children': [],
                    'dimension': {
                        'productType': {
                            'level': 'LEVEL2',
                            'value': 'my level2 type',
                        }
                    },
                    'isTargeted': False,
                },
            ],
            'dimension': {
                'productType': {
                    'level': 'LEVEL1',
                    'value': 'my level1 type',
                }
            },
            'isTargeted':
            None,
        }],
        'dimension': {},
        'isTargeted':
        None,
    }

    product_util.build_product_group_tree(dimensions, root, is_targeted)

    self.assertEqual(expected, root)

  def test_protobuf_enum_matching_and_type_preservation(self):
    """Verifies that our enum-aware _val helper correctly handles native Protobuf enums."""
    # Construct a real schema_pb2.WideProduct containing nested Protobufs
    wide = schema_pb2.WideProduct()
    wide.channel = 'online'
    wide.product.product_attributes.availability = 1 # 'IN_STOCK' Enum value (internally int 1)
    wide.product.product_attributes.condition = 1 # 'NEW' Enum value (internally int 1)
    wide.product.product_attributes.brand = 'TestBrand'
    
    # Verify set_product_in_stock matches "IN_STOCK" string against integer enum 1 correctly
    wide = product_util.set_product_in_stock(wide)
    self.assertTrue(wide.in_stock)

    # Verify set_product_approved matches "SHOPPING_ADS" enum 1 correctly
    status_msg = wide.status.destination_statuses.add()
    status_msg.reporting_context = 1 # 'SHOPPING_ADS' Enum value 1
    status_msg.approved_countries.append('US')
    wide = product_util.set_product_approved(wide)
    self.assertEqual(list(wide.approved_countries), ['US'])

    # Verify dimension_matches_product matches condition enum correctly without raising AttributeError
    dimension = {
        'productCondition': {
            'condition': 'NEW'
        }
    }
    self.assertTrue(
        product_util.dimension_matches_product(wide, dimension, {})
    )

if __name__ == '__main__':
  absltest.main()


