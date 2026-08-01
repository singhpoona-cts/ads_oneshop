# Copyright 2026 Google LLC
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
"""Merchant Center *product* ingestion via the Merchant API (stable v1).

This replaces the old per-leaf Content API `products.list` + `productstatuses.list` pulls
as part of the Content API -> Merchant API migration.

In the Merchant API the processed `Product` resource carries BOTH the offer
attributes (`product_attributes`) AND the status (`product_status`), so the old
two-collection model (and the Beam products<->statuses join) collapses into one
`accounts/{account}/products` list call.

Output is written in the NATIVE v1 shape
(snake_case keys, enum NAMES as strings)-- one `Product` per line -- to
``<mc_path>/<account_id>/products/rows.jsonlines``, preserving the existing
BigQuery glob (``merchant_center/*/products/*.jsonlines``).
The Beam stage (`create_base_tables.py`) splits `product_status`
out, derives the `channel` dimension, and joins Ads targeting.
"""

from concurrent import futures
import json
from typing import Any

from absl import logging
from etils import epath
from google.protobuf import json_format
from google.shopping import merchant_products_v1 as mp

# Key stamped onto every row so the Beam stage knows the source account. Mirrors
# resource_downloader.METADATA_KEY so downstream code is unchanged.
METADATA_KEY = 'downloaderMetadata'

_PAGE_SIZE = 250


def _to_dict(msg) -> Any:
  """Proto -> dict in native v1 shape.

  Args:
    msg: The protobuf message to convert into a dictionary.

  Returns:
    A dictionary representation of the proto message in native v1 shape with
    snake_case keys and string enum names, or None if the input message is
    None.
  """

  if msg is None:
    return None
  # Extract the underlying native protobuf if it's a proto-plus wrapper
  pb_msg = type(msg).pb(msg) if hasattr(type(msg), 'pb') else msg
  return json_format.MessageToDict(
      pb_msg,
      preserving_proto_field_name=True,
      use_integers_for_enums=False,
  )


def _list_leaf_products(client, account_id):
  """Yields all v1 products for one leaf account, as native-shape dicts.

  This is a *generator*, deliberately. The GAPIC pager already fetches pages
  lazily, so yielding each product as it is converted keeps peak memory at
  roughly one product per worker thread. Accumulating into a list instead
  would hold the entire catalog at once -- a large merchant can have millions
  of products, and with `_MAX_WORKERS` accounts downloading concurrently that
  is an OOM rather than a slowdown.

  Callers must therefore consume the result exactly once, while streaming it
  to its destination (see `download_products._process`).

  Args:
    client: The API client instance used to fetch the product catalog.
    account_id: The ID of the leaf merchant account.

  Yields:
    Native-shape dicts representing individual v1 products.
  """
  parent = f'accounts/{account_id}'
  pager = client.list_products(request=mp.ListProductsRequest(
      parent=parent, page_size=_PAGE_SIZE))
  for product in pager:
    yield product


def download_products(credentials,
                      account_ids,
                      mc_path,
                      max_workers=None):
  """Downloads products from Merchant API v1 and writes per-account files.

  Args:
    credentials: Google credentials (same as used for the Ads/Content APIs).
    account_ids: iterable of leaf/standalone Merchant Center account IDs to
      pull.
    mc_path: epath.Path to the merchant_center output directory.
    max_workers: max concurrent account downloads. Callers (namely `acit.py`)
      should pass this explicitly so it stays consistent with the other
      Merchant API ingestion stages.
  """
  client = mp.ProductsServiceClient(credentials=credentials)
  account_ids = list(account_ids)

  def _process(account_id):
    output_file = (epath.Path(mc_path) / account_id / 'products' /
                   'rows.jsonlines')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    # Stream page-by-page straight to disk rather than materializing the
    # account's whole catalog first; `_list_leaf_products`
    # is a generator and must be consumed lazily to keep that guarantee.
    count = 0
    with output_file.open('w') as f:
      for product in _list_leaf_products(client, account_id):
        d = _to_dict(product)
        d[METADATA_KEY] = {'accountId': account_id}
        f.write(json.dumps(d) + '\n')
        count += 1
    return account_id, count

  total = 0
  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {ex.submit(_process, aid): aid for aid in account_ids}
    for done in futures.as_completed(future_to_id):
      account_id, n = done.result()  # surface exceptions
      total += n
      logging.info('Wrote %d product(s) for %s', n, account_id)

  logging.info('Merchant API products: %d account(s), %d product(s) total',
               len(account_ids), total)
