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

import io
import unittest
from absl.testing import absltest
import gcsfs

class TestGcsfsUpload(absltest.TestCase):
    def test_streaming_upload_aligns_non_final_chunk(self):
        # We simulate a large upload using the exact bug conditions:
        # A file size that leaves a non-aligned remainder (e.g., 5MB + 7122 bytes)
        overflow = 7122
        buffer_size = gcsfs.core.DEFAULT_BLOCK_SIZE + overflow
        
        class MockGCSFileSystem:
            def __init__(self):
                self.calls = []

            def call(self, method, location, headers=None, data=None):
                length = int(headers["Content-Length"])
                self.calls.append((headers, data))
                # Validate the chunk alignment requirement
                assert length % gcsfs.core.GCS_MIN_BLOCK_SIZE == 0
                return {"Range": f"bytes=0-{length - 1}"}, None

        fs = MockGCSFileSystem()
        
        # Instantiate GCSFile bypassing normal init to inject our mock
        f = gcsfs.core.GCSFile.__new__(gcsfs.core.GCSFile)
        f.buffer = io.BytesIO(b"x" * buffer_size)
        f.offset = 0
        f.autocommit = True
        f.content_type = "application/octet-stream"
        f.location = "mock-location"
        f.gcsfs = fs
        f.checker = gcsfs.checkers.get_consistency_checker(None)

        # Trigger chunk upload (simulating an intermediate flush)
        f._upload_chunk(final=False)
        
        # Verify that the chunk was aligned and the remainder was kept in the buffer
        self.assertEqual(len(fs.calls), 1)
        self.assertEqual(len(fs.calls[0][1]), gcsfs.core.DEFAULT_BLOCK_SIZE)
        self.assertEqual(f.buffer.tell(), overflow)

if __name__ == '__main__':
    absltest.main()
