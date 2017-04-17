"""S3 Stream Uploader

This module provides the ability to upload an S3 object
as if you were working with a normal file stream.
"""

# Python Imports
import boto3
from io import BytesIO
import logging
import six

logger = logging.getLogger(__name__)

S3_MIN_PART_BYTES = 5 * 1024**2
"""int: Minimum part size required by AWS for S3 multipart uploads (5Mb)
"""


class S3StreamWriter(object):
    """A simple context manager for 'streaming' files INTO S3.

    Input writes are uploaded in parts, as soon as the `min_part_bytes` number
    of bytes have been accumulated. The minimum part size allowed by AWS S3
    is 5MB, and serves as the default.

    Args:
        bucket_name (str): Name of S3 bucket to uploaded file.
        key_name (str): Name of S3 object key used to save file.
        min_part_bytes (int, optional): Custom part size limit for multipart upload.
        **s3_kwargs (:obj:`str`, optional): Keyword arguments for the S3 upload.

    Attributes:
        client (:obj): The S3 boto client.
        bucket_name (str): Name of S3 bucket to uploaded file.
        key_name (str): Name of S3 object key used to save file.
        min_part_bytes (int): Minimum part size for part upload.

    Raises:
        ValueError: If `min_part_bytes` is less than `S3_MIN_PART_BYTES`
        NotImplementedError: If you try to `seek` the file

    Examples:
        >>> with S3StreamWriter('bucket', 'key', ServerSideEncryption='AES256') as file_obj:
        >>>     writer = csv.writer(file_obj)
        >>>     for row in some_reader_obj:
        >>>         writer.writerow(row)
    """

    def __init__(self, bucket_name, key_name, min_part_bytes=S3_MIN_PART_BYTES, **s3_kwargs):
        if min_part_bytes < S3_MIN_PART_BYTES:
            raise ValueError('`min_part_bytes` less than the S3 minimum part size of 5MB')

        client = boto3.client('s3')
        self.client = client
        self.bucket_name = bucket_name
        self.key_name = key_name
        self.min_part_bytes = min_part_bytes

        # initialize mulitpart upload
        multipart = client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key_name,
            **s3_kwargs
        )
        self.multipart_id = multipart['UploadId']

        # initialize stats
        self._multipart_parts = []
        self._current_part_lines = []
        self._current_part_bytes = 0
        self._total_bytes = 0
        self._total_parts = 0

    def __str__(self):
        return '%s <S3 Object: %s/%s>' % (self.__class__.__name__, self.bucket_name, self.key_name)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            self._abort('Exception while terminating upload: %s' % value)
            return False

        try:
            self.close()
        except:
            self._abort('Exception while terminating upload')
            raise

    def _upload_part(self):
        stream_buffer = b''.join(self._current_part_lines)
        self._total_parts += 1

        logger.info(
            'Uploading part #%s, %s bytes (total %.3f MB)' %
            (self._total_parts, len(stream_buffer), self._total_bytes / 1024.0 ** 2)
        )

        multipart_part = self.client.upload_part(
            Body=BytesIO(stream_buffer),
            Bucket=self.bucket_name,
            Key=self.key_name,
            PartNumber=self._total_parts,
            UploadId=self.multipart_id,
        )

        self._multipart_parts.append({
            'ETag': multipart_part['ETag'],
            'PartNumber': self._total_parts,
        })

        self._reset_part()

        logger.debug('upload of part #%i finished' % self._total_parts)

    def _reset_part(self):
        self._current_part_lines = []
        self._current_part_bytes = 0

    def _complete(self):
        self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.key_name,
            UploadId=self.multipart_id,
            MultipartUpload={
                'Parts': self._multipart_parts,
            },
        )
        logger.info('Successfully completed upload')

    def _abort(self, err_msg):
        logger.error(err_msg)
        logger.info('Aborting upload')
        self.client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.key_name,
            UploadId=self.multipart_id,
        )
        logger.info('Successfully aborted upload')

    def write(self, binary_value):
        """Prepares to write the provided binary string to the S3 Object.
        Values are stored in `_current_part_lines` until the threshold specified
        by `min_part_bytes` has been reached. Once reached, all pending
        `_current_part_lines` will be uploaded.

        Note:
            Data will not get written to S3 immediately.

        Args:
            binary_value: The next set of binary data to write to file

        Returns:
            None

        Raises:
            TypeError: If `binary_value` is not a Python (2 or 3) binary type
        """
        if isinstance(binary_value, six.text_type):
            binary_value = binary_value.encode('utf8')
        if not isinstance(binary_value, six.binary_type):
            raise TypeError('input must be a binary string')

        value_bytes = len(binary_value)
        self._current_part_lines.append(binary_value)
        self._current_part_bytes += value_bytes
        self._total_bytes += value_bytes

        if self._current_part_bytes >= self.min_part_bytes:
            self._upload_part()

    def seek(self, offset, whence=None):
        raise NotImplementedError('`seek` is not available')

    def close(self):
        """Write the given bytes (binary string) into the S3 file from constructor.
        Note there's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away.

        Args:
            None

        Returns:
            None
        """

        if not self._total_bytes:
            return self._abort('Received empty input')

        self._upload_part()
        self._complete()
