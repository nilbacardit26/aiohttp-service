import base64
import aiohttp
import logging
import backoff
from typing import Any, AsyncIterable, Tuple
from exceptions import RetriableAPIException

CHUNK_SIZE = 1204 * 1024 * 5  # 5MB
RETRIABLE_STATUS_CODES = (500, 502, 503, 504, 400, 408)
MAX_NUM_OF_RETRIES = 5
RETRIABLE_EXCEPTIONS = (aiohttp.client_exceptions.ClientOSError,
                        aiohttp.client_exceptions.ClientPayloadError,
                        aiohttp.client_exceptions.ClientConnectorError,
                        aiohttp.client_exceptions.ClientConnectionError)


async def async_gen_lookahead(gen: AsyncIterable) -> AsyncIterable[Tuple[bool, Any]]:
    """
    This async generator returns a tuple for each element in `gen`: (is_last_element, element)
    """
    async for prev in gen:
        async for el in gen:
            yield False, prev
            prev = el

        yield True, prev


class UploadTUS:

    def __init__(self, token):
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Authorization": f"Bearer {token}"
        }

    def basic_headers(self):
        return self.headers

    async def upload(self, generator, name, url, file_attr=None):
        """
        This method upload a file to canonical using TUS protocol.
        Beats processing is disabled!
        """
        # POST @tusupload/file
        name = bytes(name, encoding='utf-8')
        if not file_attr:
            tus_url = url + '/@tusupload/file'
        else:
            tus_url = url + '/@tusupload/' + file_attr

        tus_post = self.basic_headers()
        metadata = 'filename ' + base64.b64encode(name).decode('utf-8')
        decoded_name = name.decode('utf-8')
        tus_post.update({
            'TUS-RESUMABLE': '1.0.0',
            'UPLOAD-DEFER-LENGTH': '1',
            'UPLOAD-METADATA': metadata,
            'UPLOAD-EXTENSION': decoded_name.split('.')[-1]
        })

        resp = await self._tus_create(tus_url, tus_post)
        if resp.status not in [200, 201, 204]:
            text = await resp.text()
            raise Exception(f'Unexpected response code {resp.status} starting '
                            f'tus upload: {text}')
        tus_url = resp.headers['Location']

        tus_patch = self.basic_headers()
        tus_patch['X-Disable-Beats'] = 'true'

        tus_patch.update({
            'TUS-RESUMABLE': '1.0.0',
            'CONTENT-LENGTH': '0',
            'UPLOAD-OFFSET': '0',
            'CONTENT-TYPE': 'application/offset+octet-stream'
        })

        upload_offset = 0
        total_size = 0
        chunk = []

        async for is_last_chunk, next_chunk in async_gen_lookahead(generator):
            total_size += len(next_chunk)

            if is_last_chunk:
                tus_patch['UPLOAD-LENGTH'] = str(total_size)

            if chunk:
                chunk += next_chunk
            else:
                chunk = next_chunk

            while len(chunk) > CHUNK_SIZE or (is_last_chunk and chunk):
                partial_chunk = chunk[:CHUNK_SIZE]

                self.logger.debug(f'Uploading chunk [{decoded_name}]: '
                                  f'{len(partial_chunk) / 1024} / {total_size / 1024} KB')
                has_been_uploaded, upload_offset = await self._upload_chunk(partial_chunk,
                                                                            upload_offset,
                                                                            tus_url,
                                                                            tus_patch)

                if has_been_uploaded < 2 * 1024 * 1024 and not is_last_chunk:
                    self.logger.warning(f'It seems that the server has only ingested '
                                        f'{has_been_uploaded / 1024} KB')

                chunk = chunk[has_been_uploaded:]

    async def _upload_chunk(self, buf, upload_offset, tus_url, tus_patch):
        tus_patch['CONTENT-LENGTH'] = str(len(buf))
        tus_patch['UPLOAD-OFFSET'] = str(upload_offset)
        resp = await self._tus_upload_chunk(tus_url, tus_patch, buf)
        if resp.status not in [200, 201, 204]:
            text = await resp.text()
            raise Exception(f'Unexpected response code {resp.status} '
                            f'from uploading chunk: {text}')
        new_upload_offset = int(resp.headers['Upload-Offset'])
        has_been_uploaded = new_upload_offset - upload_offset
        return has_been_uploaded, new_upload_offset

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_NUM_OF_RETRIES)
    async def _tus_create(self, url, headers):
        skip = ['CONTENT-TYPE']
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url,
                    headers=headers,
                    skip_auto_headers=skip) as resp:
                if resp.status in RETRIABLE_STATUS_CODES:
                    raise await RetriableAPIException.init_from_response(resp)
                return resp

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_NUM_OF_RETRIES)
    async def _tus_upload_chunk(self, url, headers, data):
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                    url,
                    headers=headers,
                    data=data) as resp:
                if resp.status in RETRIABLE_STATUS_CODES:
                    raise await RetriableAPIException.init_from_response(resp)
                return resp
