from typing import Optional


class RequestException(Exception):

    http_status: Optional[int] = None

    def __init__(self, status=None, url=None, reason=None, response_text=None, message=None):
        self.status = status or self.http_status
        self.url = url
        self.reason = reason
        self.response_text = response_text
        self.message = message
        if message:
            super().__init__(f'{message}: {status} ({reason}) - {url}: {response_text}')
        else:
            super().__init__(f'{status} ({reason}) - {url}: {response_text}')

    @classmethod
    async def init_from_response(cls, response, message=None):
        try:
            txt = await response.text()
        except:
            pass

        return cls(status=response.status,
                   url=str(response.url),
                   reason=response.reason,
                   response_text=txt,
                   message=message)


class RetriableAPIException(RequestException):
    pass
