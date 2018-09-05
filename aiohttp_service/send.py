import asyncio
import json
import aioamqp


async def send(payload):
    transport, protocol = await aioamqp.connect()
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='screenshot-task')

    await channel.basic_publish(
        payload=payload,
        exchange_name='',
        routing_key='screenshot-task'
    )

    print(f" [x] Sent {payload}")
    await protocol.close()
    transport.close()


if __name__ == '__main__':
    payload = {
        'title': 'Spotify_Screenshot.png',
        'id': 'id_spotify',
        'url': 'https://stage.atlasense.com/api/rel0/account3/nil@onna.com/b4b6cd2b25234c1e985f4f3797ce4e61/DB463AZUK20180829',
        'url_screenshot': 'https://www.spotify.com',
        'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1MzYxNTIxOTAsImV4cCI6MTUzNjc1Njk5MCwidG9rZW4iOiI3OWQ1YmIwMWY2N2I0NjgyOWE0ZGU2ZWZkOTI1MjE1NyIsImxvZ2luIjoibmlsQG9ubmEuY29tIiwibmFtZSI6Im5pbEBvbm5hLmNvbSIsInN1cGVydXNlciI6ZmFsc2V9.aElm5SCXZ7XFJjmKdKzPD94Lb2pM6Wwkeqd_YT0XHBE'
    }
    asyncio.get_event_loop().run_until_complete(send(json.dumps(payload)))
