import asyncio
import aiohttp
from utils import logger, get_async_session, async_fetch_url
from bs4 import BeautifulSoup
import urllib.parse

# Test Setup
settings = {
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'retry_count': 1,
    'throttle': 0,
    'headers': {
        'Referer': 'http://www.nasgaweb.com/dbase/main.asp'
    }
}

ATHLETE_URL = "http://www.nasgaweb.com/dbase/resultsathlete3.asp"

async def test_hidden_params():
    session = await get_async_session(settings)
    try:
        base = "http://www.nasgaweb.com/dbase/resultsathlete3.asp"
        name = "Smith, John" # Use a generic name or "Dibbens, Aaron"
        encoded_name = urllib.parse.quote(name)
        
        # Combinations
        scenarios = [
            {"athletename": encoded_name, "type": "nasga"},
            {"athletename": encoded_name, "type": "nasga", "athleteyear": "2024"},
            {"athletename": encoded_name, "athleteyear": "2024"}
        ]
        
        for params in scenarios:
            query = "&".join([f"{k}={v}" for k, v in params.items()])
            url = f"{base}?{query}"
            print(f"Testing {url}")
            
            resp_text = await async_fetch_url(session, url, settings=settings)
            
            if resp_text and "Microsoft JET" not in resp_text:
                 print(f"SUCCESS with params {params.keys()}")
                 if "No Results Found" in resp_text:
                     print("  Result: No Results Found")
                 else:
                     print("  Result: Content found!")
                     print(resp_text[:200])
                 break
            else:
                 print(f"FAILURE with params {params.keys()} (500/Error)")

    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(test_hidden_params())
