# -*- coding: utf-8 -*-
# @Time     :2025/3/3 17:00
# @Author   :senjataik
# @File     :main.py
# @Software :PyCharm
import asyncio
import random
import ssl
import json
import time
import uuid
import aiohttp
import aiofiles
from loguru import logger
from websockets_proxy import Proxy, proxy_connect

# Configuration
import os
MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "50"))

MIN_SLEEP = 1
MAX_SLEEP = 10
PING_INTERVAL = 20
USER_AGENT_ROTATION_INTERVAL = 3600  # Rotate user agents every hour
PROXY_REFRESH_INTERVAL = 3600       # Refresh proxies every hour
MAX_FAILED_ATTEMPTS = 3             # Max failed attempts before removing proxy
MIN_WORKING_PROXIES = 10            # Minimum working proxies before refresh

# List of user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Mobile Safari/537.36"
]

async def fetch_proxies():
    proxy_urls = {
        "http": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "socks4": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "socks5": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"
    }
    proxies = []
    try:
        async with aiohttp.ClientSession() as session:
            for protocol, url in proxy_urls.items():
                async with session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        for line in text.splitlines():
                            if line.strip():
                                proxies.append(f"{protocol}://{line.strip()}")
                    else:
                        logger.error(f"Failed to fetch {protocol} proxies: {response.status}")
    except Exception as e:
        logger.error(f"Error fetching proxies: {e}")
    return proxies

async def read_user_id():
    try:
        async with aiofiles.open("user_id.txt", mode='r') as file:
            user_id = await file.read()
            return user_id.strip()
    except FileNotFoundError:
        logger.error("user_id.txt file not found in current directory")
        return None
    except Exception as e:
        logger.error(f"Error reading user_id: {e}")
        return None


async def connect_to_wss(proxy, user_id, proxy_stats):
    device_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, proxy))
    logger.info(f"\nStarting connection with:\n"
               f"Device ID: {device_id}\n"
               f"User ID: {user_id}\n"
               f"Proxy: {proxy}\n")
    
    last_rotation = time.time()
    current_user_agent = random.choice(USER_AGENTS)
    
    while True:
        try:
            # Rotate user agent periodically
            if time.time() - last_rotation > USER_AGENT_ROTATION_INTERVAL:
                current_user_agent = random.choice(USER_AGENTS)
                last_rotation = time.time()
                logger.info(f"Rotated user agent to: {current_user_agent}")

            custom_headers = {
                "User-Agent": current_user_agent
            }
            
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            uri = "wss://proxy.wynd.network:4650/"
            server_hostname = "proxy.wynd.network"
            proxy_obj = Proxy.from_url(proxy)
            
            async with proxy_connect(uri, proxy=proxy_obj, ssl=ssl_context, 
                                    server_hostname=server_hostname, extra_headers=custom_headers) as websocket:
                # Reset failed attempts on successful connection
                proxy_stats[proxy] = 0
                
                async def send_ping():
                    while True:
                        send_message = json.dumps({
                            "id": str(uuid.uuid4()),
                            "version": "1.0.0",
                            "action": "PING",
                            "data": {}
                        })
                        logger.debug(f"Sending PING: {send_message}")
                        await websocket.send(send_message)
                        await asyncio.sleep(PING_INTERVAL)
                
                # Start ping task
                asyncio.create_task(send_ping())
                
                # Handle incoming messages
                while True:
                    response = await websocket.recv()
                    message = json.loads(response)
                    logger.info(f"Received message: {message}")
                    
                    if message.get("action") == "AUTH":
                        auth_response = {
                            "id": message["id"],
                            "origin_action": "AUTH",
                            "result": {
                                "browser_id": device_id,
                                "user_id": user_id,
                                "user_agent": custom_headers['User-Agent'],
                                "timestamp": int(time.time()),
                                "device_type": "extension",
                                "version": "2.5.0"
                            }
                        }
                        logger.debug(f"Sending AUTH response: {auth_response}")
                        await websocket.send(json.dumps(auth_response))
                    
                    elif message.get("action") == "PONG":
                        pong_response = {
                            "id": message["id"],
                            "origin_action": "PONG"
                        }
                        logger.debug(f"Sending PONG response: {pong_response}")
                        await websocket.send(json.dumps(pong_response))
        
        except Exception as e:
            logger.error(f"Connection error: {e}")
            logger.error(f"Proxy: {proxy}")
            
            # Track failed attempts
            proxy_stats[proxy] = proxy_stats.get(proxy, 0) + 1
            if proxy_stats[proxy] >= MAX_FAILED_ATTEMPTS:
                logger.warning(f"Removing proxy due to repeated failures: {proxy}")
            return
            
            await asyncio.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

async def main():
    user_id = await read_user_id()
    if not user_id:
        logger.error("No valid user_id found")
        return

    # Proxy selection menu
    print("\nMenu Utama:")
    print("0. Update Daftar Proxy")
    print("1. Menggunakan Proxy HTTP")
    print("2. Menggunakan Proxy SOCKS4") 
    print("3. Menggunakan Proxy SOCKS5")
    print("4. Menggunakan Semua Proxy")
    while True:
        choice = input("Masukkan pilihan (0-4): ")
        if choice == "0":
            logger.info("Memperbarui daftar proxy...")
            proxies = await fetch_proxies()
            proxy_types = {}
            for proxy in proxies:
                protocol = proxy.split("://")[0]
                proxy_types[protocol] = proxy_types.get(protocol, 0) + 1
            logger.info(f"Proxy berhasil diperbarui. Total proxy tersedia: {len(proxies)}")
            logger.info(f"Jenis proxy: {proxy_types}")
            continue
        if choice in ("1", "2", "3", "4"):
            break
        print("Pilihan tidak valid. Silakan masukkan angka antara 0-4.")


    last_proxy_update = time.time()
    proxies = await fetch_proxies()
    
    # Filter proxies based on user selection
    if choice == "1":
        proxies = [p for p in proxies if p.startswith("http://")]
    elif choice == "2":
        proxies = [p for p in proxies if p.startswith("socks4://")]
    elif choice == "3":
        proxies = [p for p in proxies if p.startswith("socks5://")]
    elif choice != "4":
        logger.error("Pilihan tidak valid, menggunakan semua proxy")

    proxy_stats = {}  # Track failed attempts per proxy
    
    while True:
        # Refresh proxies periodically or if too many are removed
        if (time.time() - last_proxy_update > PROXY_REFRESH_INTERVAL or 
            len(proxies) < MIN_WORKING_PROXIES):
            logger.info("Refreshing proxy list...")
            proxies = await fetch_proxies()
            last_proxy_update = time.time()
            proxy_stats = {}  # Reset stats after refresh
            logger.info(f"Proxies last updated at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_proxy_update))}")

        if not proxies:
            logger.warning("No proxies available, retrying in 60 seconds...")
            await asyncio.sleep(60)
            continue

        # Remove failed proxies
        proxies = [proxy for proxy in proxies if proxy_stats.get(proxy, 0) < MAX_FAILED_ATTEMPTS]

        # Display proxy type distribution
        proxy_types = {}
        for proxy in proxies:
            protocol = proxy.split("://")[0]
            proxy_types[protocol] = proxy_types.get(protocol, 0) + 1
        
        logger.info(f"\nStarting Grass Bot with:\n"
                  f"User ID: {user_id}\n"
                  f"Total proxies available: {len(proxies)}\n"
                  f"Proxy types: {proxy_types}\n"
                  f"Selected proxy type: {'HTTP' if choice == '1' else 'SOCKS4' if choice == '2' else 'SOCKS5' if choice == '3' else 'All'}\n"

                  f"Proxies last updated: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_proxy_update))}\n"
                  f"Max concurrent connections: {MAX_CONNECTIONS}\n")

        # Limit the number of concurrent connections
        selected_proxies = proxies[:MAX_CONNECTIONS]
        logger.info(f"Starting {len(selected_proxies)} connections")
        
        tasks = [asyncio.ensure_future(connect_to_wss(proxy, user_id, proxy_stats)) 
                for proxy in selected_proxies]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error in main task: {e}")
            await asyncio.sleep(60)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
