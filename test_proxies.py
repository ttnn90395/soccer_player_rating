import requests

PROXIES = [
    "http://104.18.236.155:80",
    "http://193.142.39.249:8085",
    "http://104.18.45.51:80",
    "http://104.19.90.154:80",
    "http://172.67.30.70:80",
    "http://162.159.247.50:80",
    "http://185.162.229.146:80",
]

TEST_URL = "https://httpbin.org/ip"
TIMEOUT = 5  # seconds

for proxy in PROXIES:
    proxies = {"http": proxy, "https": proxy}
    try:
        response = requests.get(TEST_URL, proxies=proxies, timeout=TIMEOUT)
        print(f"Proxy OK: {proxy} → IP seen: {response.json()['origin']}")
    except Exception as e:
        print(f"Proxy Failed: {proxy} → {e}")
