import requests
from bs4 import BeautifulSoup

# URL of Erling Haaland's profile
url = "https://fbref.com/en/players/1f44ac21/Erling-Haaland"

# Send HTTP request
response = requests.get(url, verify=False)
soup = BeautifulSoup(response.text, "html.parser")

# Extract name
name_tag = soup.find("h1")
name = name_tag.find("span").text if name_tag else "N/A"

# Extract birth info
birth_tag = soup.find("span", id="necro-birth")
birth_date = birth_tag.text.strip() if birth_tag else "N/A"
birth_place = (
    birth_tag.find_next("span").find_next("span").text.strip() if birth_tag else "N/A"
)

# Extract wages
wages_tag = soup.find("span", style=lambda value: value and "color:#932a12" in value)
weekly_wage = wages_tag.text.strip() if wages_tag else "N/A"

# Extract Instagram
insta_tag = soup.find("a", href=lambda href: href and "instagram.com" in href)
instagram = insta_tag["href"] if insta_tag else "N/A"

# Extract recognitions
bling_section = soup.find("span", id="bling-alt-text")
recognitions = []
if bling_section and bling_section.string:
    recognitions = [
        line.strip("* ").strip()
        for line in bling_section.string.strip().split("\n")
        if line.strip()
    ]

# Display results
player_info = {
    "name": name,
    "birth_date": birth_date,
    "birth_place": birth_place,
    "weekly_wage": weekly_wage,
    "instagram": instagram,
    "recognitions": recognitions,
}

for key, value in player_info.items():
    print(f"{key}: {value if not isinstance(value, list) else ''}")
    if isinstance(value, list):
        for item in value:
            print(f"  - {item}")
