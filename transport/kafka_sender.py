#!/usr/bin/python3.6

import requests
import sys

# ---- KONFIGURACE ----
BASE_URL = "http://localhost:6996/api/management/kafka/web/kafka/send"

# Endpointy, které chceš volat
ENDPOINTS = [
    "operators",
    "tokens",
    "terminals",
    "taps",
    "paywindows",
    "authtxs",
    "clearing-days",
    "clearing-items",
    "audits"
]


HEADERS = {
    "accept": "*/*",
    "Authorization": "Basic bWdtdDptZ210"
}

# INPUT
while True:
    operator_input = input("Zadej operator ID (oddělené čárkou): ").strip()
    try:
        # validace – převede na int a zpět, aby se odstranily případné mezery a nesmysly
        operator_ids = [str(int(x.strip())) for x in operator_input.split(",") if x.strip()]
        break
    except ValueError:
        print("⚠️ Zadávej pouze čísla oddělená čárkou. Zkus to znovu.\n")

# zobrazí seznam operátorů a vyžádá si potvrzení
print(f"\nSpustím {len(ENDPOINTS)} endpointů pro operátory: {', '.join(operator_ids)}")
confirm = input("Pokračovat? (y/n): ").strip().lower()
if confirm != "y":
    print("❌ Přerušeno uživatelem.")
    sys.exit(0)

dttm_from = input("Zadej dttmFrom (např. 2025-09-01T00:01:23): ").strip()
dttm_to = input("Zadej dttmTo (např. 2025-09-28T23:52:23): ").strip()

print(f"\n▶️ Spouštím endpoint {len(ENDPOINTS)} pro operator IDs : {len(operator_ids)}\n")

# ---- LOGIKA ----
for operator_id in operator_ids:
    for endpoint in ENDPOINTS:
        url = f"{BASE_URL}/{endpoint}"
        params = {
            "dttmFrom": dttm_from,
            "dttmTo": dttm_to,
            "opearatorId": operator_id  # (ponechávám překlep, pokud API vyžaduje přesně tohle)
        }

        print(f"▶️ Volám {endpoint} pro operatorId={operator_id}...")
        try:
            response = requests.post(url, headers=HEADERS, params=params)
            if response.status_code == 200:
                print(f"✅ OK ({response.status_code})")
            else:
                print(f"❌ CHYBA: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"💥 CHYBA PŘIPOJENÍ: {e}")

print("\n✅ Hotovo.")





