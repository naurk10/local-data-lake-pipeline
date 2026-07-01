import requests

TELEGRAM_TOKEN = '8867797595:AAF9_v3Shm7bEtl_CKC_zMH5FHnhzlu2qSI'
TELEGRAM_CHAT_ID = '2021727759'

url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
res = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': '🚀 Teste de Conexão Direta do Mac!'})

print("Status:", res.status_code)
print("Resposta:", res.text)