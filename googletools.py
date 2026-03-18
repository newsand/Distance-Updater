import os
import requests

def buscar_google_maps(cidade_origem, uf_origem, cidade_destino, uf_destino):
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ ERRO: Configure GOOGLE_API_KEY!")
        return None
    
    # Limpa espaços
    cidade_origem = str(cidade_origem).strip() if cidade_origem else ""
    uf_origem = str(uf_origem).strip() if uf_origem else ""
    cidade_destino = str(cidade_destino).strip() if cidade_destino else ""
    uf_destino = str(uf_destino).strip() if uf_destino else ""
    
    if not all([cidade_origem, uf_origem, cidade_destino, uf_destino]):
        return None
    
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": f"{cidade_origem},{uf_origem},BR",
        "destinations": f"{cidade_destino},{uf_destino},BR",
        "key": api_key,
        "language": "pt-BR",
        "units": "metric"
    }
    
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.ok:
            data = resp.json()
            if data.get('status') == 'OK':
                elemento = data['rows'][0]['elements'][0]
                if elemento.get('status') == 'OK':
                    distancia_km = elemento['distance']['value'] / 1000
                    return round(distancia_km, 1)
    except Exception as e:
        print(f"Erro: {e}")
    
    return None