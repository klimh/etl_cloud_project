import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta

def generate_csv_data(filename="raw_weather_logs.csv", num_rows=10000):
    print(f"Generowanie {num_rows} wierszy logów...")
    
    cities = ["Warszawa", "Krakow", "Wroclaw", "Gdansk", "Poznan", "Lodz", "Szczecin", "Lublin", None] # Dodajemy None dla błędnych danych
    descriptions = ["słonecznie", "pochmurno", "deszczowo", "śnieg", "burza", "mgła"]
    
    data = []
    now = datetime.utcnow()
    
    for i in range(num_rows):
        # Symulacja historycznych dat
        time_offset = timedelta(minutes=random.randint(0, 43200)) # Ostatnie 30 dni
        record_time = now - time_offset
        
        city = random.choice(cities)
        
        # 5% szansy na błędną temperaturę (string zamiast liczby lub absurdalna wartość)
        if random.random() < 0.05:
            temp = "brak_danych" if random.random() < 0.5 else random.uniform(100, 500)
        else:
            temp = round(random.uniform(-20.0, 35.0), 2)
            
        desc = random.choice(descriptions)
        
        row = {
            "id": str(uuid.uuid4()),
            "timestamp": record_time.isoformat(),
            "city": city,
            "temperature": temp,
            "description": desc
        }
        data.append(row)
        
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Zapisano plik: {filename} z {len(df)} rekordami.")

if __name__ == "__main__":
    generate_csv_data()
