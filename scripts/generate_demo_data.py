"""
N-BeMod — Dataset demo sintético
Genera 400 filas con 3 portfolios, 2 productos, 5 segmentos.
Ejecutar: python -m scripts.generate_demo_data
"""
import random
import pandas as pd
import numpy as np
from datetime import date

random.seed(42)
np.random.seed(42)

AS_OF_DATE = "2024-12-31"
ENTITY_ID = "entity_demo_001"
PORTFOLIOS = ["MORTGAGES", "CONSUMER", "CORPORATE"]
PRODUCTS = ["FIXED_RATE", "VARIABLE_RATE"]
SEGMENTS = ["PRIME", "NEAR_PRIME", "SUBPRIME", "BUY_TO_LET", "FIRST_TIME_BUYER"]

rows = []
for _ in range(400):
    portfolio = random.choice(PORTFOLIOS)
    product = random.choice(PRODUCTS)
    segment = random.choice(SEGMENTS)

    balance = round(np.random.lognormal(mean=11, sigma=1.2), 2)  # ~€50k–€2M range
    rate = round(np.random.uniform(0.01, 0.08), 4) if product == "FIXED_RATE" else round(np.random.uniform(0.005, 0.06), 4)
    origination_years_ago = random.randint(0, 20)
    maturity_years = random.randint(5, 30)

    rows.append({
        "as_of_date": AS_OF_DATE,
        "entity_id": ENTITY_ID,
        "contract_id": f"LOAN-{_:05d}",
        "portfolio": portfolio,
        "product": product,
        "segment": segment,
        "balance": balance,
        "rate": rate,
        "origination_date": str(date(2024 - origination_years_ago, random.randint(1, 12), 1)),
        "maturity_date": str(date(2024 + maturity_years, random.randint(1, 12), 1)),
    })

df = pd.DataFrame(rows)
out_path = "data/demo_loans.csv"
import os
os.makedirs("data", exist_ok=True)
df.to_csv(out_path, index=False)
print(f"Demo dataset saved: {out_path} ({len(df)} rows)")
print(df.groupby(["portfolio", "product"])["balance"].agg(["count", "sum"]).round(0))
