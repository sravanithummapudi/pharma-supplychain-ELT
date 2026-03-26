import pandas as pd
import random

df = pd.read_csv("SCMS_Delivery_History_Dataset.csv")

# Clean column names for GCP/BigQuery
df.columns = (
    df.columns.str.strip()
              .str.lower()
              .str.replace(' ', '_')
              .str.replace(r'[()/]', '', regex=True)
              .str.replace(r'[#]', 'no', regex=True)
              .str.replace(r'[^0-9a-zA-Z_]', '_', regex=True)
)

# Add synthetic PII
def fake_email(name): return f"{name.lower().replace(' ', '')}@example.com"
def fake_phone(): return f"+91{random.randint(6000000000, 9999999999)}"
def fake_aadhaar(): return f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
def fake_dob(): return f"{random.randint(1960, 2000)}-{random.randint(1,12):02}-{random.randint(1,28):02}"

df['name'] = df['country'] + ' Pharma Ltd'
df['email'] = df['name'].apply(fake_email)
df['phone'] = df['name'].apply(lambda x: fake_phone())
df['aadhaar'] = df['name'].apply(lambda x: fake_aadhaar())
df['dob'] = df['name'].apply(lambda x: fake_dob())

# Save cleaned + enriched version
df.to_csv("scms-pii.csv", index=False)
