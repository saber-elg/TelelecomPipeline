import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

# ----- Moroccan Name Generator (30 male + 30 female + 30 last names) -----
MOROCCAN_FIRST_NAMES_MALE = [
    "Mohamed", "Ahmed", "Youssef", "Mustapha", "Hassan", "Karim", "Mehdi", "Bilal", "Omar", "Ibrahim",
    "Adil", "Anas", "Hamza", "Rachid", "Samir", "Walid", "Zakaria", "Abdel", "Amine", "Ayoub",
    "Hicham", "Imad", "Jamal", "Khalid", "Marouane", "Nabil", "Othman", "Reda", "Said", "Tarik"
]

MOROCCAN_FIRST_NAMES_FEMALE = [
    "Fatima", "Amina", "Khadija", "Zahra", "Hafsa", "Soukaina", "Asmae", "Leila", "Naima", "Samira",
    "Yasmina", "Hayat", "Nadia", "Farida", "Hanane", "Latifa", "Malika", "Noura", "Rachida", "Salma",
    "Sanaa", "Zineb", "Bouchra", "Imane", "Jamila", "Kawtar", "Laila", "Meryem", "Najat", "Saida"
]

MOROCCAN_LAST_NAMES = [
    "Alaoui", "Benjelloun", "El Fassi", "Bennani", "Cherkaoui", "Amrani", "Khalfi", "Rami", "Saidi", "El Khattabi",
    "Mernissi", "Berrada", "Chraibi", "Daoudi", "El Yazidi", "Fassi", "Ghannam", "Hakimi", "Idrissi", "Jazouli",
    "Kabbaj", "Lahlou", "Mahfoud", "Naciri", "Ouazzani", "Qasmi", "Rhouzi", "Sefrioui", "Tazi", "Zerouali"
]

def generate_moroccan_name():
    gender = random.choice(['male', 'female'])
    if gender == 'male':
        first_name = random.choice(MOROCCAN_FIRST_NAMES_MALE)
    else:
        first_name = random.choice(MOROCCAN_FIRST_NAMES_FEMALE)
    last_name = random.choice(MOROCCAN_LAST_NAMES)
    return f"{first_name} {last_name}"

# ----- Database Configuration -----
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'telecomdb',
    'user': 'postgres',
    'password': '0000'
}

# ----- Database Connection -----
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ----- Table Creation -----

# 1. Product Catalog Table
create_product_catalog_query = """
CREATE TABLE IF NOT EXISTS product_catalog (
    product_code VARCHAR(20) PRIMARY KEY,
    service_type VARCHAR(10) NOT NULL,
    unit VARCHAR(10) NOT NULL,
    rate_type VARCHAR(10) NOT NULL,
    description TEXT
);
"""

# 2. Rate Plans Table
create_rate_plans_query = """
CREATE TABLE IF NOT EXISTS rate_plans (
    rate_plan_id VARCHAR(20) NOT NULL,
    product_code VARCHAR(20) NOT NULL,
    service_type VARCHAR(10) NOT NULL,
    unit_price NUMERIC(6,3) NOT NULL,
    free_units INTEGER,
    tier_threshold INTEGER,
    tier_price NUMERIC(6,3),
    PRIMARY KEY (rate_plan_id, product_code),
    FOREIGN KEY (product_code) REFERENCES product_catalog(product_code)
);
"""

# 3. Customer Subscriptions Table (updated)
create_customer_subscriptions_query = """
CREATE TABLE IF NOT EXISTS customer_subscriptions (
    customer_id VARCHAR(15) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    activation_date DATE NOT NULL,
    subscription_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    region VARCHAR(100),
    rate_plan_id VARCHAR(20),
    FOREIGN KEY (rate_plan_id) REFERENCES rate_plans(rate_plan_id)
);
"""

# Execute table creation
cur.execute(create_product_catalog_query)
cur.execute(create_rate_plans_query)
cur.execute(create_customer_subscriptions_query)
conn.commit()
print("✅ All tables created successfully")

# ----- Data Insertion -----

# Insert Product Catalog Data
product_catalog_data = [
    ('DATA_STD', 'data', 'MB', 'flat', 'Connexion Internet standard (facturation au Mo)'),
    ('DATA_ULTRA', 'data', 'MB', 'tiered', 'Connexion Internet haut débit (palier gratuit + tarification progressive)'),
    ('SMS_STD', 'sms', 'sms', 'flat', 'Envoi SMS standard (tarif unitaire par SMS)'),
    ('VOICE_INT', 'voice', 'second', 'flat', 'Appel vocal international (facturation à la seconde)'),
    ('VOICE_NAT', 'voice', 'second', 'flat', 'Appel vocal local (facturation à la seconde)')
]

for product in product_catalog_data:
    cur.execute(
        """
        INSERT INTO product_catalog (product_code, service_type, unit, rate_type, description)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_code) DO NOTHING
        """,
        product
    )

# Insert Rate Plans Data
rate_plans_data = [
    # Gold Plan
    ('Gold', 'DATA_STD', 'data', 0.008, 200, None, None),
    ('Gold', 'DATA_ULTRA', 'data', 0.003, 250, 1000, 0.001),
    ('Gold', 'SMS_STD', 'sms', 0.075, 1, None, None),
    ('Gold', 'VOICE_INT', 'voice', 0.180, 10, None, None),
    ('Gold', 'VOICE_NAT', 'voice', 0.035, 60, None, None),
    
    # Silver Plan
    ('Silver', 'DATA_STD', 'data', 0.010, 100, None, None),
    ('Silver', 'DATA_ULTRA', 'data', 0.005, 200, 500, 0.002),
    ('Silver', 'SMS_STD', 'sms', 0.100, 0, None, None),
    ('Silver', 'VOICE_INT', 'voice', 0.250, 0, None, None),
    ('Silver', 'VOICE_NAT', 'voice', 0.050, 30, None, None),
    
    # Titanium Plan
    ('Titanium', 'DATA_STD', 'data', 0.005, 300, None, None),
    ('Titanium', 'DATA_ULTRA', 'data', 0.002, 512, 2048, 0.001),
    ('Titanium', 'SMS_STD', 'sms', 0.050, 3, None, None),
    ('Titanium', 'VOICE_INT', 'voice', 0.120, 30, None, None),
    ('Titanium', 'VOICE_NAT', 'voice', 0.020, 120, None, None)
]

for rate_plan in rate_plans_data:
    cur.execute(
        """
        INSERT INTO rate_plans (rate_plan_id, product_code, service_type, unit_price, free_units, tier_threshold, tier_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (rate_plan_id, product_code) DO NOTHING
        """,
        rate_plan
    )

conn.commit()
print("✅ Product catalog and rate plans data inserted successfully")

# ----- Customer Data Generation -----
fake = Faker()
regions = [
    "Tanger-Tétouan-Al Hoceïma", "Oriental", "Fès-Meknès", "Rabat-Salé-Kénitra",
    "Béni Mellal-Khénifra", "Casablanca-Settat", "Marrakech-Safi", "Drâa-Tafilalet",
    "Souss-Massa", "Guelmim-Oued Noun", "Laâyoune-Sakia El Hamra", "Dakhla-Oued Ed-Dahab"
]
rate_plans_list = ["Silver", "Gold", "Titanium"]

def generate_moroccan_phone():
    prefix = random.choice(["2126", "2127"])
    suffix = "".join(str(random.randint(0,9)) for _ in range(8))
    return prefix + suffix

# Insert 100 customers
customers_inserted = 0
attempts = 0
max_attempts = 200

while customers_inserted < 100 and attempts < max_attempts:
    customer_id = generate_moroccan_phone()
    customer_name = generate_moroccan_name()
    
    try:
        cur.execute(
            """
            INSERT INTO customer_subscriptions (
                customer_id, customer_name, activation_date,
                subscription_type, status, region, rate_plan_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                customer_id,
                customer_name,
                fake.date_between(start_date='-2y', end_date='today'),
                "postpaid",
                "active",
                random.choice(regions),
                random.choice(rate_plans_list)
            )
        )
        customers_inserted += 1
    except psycopg2.IntegrityError:
        # Skip duplicate customer_id
        conn.rollback()
        pass
    
    attempts += 1

conn.commit()
print(f"✅ Successfully inserted {customers_inserted} Moroccan customers")

# Close connection
cur.close()
conn.close()
print("\n✅ Database setup completed successfully!")