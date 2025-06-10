import random
from faker import Faker
from datetime import datetime
fake = Faker()
import string
import uuid
# Liste des types d'enregistrement
import psycopg2


# Paramètres de connexion
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DBNAME   = "telecomdb"
PG_USER     = "postgres"
PG_PASSWORD = "0000"

# Se connecter à PostgreSQL
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DBNAME,
    user=PG_USER,
    password=PG_PASSWORD
)
cur = conn.cursor()
cur.execute("""Select customer_id from customer_subscriptions """)
customer_ids = [row[0] for row in cur.fetchall()]
conn.commit()
cur.close()
conn.close()

def generate_cdr():
    def maroc_msisdn():
        if random.random() > 0.05:  # 95% chance to return a number
            return random.choice(customer_ids)
        else:
            if random.random() > 0.5 :
                return None  # 5% chance of missing value
            else :
                chars = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
                return ''.join(random.choices(chars, k=12))  
    # Dictionnaire indicative des indicatifs et longueurs typiques
    FOREIGN_CC = {
    "1":    10,  # USA/Canada
    "44":   10,  # UK
    "49":   11,  # Germany
    "61":    9,  # Australia
    "86":   11,  # China
    "91":   10,  # India
    "351":   9,  # Portugal
    "55":   11,  # Brazil
    "32":    9,  # Belgium
    "90":   10,  # Turkey
}

    def generate_foreign_msisdn() -> str:
        """
        Génère un MSISDN étranger (sans le '+'), en choisissant aléatoirement
        un indicatif pays dans FOREIGN_CC et le nombre de chiffres approprié.
        """
        if random.random() > 0.05:
            cc = random.choice(list(FOREIGN_CC.keys()))
            length = FOREIGN_CC[cc]
            # génère la partie abonnée de la bonne longueur
            subscriber = "".join(random.choices("0123456789", k=length))
            return cc + subscriber
        else:
            if random.random() > 0.5 :
                return None  # 5% chance of missing value
            else :
                chars = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
                return ''.join(random.choices(chars, k=12))  
             
    record_types = ['voice', 'sms', 'data']
    weights = [0.6, 0.1, 0.3]  
    
    # List of Moroccan cities (uppercase and formatted as per your original cell_ids)
    cities = [
        "RABAT", "CASABLANCA", "MARRAKECH", "TAZA", "FES", "TANGER", "AGADIR", "OUJDA",
        "MEKNES", "KENITRA", "NADOR", "ALHOCEIMA", "SAFI", "ELJADIDA", "KHOURIBGA",
        "TETOUAN", "LAAYOUNE", "DAKHLA"
    ]

    def generate_cell_id(probability_none=0.3):
        if random.random() < probability_none:
            return None
        city = random.choice(cities)
        number = f"{random.randint(1, 20):02}"  # Format as two digits
        return f"{city}_{number}"
        
    
    data_products=["DATA_PLUS","DATA_ULTRA"]
   
    record_type = random.choices(record_types, weights=weights, k=1)[0]
    cdr = {
        "record_ID" : str(uuid.uuid4()),
        "record_type": record_type,
        "timestamp": datetime.now().isoformat() if random.random() > 0.1 else fake.date_time_between(start_date=datetime(2010, 1, 1), end_date="now").isoformat() if random.random() > 0.5 else None,  # 2% de chance d'être null
        "cell_id":  generate_cell_id(),  # 30% de chance d'être null pqr defaut
        "technology": random.choices(["2G", "3G", "4G", "5G","LTE","NR","UMTS"],weights=[0.2,0.3,0.6,0.3,0.1,0.08,0.02],k=1)[0] if random.random() > 0.1 else None,  # 10% de chance d'être null
    }

    if record_type == "voice":
        # Pour les appels vocaux, ajouter des champs spécifiques avec possibilité de nulls
        cdr.update({
            "caller_id": maroc_msisdn(),
            "callee_id": random.choices([maroc_msisdn(),generate_foreign_msisdn()],weights=[0.8,0.2],k=1)[0],
            "duration_sec": random.randint(10, 600) if random.random() > 0.2 else random.randint(-600, -10)  if random.random() > 0.5 else None   # 20% de chance d'être null
        })
    elif record_type == "sms":
        # Pour les SMS, ajouter des champs spécifiques avec possibilité de nulls
        cdr.update({
            "sender_id": maroc_msisdn(),  
            "receiver_id": random.choices([maroc_msisdn(),generate_foreign_msisdn()],weights=[0.8,0.2],k=1)[0],  
        })
    elif record_type == "data":
        # Pour les données, ajouter des champs spécifiques avec possibilité de nulls
        cdr.update({
            "user_id": maroc_msisdn(),
            "data_volume_mb": round(random.uniform(0.1, 2000), 2) if random.random() > 0.1 else None,  # 10% de chance d'être null
            "session_duration_sec": random.randint(60, 600) if random.random() > 0.2 else random.randint(-600, -10)  if random.random() > 0.5 else None , # 20% de chance d'être null
            "product_code":random.choices(data_products,weights=[0.8,0.2],k=1)[0],
        })

    return cdr