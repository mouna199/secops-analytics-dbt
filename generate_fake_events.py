import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# === AUTH LOGS ===
def generate_auth_logs(n=100000):
    users = [f"user_{i:04d}" for i in range(1, 1001)]
    events = []
    
    for i in range(n):
        timestamp = datetime(2024, 9, 1) + timedelta(
            days=random.randint(0, 90),
            hours=random.choices(range(24), weights=[1]*6 + [15]*12 + [5]*6)[0],
            minutes=random.randint(0, 59)
        )
        
        user = random.choice(users)
        status = 'success' if random.random() < 0.95 else 'failure'
        
        events.append({
            'event_id': fake.uuid4(),
            'event_timestamp': timestamp,
            'user_id': user,
            'email': f"{user}@company.com",
            'event_type': 'login_success' if status == 'success' else 'login_failure',
            'status': status,
            'source_ip': fake.ipv4_private() if random.random() < 0.8 else fake.ipv4_public(),
            'source_country': fake.country_code(),
            'device_type': random.choice(['laptop', 'mobile', 'tablet']),
            'mfa_enabled': random.random() < 0.7,
            'failure_reason': random.choice(['wrong_password', 'account_locked', None]) if status == 'failure' else None
        })
    
    return pd.DataFrame(events)

# === NETWORK LOGS ===
def generate_network_logs(n=150000):
    events = []
    internal_ips = [f"10.0.{random.randint(1,255)}.{random.randint(1,255)}" for _ in range(500)]
    
    for i in range(n):
        timestamp = datetime(2024, 9, 1) + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        action = 'allow' if random.random() < 0.98 else 'deny'
        threat = random.random() < 0.05
        
        events.append({
            'event_id': fake.uuid4(),
            'event_timestamp': timestamp,
            'source_ip': random.choice(internal_ips),
            'destination_ip': fake.ipv4_public(),
            'destination_port': random.choice([80, 443, 22, 3389, 8080, 53]),
            'protocol': random.choice(['TCP', 'UDP', 'ICMP']),
            'action': action,
            'bytes_sent': random.randint(100, 100000),
            'threat_detected': threat,
            'threat_type': random.choice(['malware', 'botnet', 'port_scan', None]) if threat else None,
            'threat_severity': random.choice(['critical', 'high', 'medium', 'low']) if threat else 'info'
        })
    
    return pd.DataFrame(events)

# === INCIDENTS ===
def generate_incidents(n=5000):
    incidents = []
    priorities = ['P1', 'P2', 'P3', 'P4']
    priority_weights = [5, 15, 50, 30]
    
    for i in range(n):
        created = datetime(2024, 9, 1) + timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
        priority = random.choices(priorities, weights=priority_weights)[0]
        
        # MTTA selon priorité
        mtta_minutes = {'P1': 15, 'P2': 120, 'P3': 480, 'P4': 1440}[priority]
        ack_at = created + timedelta(minutes=random.randint(int(mtta_minutes*0.5), int(mtta_minutes*1.5)))
        
        # MTTR selon priorité
        mttr_hours = {'P1': 4, 'P2': 24, 'P3': 72, 'P4': 168}[priority]
        resolved = ack_at + timedelta(hours=random.randint(int(mttr_hours*0.5), int(mttr_hours*2)))
        
        incidents.append({
            'incident_id': f"INC{i:07d}",
            'created_at': created,
            'acknowledged_at': ack_at,
            'resolved_at': resolved if random.random() < 0.9 else None,
            'status': random.choice(['new', 'investigating', 'resolved', 'closed']),
            'priority': priority,
            'category': random.choice(['authentication', 'network', 'malware', 'access_control']),
            'false_positive': random.random() < 0.3,
            'sla_breached': random.random() < 0.15
        })
    
    return pd.DataFrame(incidents)

# Exécution
df_auth = generate_auth_logs()
df_network = generate_network_logs()
df_incidents = generate_incidents()

# Sauvegarde des données générées
df_auth.to_csv('data/auth_logs.csv', index=False)
df_network.to_csv('data/network_logs.csv', index=False)
df_incidents.to_csv('data/incidents.csv', index=False)
print("Fake event data generated and saved to CSV files.")