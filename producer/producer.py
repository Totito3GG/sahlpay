"""
Generateur de transactions SahlPay (version de secours).

A utiliser seulement si le producer.py fourni par l'enseignant n'est pas disponible :
dans ce cas, remplacez ce fichier par le sien. Le format des evenements suit le
cahier des charges (section 2.1).

Ce que le generateur simule :
  - le cycle de vie d'une transaction : INITIATED -> AUTHORIZED -> COMPLETED ou REJECTED,
    parfois REVERSED apres COMPLETED (un evenement JSON par changement d'etat) ;
  - environ 5 % d'evenements en retard (jusqu'a 15 minutes, telephone hors reseau) ;
  - environ 2 % de doublons (meme tx_id et meme status) ;
  - des scenarios de fraude : un appareil partage par de nombreux comptes, des transferts
    circulaires A -> B -> C -> A, des comptes collecteurs qui recoivent beaucoup de petits virements.

Exemples :
  python producer.py --mode merchants --out ../data/merchants.json
  python producer.py --mode file  --n 50000 --out ../data/transactions_history.jsonl
  python producer.py --mode kafka --rate 20 --bootstrap localhost:29092
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone

CITIES = {
    "Casablanca": (33.5731, -7.5898), "Rabat": (34.0209, -6.8416), "Marrakech": (31.6295, -7.9811),
    "Fes": (34.0181, -5.0078), "Tanger": (35.7595, -5.8340), "Agadir": (30.4278, -9.5981),
    "Meknes": (33.8935, -5.5473), "Oujda": (34.6814, -1.9086), "Kenitra": (34.2610, -6.5802),
    "Tetouan": (35.5785, -5.3684),
}
CITY_WEIGHTS = [30, 15, 12, 10, 9, 7, 5, 4, 4, 4]
CATEGORIES = ["epicerie", "carburant", "telecom", "restauration", "pharmacie", "e-commerce",
              "transport", "habillement"]
NAME_PARTS = ["Atlas", "Zitoun", "Sahara", "Andalous", "Medina", "Oasis", "Riad", "Marina", "Kasbah",
              "Nour", "Yasmine", "Argan", "Rif", "Souss", "Bouregreg", "Menara"]
CHANNELS = ["APP", "QR", "USSD"]

N_MERCHANTS = 300
N_WALLETS = 5000


def make_merchants(rng):
    merchants = []
    cities = list(CITIES)
    for i in range(1, N_MERCHANTS + 1):
        city = rng.choices(cities, weights=CITY_WEIGHTS)[0]
        lat, lon = CITIES[city]
        cat = rng.choice(CATEGORIES)
        merchants.append({
            "merchant_id": f"M-{i:03d}",
            "name": f"{rng.choice(NAME_PARTS)} {cat.capitalize()} {i}",
            "city": city,
            "category": cat,
            "latitude": round(lat + rng.uniform(-0.05, 0.05), 6),
            "longitude": round(lon + rng.uniform(-0.05, 0.05), 6),
        })
    return merchants


class Generator:
    def __init__(self, seed, merchants):
        self.rng = random.Random(seed)
        self.merchants = merchants
        self.tx_counter = 0
        # chaque wallet a une ville et 1 a 2 appareils habituels
        self.wallets = {}
        for i in range(1, N_WALLETS + 1):
            w = f"W-{10000 + i}"
            self.wallets[w] = {
                "city": self.rng.choices(list(CITIES), weights=CITY_WEIGHTS)[0],
                "devices": [self._device() for _ in range(self.rng.choice([1, 1, 1, 2]))],
            }
        self.wallet_ids = list(self.wallets)
        # scenarios de fraude
        self.shared_device = "D-fr0d01"
        self.shared_device_wallets = self.rng.sample(self.wallet_ids, 25)
        self.collectors = self.rng.sample(self.wallet_ids, 3)
        self.cycles = [self.rng.sample(self.wallet_ids, 3) for _ in range(4)]

    def _device(self):
        return "D-" + "".join(self.rng.choice("0123456789abcdef") for _ in range(6))

    def _next_tx_id(self):
        self.tx_counter += 1
        return f"TX-{self.tx_counter:08d}"

    def _amount(self, kind):
        if kind == "P2M":
            return round(min(self.rng.lognormvariate(4.6, 1.0), 15000), 2)   # mediane ~100 MAD
        return round(min(self.rng.lognormvariate(5.5, 1.1), 20000), 2)       # mediane ~250 MAD

    def transaction(self, now):
        """Retourne la liste des evenements (changements d'etat) d'une transaction."""
        r = self.rng.random()
        payer = self.rng.choice(self.wallet_ids)
        device = self.rng.choice(self.wallets[payer]["devices"])
        city = self.wallets[payer]["city"]

        if r < 0.02:      # appareil partage par de nombreux comptes
            payer = self.rng.choice(self.shared_device_wallets)
            device = self.shared_device
            kind, payee = "P2P", self.rng.choice(self.wallet_ids)
            amount = round(self.rng.uniform(100, 900), 2)
        elif r < 0.04:    # compte collecteur : beaucoup de petits virements entrants
            kind, payee = "P2P", self.rng.choice(self.collectors)
            amount = round(self.rng.uniform(10, 60), 2)
        elif r < 0.65:
            m = self.rng.choice(self.merchants)
            kind, payee, city = "P2M", m["merchant_id"], m["city"]
            amount = self._amount("P2M")
        else:
            kind = "P2P"
            payee = self.rng.choice(self.wallet_ids)
            while payee == payer:
                payee = self.rng.choice(self.wallet_ids)
            amount = self._amount("P2P")

        channel = "QR" if kind == "P2M" and self.rng.random() < 0.7 else self.rng.choice(CHANNELS)
        return self._lifecycle(kind, payer, payee, device, city, channel, amount, now)

    def cycle_transactions(self, now):
        """Transferts circulaires A -> B -> C -> A."""
        a, b, c = self.rng.choice(self.cycles)
        events = []
        amount = round(self.rng.uniform(1000, 5000), 2)
        for i, (src, dst) in enumerate([(a, b), (b, c), (c, a)]):
            w = self.wallets[src]
            events += self._lifecycle("P2P", src, dst, w["devices"][0], w["city"], "APP",
                                      round(amount * (1 - 0.01 * i), 2), now + timedelta(seconds=40 * i),
                                      force_ok=True)
        return events

    def _lifecycle(self, kind, payer, payee, device, city, channel, amount, now, force_ok=False):
        tx_id = self._next_tx_id()
        base = {"tx_id": tx_id, "type": kind, "payer_wallet": payer, "payee_id": payee,
                "device_id": device, "city": city, "channel": channel, "amount_mad": amount}
        statuses = ["INITIATED", "AUTHORIZED"]
        reject_p = 0.03 if force_ok else (0.12 if channel == "USSD" else 0.06)
        if amount > 5000:
            reject_p += 0.08
        if self.rng.random() < reject_p:
            statuses = ["INITIATED", "REJECTED"]
        else:
            statuses.append("COMPLETED")
            if self.rng.random() < 0.01:
                statuses.append("REVERSED")
        events, t = [], now
        for st in statuses:
            t = t + timedelta(milliseconds=self.rng.randint(200, 3000))
            events.append(dict(base, status=st, event_time=t.strftime("%Y-%m-%dT%H:%M:%SZ")))
        return events


def with_noise(rng, events):
    """Ajoute ~2 % de doublons et marque ~5 % d'evenements en retard (jusqu'a 15 minutes)."""
    out = []
    for e in events:
        delay = 0
        if rng.random() < 0.05:
            delay = rng.randint(60, 900)                       # 1 a 15 minutes de retard
        out.append((e, delay))
        if rng.random() < 0.02:
            out.append((dict(e), delay + rng.randint(1, 30)))  # doublon : meme tx_id et meme status
    return out


def run_file(args, gen):
    rng = random.Random(args.seed + 1)
    start = datetime(2026, 11, 1, tzinfo=timezone.utc)
    span = timedelta(days=14)
    written = 0
    with open(args.out, "w", encoding="utf-8") as f:
        while written < args.n:
            now = start + timedelta(seconds=rng.random() * span.total_seconds())
            events = gen.cycle_transactions(now) if rng.random() < 0.003 else gen.transaction(now)
            for e, _delay in with_noise(rng, events):
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
                written += 1
                if written >= args.n:
                    break
    print(f"{written} evenements ecrits dans {args.out}")


def run_kafka(args, gen):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             key_serializer=lambda k: k.encode("utf-8"))
    rng = random.Random(args.seed + 2)
    pending = []           # evenements retardes : (heure_d_envoi, evenement)
    sent = 0
    interval = 1.0 / args.rate
    print(f"Envoi vers {args.bootstrap}, topic {args.topic}, {args.rate} evt/s (Ctrl+C pour arreter)")
    try:
        while args.n is None or sent < args.n:
            now = datetime.now(timezone.utc)
            events = gen.cycle_transactions(now) if rng.random() < 0.003 else gen.transaction(now)
            for e, delay in with_noise(rng, events):
                if delay:
                    pending.append((time.time() + delay, e))
                    continue
                producer.send(args.topic, key=e["tx_id"], value=e)
                sent += 1
                time.sleep(interval)
            due = [p for p in pending if p[0] <= time.time()]
            pending = [p for p in pending if p[0] > time.time()]
            for _, e in due:
                producer.send(args.topic, key=e["tx_id"], value=e)   # event_time ancien = retard
                sent += 1
            if sent and sent % 200 < len(events):
                print(f"{sent} evenements envoyes ({len(pending)} en attente de retard)")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Termine : {sent} evenements envoyes")


def main():
    p = argparse.ArgumentParser(description="Generateur de transactions SahlPay")
    p.add_argument("--mode", choices=["file", "kafka", "merchants"], required=True)
    p.add_argument("--n", type=int, default=None, help="nombre d'evenements")
    p.add_argument("--rate", type=float, default=20, help="evenements par seconde (mode kafka)")
    p.add_argument("--out", default=None)
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--merchants", default=None, help="chemin de merchants.json (sinon regenere)")
    args = p.parse_args()

    rng = random.Random(args.seed)
    if args.merchants:
        with open(args.merchants, encoding="utf-8") as f:
            merchants = [json.loads(line) for line in f if line.strip()]
    else:
        merchants = make_merchants(rng)

    if args.mode == "merchants":
        out = args.out or "merchants.json"
        with open(out, "w", encoding="utf-8") as f:
            for m in merchants:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        print(f"{len(merchants)} commercants ecrits dans {out}")
        return

    gen = Generator(args.seed, merchants)
    if args.mode == "file":
        args.n = args.n or 50000
        args.out = args.out or "transactions_history.jsonl"
        run_file(args, gen)
    else:
        run_kafka(args, gen)


if __name__ == "__main__":
    sys.exit(main())
