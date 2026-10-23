from __future__ import annotations

import hashlib
import logging
import os
import re
import urllib.robotparser
from datetime import datetime, timedelta
from urllib.parse import urlparse

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values


SOURCES = {
    "checkraka": {
        "url": "https://www.checkraka.com/car/toyota/",
        "price_type": "listed_price",
    },
    "toyota_nakornpathom": {
        "url": "https://www.toyotanakornpathom.com/sedan/",
        "price_type": "starting_price",
    },
    "toyota_jd_cars": {
        "url": "https://www.toyotajdcars.com/models-prices/",
        "price_type": "dealer_listed_price",
    },
}
HEADERS = {
    "User-Agent": "ToyotaPriceResearch/1.0 (educational project; contact: student via course)"
}
PRICE_RE = re.compile(
    r"(?:฿\s*)?(\d[\d,]*(?:\.\d+)?)\s*(ล้าน|ลบ\.?|บาท|฿)?",
    re.IGNORECASE,
)
PRICE_CONTEXT_RE = re.compile(
    r"(?:฿\s*\d[\d,]*(?:\.\d+)?|\d[\d,]*(?:\.\d+)?\s*(?:ล้าน|ลบ\.?|บาท|฿))",
    re.I,
)
MODEL_WORDS = re.compile(
    r"(yaris|corolla|camry|hilux|revo|travo|fortuner|land cruiser|bZ4X|innova|velo[z]?|hiace|commuter|majesty|vellfire|alphard|supra|86|toyota|โตโยต้า)",
    re.I,
)


def db():
    return psycopg2.connect(
        host=os.getenv("TOYOTA_DB_HOST", "localhost"),
        port=os.getenv("TOYOTA_DB_PORT", "5433"),
        dbname=os.getenv("TOYOTA_DB_NAME", "toyota_prices"),
        user=os.getenv("TOYOTA_DB_USER", "toyota"),
        password=os.getenv("TOYOTA_DB_PASSWORD", "toyota_dev_password"),
    )


def init_db():
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS vehicle_prices (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                source_url TEXT NOT NULL,
                model TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                year INTEGER,
                price_thb NUMERIC(14,2) NOT NULL,
                price_type TEXT NOT NULL DEFAULT 'listed_price',
                scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                dedupe_key TEXT NOT NULL UNIQUE,
                quality_status TEXT NOT NULL DEFAULT 'passed',
                raw_text TEXT
            )"""
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vehicle_prices_model ON vehicle_prices(model)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vehicle_prices_source ON vehicle_prices(source)")
        cur.execute("ALTER TABLE vehicle_prices ADD COLUMN IF NOT EXISTS price_type TEXT NOT NULL DEFAULT 'listed_price'")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS pipeline_runs (
                id BIGSERIAL PRIMARY KEY,
                run_at TIMESTAMPTZ DEFAULT NOW(),
                source TEXT,
                status TEXT,
                records_count INTEGER DEFAULT 0,
                message TEXT
            )"""
        )


def price_from_text(text: str):
    match = PRICE_CONTEXT_RE.search(text.replace("\xa0", " "))
    if not match:
        return None
    price_text = match.group(0).replace("฿", "").strip()
    number = PRICE_RE.search(price_text)
    if not number:
        return None
    amount = float(number.group(1).replace(",", ""))
    unit = (number.group(2) or "").lower()
    if unit.startswith("ลบ") or unit == "ล้าน":
        amount *= 1_000_000
    return int(amount)


def clean_title(text: str):
    text = " ".join(text.replace("\xa0", " ").split())
    text = re.sub(r"^(?:ดูเพิ่มเติม|ดูรุ่นย่อยทั้งหมด|ดูทั้งหมด)\s*", "", text, flags=re.I)
    text = re.sub(r"\s+(?:เริ่ม|ราคาเริ่มต้น)\s*$", "", text, flags=re.I)
    return text.strip(" -|:·")


def parse_records(source: str, url: str, html: str):
    """Extract one observed price per model card; never synthesize missing prices."""
    soup = BeautifulSoup(html, "lxml")
    records, seen = [], set()

    # Model names are normally in headings or linked product titles. Walk only a few
    # ancestors to pair each title with its nearest price card, avoiding page-wide divs.
    titles = soup.select("h1, h2, h3, h4, h5, a")
    for title_node in titles:
        title = clean_title(title_node.get_text(" ", strip=True))
        if len(title) < 3 or len(title) > 120 or not MODEL_WORDS.search(title):
            continue

        card = title_node
        card_text = ""
        for _ in range(5):
            card = card.parent
            if not card or not getattr(card, "get_text", None):
                break
            card_text = " ".join(card.get_text(" ", strip=True).split())
            # A vehicle price occurs before any finance copy on these listing pages.
            if PRICE_CONTEXT_RE.search(card_text) and len(card_text) <= 700:
                break
        if not card_text or len(card_text) > 700 or not PRICE_CONTEXT_RE.search(card_text):
            continue

        price = price_from_text(card_text)
        if price is None or not 100_000 <= price <= 30_000_000:
            continue
        # Keep source titles as observed, but avoid navigation labels and broad headings.
        if re.search(r"^(?:toyota|โตโยต้า|รถยนต์โตโยต้า)$", title, re.I):
            continue
        year_match = re.search(r"\b(20\d{2})\b", title)
        year = int(year_match.group(1)) if year_match else None
        normalized = re.sub(r"\W+", "", title.lower())
        key_parts = (source, normalized, str(year or ""), str(price))
        if key_parts in seen:
            continue
        seen.add(key_parts)
        records.append(
            {
                "source": source,
                "source_url": url,
                "model": title[:180],
                "variant": "",
                "year": year,
                "price_thb": price,
                "price_type": SOURCES[source]["price_type"],
                "raw_text": card_text[:500],
            }
        )
    return records


def log_run(source, status, count, message):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs(source,status,records_count,message) VALUES (%s,%s,%s,%s)",
            (source, status, count, message[:500]),
        )


def extract_source(source: str, **context):
    url = SOURCES[source]["url"]
    try:
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        robots_response = requests.get(robots_url, headers=HEADERS, timeout=(8, 15))
        robots_response.raise_for_status()
        robots = urllib.robotparser.RobotFileParser()
        robots.set_url(robots_url)
        robots.parse(robots_response.text.splitlines())
        if not robots.can_fetch(HEADERS["User-Agent"], url):
            raise PermissionError(f"robots.txt ไม่อนุญาตให้ดึง URL นี้: {url}")
        response = requests.get(url, headers=HEADERS, timeout=(8, 25))
        response.raise_for_status()
        records = parse_records(source, url, response.text)
        if not records:
            raise ValueError("ไม่พบรายการราคาใน HTML ที่ได้รับ ตรวจโครงสร้างหน้าเว็บก่อนปรับ parser")
        context["ti"].xcom_push(key=f"{source}_records", value=records)
        log_run(source, "success", len(records), f"ดึงได้ {len(records)} รายการ")
        return "success"
    except Exception as exc:
        logging.exception("Source %s failed", source)
        context["ti"].xcom_push(key=f"{source}_records", value=[])
        log_run(source, "failed", 0, str(exc))
        return "failed"


def make_extract(source):
    return lambda **context: extract_source(source, **context)


def load_and_quality(**context):
    init_db()
    ti = context["ti"]
    all_rows = []
    for source in SOURCES:
        all_rows.extend(
            ti.xcom_pull(task_ids=f"extract_sources.extract_{source}", key=f"{source}_records") or []
        )

    valid, rejected = [], 0
    for row in all_rows:
        price = row.get("price_thb")
        if not row.get("model") or not isinstance(price, (int, float)) or not 100_000 <= price <= 30_000_000:
            rejected += 1
            continue
        # Preserve a row from every source so the dashboard can compare what each site
        # reports. The key removes rerun duplicates within the same source.
        normalized = re.sub(r"\W+", "", row["model"].lower())
        identity = "|".join(
            [row["source"], normalized, row.get("variant", "").lower(), str(row.get("year") or ""), str(price)]
        )
        row["dedupe_key"] = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        row["quality_status"] = "passed"
        valid.append(row)

    if rejected:
        log_run("quality", "warning", rejected, "มีรายการไม่ผ่านเงื่อนไขคุณภาพและถูกตัดออก")
    if not valid:
        log_run("load", "warning", 0, "รอบนี้ไม่มีข้อมูลที่ผ่าน quality check; เก็บข้อมูลเดิมไว้")
        return 0

    with db() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO vehicle_prices
                (source,source_url,model,variant,year,price_thb,price_type,raw_text,dedupe_key,quality_status)
                VALUES %s ON CONFLICT(dedupe_key) DO UPDATE SET
                  source_url=EXCLUDED.source_url, model=EXCLUDED.model, variant=EXCLUDED.variant,
                  year=EXCLUDED.year, price_thb=EXCLUDED.price_thb, price_type=EXCLUDED.price_type,
                  raw_text=EXCLUDED.raw_text, scraped_at=NOW(), quality_status=EXCLUDED.quality_status""",
            [
                (
                    row["source"], row["source_url"], row["model"], row["variant"], row["year"],
                    row["price_thb"], row["price_type"], row["raw_text"], row["dedupe_key"], row["quality_status"],
                )
                for row in valid
            ],
        )
    log_run("load", "success", len(valid), f"valid={len(valid)}, rejected={rejected}")
    return len(valid)


def choose_quality_path(**context):
    count = context["ti"].xcom_pull(task_ids="load_merge_quality") or 0
    return "quality_passed" if count else "quality_failed"


def log_quality_pass(**context):
    logging.info("At least one valid source record was loaded or updated.")


def log_quality_fail(**context):
    logging.warning("No valid data this run. Existing PostgreSQL data is preserved.")


with DAG(
    dag_id="toyota_price_pipeline",
    description="Collect Toyota model prices from CheckRaka and two Toyota dealers",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "student", "retries": 0},
    tags=["toyota", "prices", "etl"],
) as dag:
    setup = PythonOperator(task_id="init_database", python_callable=init_db)
    with TaskGroup("extract_sources", tooltip="Extract public price pages independently") as extract_group:
        for source_name in SOURCES:
            PythonOperator(
                task_id=f"extract_{source_name}",
                python_callable=make_extract(source_name),
                execution_timeout=timedelta(minutes=2),
            )
    load = PythonOperator(task_id="load_merge_quality", python_callable=load_and_quality)
    branch = BranchPythonOperator(task_id="quality_branch", python_callable=choose_quality_path)
    passed = PythonOperator(task_id="quality_passed", python_callable=log_quality_pass)
    failed = PythonOperator(task_id="quality_failed", python_callable=log_quality_fail)
    setup >> extract_group >> load >> branch >> [passed, failed]
