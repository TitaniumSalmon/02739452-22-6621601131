# Toyota Price Intelligence

โปรเจกต์ ETL รวบรวมและเปรียบเทียบราคารถ Toyota จาก 3 เว็บไซต์ด้วย Apache Airflow, จัดเก็บ PostgreSQL และแสดงผลด้วย Streamlit Dashboard

## แหล่งข้อมูล
1. CheckRaka: https://www.checkraka.com/car/toyota/
2. Toyota Nakornpathom (Sedan): https://www.toyotanakornpathom.com/sedan/
3. Toyota JD Cars: https://www.toyotajdcars.com/models-prices/

## โครงสร้าง
- `dags/toyota_prices_dag.py`: DAG, robots.txt check, extraction, transform, quality checks, UPSERT
- `web/app.py`: dashboard ที่อ่านข้อมูลจาก PostgreSQL โดยตรง
- `docker-compose.yml`: PostgreSQL, Airflow, Streamlit
- `.env.example`: ตัวอย่างค่าตัวแปรแวดล้อม ห้ามใส่ secret จริงลง Git

## วิธีรัน (Docker Desktop ต้องเปิดอยู่)
1. คัดลอก `.env.example` เป็น `.env` แล้วแก้ `POSTGRES_PASSWORD` และ `AIRFLOW_SECRET_KEY`
2. รัน `docker compose up -d --build`
3. Airflow: http://localhost:8080 (local demo login: `admin` / `admin`; เปลี่ยนก่อนใช้จริง)
4. เปิด DAG `toyota_price_pipeline` แล้วกด Trigger DAG
5. Dashboard: http://localhost:8501
6. PostgreSQL เปิดที่ `localhost:5433` (database `toyota_prices`)

## หมายเหตุสำคัญ
- Parser เป็น HTML-based และไม่ใช้ Selenium ตามข้อกำหนด DAG อ่าน `robots.txt` ก่อนเข้าหน้าราคา หาก robots.txt ใช้งานไม่ได้หรือไม่อนุญาต DAG จะข้ามแหล่งนั้นและบันทึกสถานะล้มเหลว
- DAG ออกแบบให้แหล่งที่ล้มเหลวไม่หยุดการรวมข้อมูลจากแหล่งอื่น, ตรวจคุณภาพ, และ UPSERT ด้วย dedupe key
- ขอบเขตคือราคารถยนต์ Toyota แยกตามรุ่นและแหล่งข้อมูล; แหล่ง Nakornpathom `/sedan/` ครอบคลุมรถเก๋งเท่านั้น
- เก็บ `price_type` เพื่อแยกราคาเริ่มต้นออกจากราคาที่แสดงเป็นรายการ และไม่ใช้ยอดผ่อนต่อเดือนเป็นราคารถ
- ตรวจเงื่อนไขการใช้งานและความถี่ที่เหมาะสมด้วย ดึงข้อมูลอย่างสุภาพและไม่เก็บข้อมูลส่วนบุคคล
- จำนวนรายการขึ้นกับข้อมูลที่หน้าเว็บเปิดเผยจริง ให้ตรวจจำนวนใน Dashboard หลังรัน DAG; อย่าสร้างราคาปลอมเพื่อให้ครบ 100 รายการ

## Troubleshooting
- ถ้า `docker compose` รันไม่ได้ ให้ตรวจว่า Docker Desktop ใช้งานได้
- ถ้า dashboard ต่อ DB ไม่ได้ ให้ตรวจ container `postgres` และ log ด้วย `docker compose logs postgres dashboard`
- ถ้า source task ขึ้น failed ให้เปิด log ของ task เพื่อดู HTTP status หรือข้อความ parser ไม่พบรายการ
- หลังแก้โค้ด DAG ให้กด refresh หน้า Airflow และ rerun DAG
