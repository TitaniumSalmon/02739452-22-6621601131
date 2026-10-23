import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

st.set_page_config(page_title='Toyota Price Intelligence',page_icon='🚗',layout='wide')
st.title('🚗 Toyota Price Intelligence')
st.caption('เปรียบเทียบราคารถยนต์ Toyota จาก CheckRaka และผู้จำหน่าย Toyota 2 แห่ง')

def connect():
    return psycopg2.connect(host=os.getenv('TOYOTA_DB_HOST','localhost'),port=os.getenv('TOYOTA_DB_PORT','5433'),dbname=os.getenv('TOYOTA_DB_NAME','toyota_prices'),user=os.getenv('TOYOTA_DB_USER','toyota'),password=os.getenv('TOYOTA_DB_PASSWORD','toyota_dev_password'))

@st.cache_data(ttl=60)
def load_data():
    with connect() as conn:
        try:
            return pd.read_sql_query('''SELECT cp.id AS canonical_id, o.source, o.source_url,
                cp.model, o.source_model AS variant, o.year, cp.price_thb, o.price_type,
                o.scraped_at, cp.quality_status
                FROM canonical_vehicle_prices cp
                JOIN price_source_observations o ON o.canonical_price_id = cp.id
                ORDER BY cp.price_thb''',conn)
        except Exception:
            return pd.DataFrame(columns=['canonical_id','source','source_url','model','variant','year','price_thb','price_type','scraped_at','quality_status'])

@st.cache_data(ttl=60)
def load_runs():
    with connect() as conn:
        try: return pd.read_sql_query('SELECT run_at,source,status,records_count,message FROM pipeline_runs ORDER BY run_at DESC LIMIT 100',conn)
        except Exception: return pd.DataFrame(columns=['run_at','source','status','records_count','message'])

df=load_data()
if df.empty:
    st.warning('ยังไม่มีข้อมูลในฐานข้อมูล กรุณาเปิด Airflow ที่ http://localhost:8080 แล้ว Trigger DAG: toyota_price_pipeline')
else:
    df['price_thb']=pd.to_numeric(df['price_thb'])
    c1,c2,c3,c4=st.columns(4)
    c1.metric('รายการหลังตัดซ้ำ',f'{df.canonical_id.nunique():,}')
    c2.metric('ราคาต่ำสุด',f"฿{df.price_thb.min():,.0f}")
    c3.metric('ราคาสูงสุด',f"฿{df.price_thb.max():,.0f}")
    c4.metric('แหล่งข้อมูล',str(df.source.nunique()))
    st.subheader('ตัวกรองข้อมูล')
    f1,f2,f3=st.columns(3)
    sources=f1.multiselect('แหล่งข้อมูล',sorted(df.source.unique()),default=sorted(df.source.unique()))
    q=f2.text_input('ค้นหารุ่นรถ')
    minp,maxp=int(df.price_thb.min()),int(df.price_thb.max())
    price_range=f3.slider('ช่วงราคา (บาท)',min_value=minp,max_value=maxp,value=(minp,maxp),step=max(1000,(maxp-minp)//100 or 1000)) if maxp>minp else (minp,maxp)
    filtered=df[df.source.isin(sources) & df.price_thb.between(*price_range)]
    if q: filtered=filtered[filtered.model.str.contains(q,case=False,na=False)]
    st.subheader('เปรียบเทียบราคา')
    if not filtered.empty:
        chart=filtered.groupby('source',as_index=False).price_thb.median()
        st.plotly_chart(px.bar(chart,x='source',y='price_thb',title='ราคา Median แยกตามแหล่งข้อมูล',labels={'source':'แหล่งข้อมูล','price_thb':'ราคา (บาท)'}),use_container_width=True)
        st.plotly_chart(px.strip(filtered,x='price_thb',y='source',color='source',hover_data=['model','price_type'],stripmode='overlay',title='การกระจายตัวของราคาแต่ละรายการ',labels={'price_thb':'ราคา (บาท)','source':'แหล่งข้อมูล','model':'รุ่นรถ','price_type':'ประเภทราคา'}),use_container_width=True)
    st.subheader('ตารางข้อมูลรถยนต์')
    st.dataframe(filtered[['source','model','variant','year','price_thb','price_type','scraped_at','source_url']].rename(columns={'source':'แหล่งข้อมูล','model':'รุ่นหลัก','variant':'ชื่อรุ่นบนแหล่งข้อมูล','year':'ปี','price_thb':'ราคา (บาท)','price_type':'ประเภทราคา','scraped_at':'เวลาที่ดึงข้อมูล','source_url':'URL'}),use_container_width=True,hide_index=True)
    st.download_button('ดาวน์โหลด CSV',filtered.to_csv(index=False).encode('utf-8-sig'),'toyota_prices.csv','text/csv')

st.divider()
st.subheader('สถานะ Pipeline (โบนัส)')
runs=load_runs()
if runs.empty: st.info('ยังไม่มีประวัติ Pipeline')
else: st.dataframe(runs,use_container_width=True,hide_index=True)
