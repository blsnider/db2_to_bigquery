#!/bin/bash

# Configuration
PROJECT_ID="sis-sandbox-463113"
DATASET="db2_migration"

echo "Creating BigQuery tables for DB2 migration..."

# Create dataset if it doesn't exist
echo "Creating dataset ${DATASET} if not exists..."
bq mk --dataset --location=US ${PROJECT_ID}:${DATASET} 2>/dev/null || echo "Dataset already exists"

# Create PO Staging Table
echo "Creating PO staging table..."
bq mk --table \
  ${PROJECT_ID}:${DATASET}.po_inbound_daily \
  store:STRING,vendor:STRING,ss:STRING,type:STRING,po_code:STRING,po:STRING,po_man:STRING,sku:STRING,style:STRING,skudesc1:STRING,expected_date:DATE,cancel_date:DATE,ooqty:INT64,rcvdqty:INT64,breakoutqty:INT64,allocated:INT64,unitinvc_per_sku:INT64,shipped_sku:STRING,shipped_qty:INT64,latest_datercv:DATE,shipnotice_date:DATE,record_key:STRING,load_timestamp:TIMESTAMP \
  2>/dev/null || echo "Table po_inbound_daily already exists"

# Create PO Final Table
echo "Creating PO final table..."
bq mk --table \
  ${PROJECT_ID}:${DATASET}.po_inbound_final \
  store:STRING,vendor:STRING,ss:STRING,type:STRING,po_code:STRING,po:STRING,po_man:STRING,sku:STRING,style:STRING,skudesc1:STRING,expected_date:DATE,cancel_date:DATE,ooqty:INT64,rcvdqty:INT64,breakoutqty:INT64,allocated:INT64,unitinvc_per_sku:INT64,shipped_sku:STRING,shipped_qty:INT64,latest_datercv:DATE,shipnotice_date:DATE,record_key:STRING,load_timestamp:TIMESTAMP \
  2>/dev/null || echo "Table po_inbound_final already exists"

# Create Breakout Staging Table (Pivoted Format)
echo "Creating Breakout staging table (pivoted format)..."
bq mk --table \
  ${PROJECT_ID}:${DATASET}.po_breakout_staging \
  po_man:STRING,ss_num:STRING,vendor:STRING,sku_num:STRING,style:STRING,skudesc:STRING,ship_date:DATE,exp_date:DATE,parent_exp_date:DATE,parent_stores:STRING,DS:INT64,SH:INT64,MO:INT64,CS:INT64,MI:INT64,CH:INT64,TU:INT64,MB:INT64,WI:INT64,EA:INT64,IC:INT64,SF:INT64,SM:INT64,AP:INT64,SC:INT64,OM:INT64,DM:INT64,RH:INT64,FA:INT64,GF:INT64,KI:INT64,RS:INT64,RC:INT64,SP:INT64,SS:INT64,CF:INT64,GK:INT64,BL:INT64,OP:INT64,RO:INT64,JO:INT64,LI:INT64,TC:INT64,EN:INT64,SD:INT64,FFC:INT64,IP:INT64,Sidney:INT64,FFC_WEB:INT64,IP_WEB:INT64,Sidney_WEB:INT64,record_key:STRING,load_timestamp:TIMESTAMP \
  2>/dev/null || echo "Table po_breakout_staging already exists"

# Create Breakout Final Table (Pivoted Format)
echo "Creating Breakout final table (pivoted format)..."
bq mk --table \
  ${PROJECT_ID}:${DATASET}.po_breakout_final \
  po_man:STRING,ss_num:STRING,vendor:STRING,sku_num:STRING,style:STRING,skudesc:STRING,ship_date:DATE,exp_date:DATE,parent_exp_date:DATE,parent_stores:STRING,DS:INT64,SH:INT64,MO:INT64,CS:INT64,MI:INT64,CH:INT64,TU:INT64,MB:INT64,WI:INT64,EA:INT64,IC:INT64,SF:INT64,SM:INT64,AP:INT64,SC:INT64,OM:INT64,DM:INT64,RH:INT64,FA:INT64,GF:INT64,KI:INT64,RS:INT64,RC:INT64,SP:INT64,SS:INT64,CF:INT64,GK:INT64,BL:INT64,OP:INT64,RO:INT64,JO:INT64,LI:INT64,TC:INT64,EN:INT64,SD:INT64,FFC:INT64,IP:INT64,Sidney:INT64,FFC_WEB:INT64,IP_WEB:INT64,Sidney_WEB:INT64,record_key:STRING,load_timestamp:TIMESTAMP \
  2>/dev/null || echo "Table po_breakout_final already exists"

echo ""
echo "Table creation complete!"
echo ""
echo "To verify tables were created:"
echo "bq ls ${PROJECT_ID}:${DATASET}"
echo ""
echo "To see table schema:"
echo "bq show ${PROJECT_ID}:${DATASET}.po_breakout_staging"