use inventory_management

db.createCollection("audit_logs")

db.audit_logs.insertMany([
{product_id:1,warehouse_id:1,adjustment_type:"Stock Correction",reason:"Damaged units removed",quantity_adjusted:-5,audit_date:new Date("2026-05-06")},
{product_id:2,warehouse_id:1,adjustment_type:"Manual Adjustment",reason:"Inventory recount mismatch",quantity_adjusted:-3,audit_date:new Date("2026-05-07")},
{product_id:3,warehouse_id:2,adjustment_type:"Supplier Return",reason:"Defective batch returned",quantity_adjusted:-10,audit_date:new Date("2026-05-08")},
{product_id:4,warehouse_id:3,adjustment_type:"Restock",reason:"Emergency replenishment",quantity_adjusted:15,audit_date:new Date("2026-05-09")},
{product_id:5,warehouse_id:2,adjustment_type:"Warehouse Transfer",reason:"Moved to central warehouse",quantity_adjusted:-2,audit_date:new Date("2026-05-10")}
])

db.audit_logs.createIndex({product_id:1})

db.audit_logs.createIndex({warehouse_id:1})

db.audit_logs.find()

db.audit_logs.find({product_id:1})

db.audit_logs.find({warehouse_id:2})