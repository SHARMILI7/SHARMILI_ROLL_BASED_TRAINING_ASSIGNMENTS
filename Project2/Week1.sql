CREATE DATABASE IF NOT EXISTS inventory_management;
USE inventory_management;

CREATE TABLE products (product_id INT PRIMARY KEY AUTO_INCREMENT, product_name VARCHAR(100), category VARCHAR(50), reorder_level INT, supplier_id INT);

CREATE TABLE suppliers (supplier_id INT PRIMARY KEY AUTO_INCREMENT, supplier_name VARCHAR(100), contact_email VARCHAR(100));

CREATE TABLE warehouses (warehouse_id INT PRIMARY KEY AUTO_INCREMENT, warehouse_name VARCHAR(100), location VARCHAR(100));

CREATE TABLE stock_movements (movement_id INT PRIMARY KEY AUTO_INCREMENT, product_id INT, warehouse_id INT, movement_type VARCHAR(20), quantity INT, movement_date DATE, FOREIGN KEY (product_id) REFERENCES products(product_id), FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id));

INSERT INTO suppliers (supplier_name,contact_email) VALUES ('ABC Supplies','abc@example.com'),('Global Traders','global@example.com'),('Prime Wholesale','prime@example.com');

INSERT INTO products (product_name,category,reorder_level,supplier_id) VALUES ('Laptop','Electronics',10,1),('Mouse','Electronics',20,2),('Keyboard','Electronics',15,2),('Chair','Furniture',5,3),('Desk','Furniture',3,3);

INSERT INTO warehouses (warehouse_name,location) VALUES ('Central Warehouse','Chennai'),('North Warehouse','Bangalore'),('South Warehouse','Hyderabad');

INSERT INTO stock_movements (product_id,warehouse_id,movement_type,quantity,movement_date) VALUES
(1,1,'IN',50,'2026-05-01'),
(2,1,'IN',100,'2026-05-02'),
(3,2,'IN',80,'2026-05-03'),
(4,3,'IN',20,'2026-05-04'),
(5,2,'IN',10,'2026-05-05'),
(1,1,'OUT',45,'2026-05-06'),
(2,1,'OUT',85,'2026-05-07'),
(3,2,'OUT',60,'2026-05-08'),
(4,3,'OUT',18,'2026-05-09'),
(5,2,'OUT',4,'2026-05-10');

SELECT * FROM products;
SELECT * FROM suppliers;
SELECT * FROM warehouses;
SELECT * FROM stock_movements;

UPDATE stock_movements SET quantity=40 WHERE movement_id=6;

DELETE FROM stock_movements WHERE movement_id=10;

CREATE INDEX idx_product_id ON stock_movements(product_id);
CREATE INDEX idx_warehouse_id ON stock_movements(warehouse_id);

DELIMITER //

CREATE PROCEDURE LowStockProducts()
BEGIN
SELECT p.product_id,p.product_name,p.reorder_level,
SUM(CASE WHEN sm.movement_type='IN' THEN sm.quantity ELSE -sm.quantity END) AS current_stock
FROM products p
JOIN stock_movements sm ON p.product_id=sm.product_id
GROUP BY p.product_id,p.product_name,p.reorder_level
HAVING current_stock<p.reorder_level;
END //

DELIMITER ;

CALL LowStockProducts();

SELECT sm.product_id,p.product_name,sm.warehouse_id,w.warehouse_name,sm.movement_type,sm.quantity,sm.movement_date
FROM stock_movements sm
JOIN products p ON sm.product_id=p.product_id
JOIN warehouses w ON sm.warehouse_id=w.warehouse_id;