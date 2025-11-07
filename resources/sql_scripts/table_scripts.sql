CREATE TABLE product_staging_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    file_location VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(1)
);

CREATE TABLE s3_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bucket_name VARCHAR(100) NOT NULL,
    folder_path VARCHAR(200) NOT NULL,
    file_type VARCHAR(20) DEFAULT 'csv',
    status CHAR(1) DEFAULT 'A',          -- A = Active, I = Inactive
    environment VARCHAR(20) NOT NULL,    -- dev / qa / prod
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO s3_metadata (bucket_name, folder_path, file_type, status, environment)
VALUES
('relmart-sales-project', 'sales_data/', 'csv', 'A', 'dev');

CREATE TABLE customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(255),
    pincode VARCHAR(10),
    phone_number VARCHAR(20),
    customer_joining_date DATE
);

-- Insert command for customer
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Saanvi', 'Krishna', 'Delhi', '122009', '9173121081', '2021-01-20');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Dhanush', 'Sahni', 'Delhi', '122009', '9155328165', '2022-03-27');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Yasmin', 'Shan', 'Delhi', '122009', '9191478300', '2023-04-08');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Vidur', 'Mammen', 'Delhi', '122009', '9119017511', '2020-10-12');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Shamik', 'Doctor', 'Delhi', '122009', '9105180499', '2022-10-30');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Ryan', 'Dugar', 'Delhi', '122009', '9142616565', '2020-08-10');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Romil', 'Shanker', 'Delhi', '122009', '9129451313', '2021-10-29');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Krish', 'Tandon', 'Delhi', '122009', '9145683399', '2020-01-08');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Divij', 'Garde', 'Delhi', '122009', '9141984713', '2020-11-10');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Hunar', 'Tank', 'Delhi', '122009', '9169808085', '2023-01-27');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Zara', 'Dhaliwal', 'Delhi', '122009', '9129776379', '2023-06-13');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Sumer', 'Mangal', 'Delhi', '122009', '9138607933', '2020-05-01');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Rhea', 'Chander', 'Delhi', '122009', '9103434731', '2023-08-09');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Yuvaan', 'Bawa', 'Delhi', '122009', '9162077019', '2023-02-18');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Sahil', 'Sabharwal', 'Delhi', '122009', '9174928780', '2021-03-16');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Tiya', 'Kashyap', 'Delhi', '122009', '9105126094', '2023-03-23');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Kimaya', 'Lala', 'Delhi', '122009', '9115616831', '2021-03-14');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Vardaniya', 'Jani', 'Delhi', '122009', '9125068977', '2022-07-19');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Indranil', 'Dutta', 'Delhi', '122009', '9120667755', '2023-07-18');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Kavya', 'Sachar', 'Delhi', '122009', '9157628717', '2022-05-04');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Manjari', 'Sule', 'Delhi', '122009', '9112525501', '2023-02-12');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Akarsh', 'Kalla', 'Delhi', '122009', '9113226332', '2021-03-05');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Miraya', 'Soman', 'Delhi', '122009', '9111455455', '2023-07-06');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Shalv', 'Chaudhary', 'Delhi', '122009', '9158099495', '2021-03-14');
INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('Jhanvi', 'Bava', 'Delhi', '122009', '9110074097', '2022-07-14');


--store table
CREATE TABLE store (
    id INT PRIMARY KEY,
    address VARCHAR(255),
    store_pincode VARCHAR(10),
    store_manager_name VARCHAR(100),
    store_opening_date DATE,
    reviews TEXT
);

--data of store table
INSERT INTO store (id, address, store_pincode, store_manager_name, store_opening_date, reviews)
VALUES
    (121,'Delhi', '122009', 'Manish', '2022-01-15', 'Great store with a friendly staff.'),
    (122,'Delhi', '110011', 'Nikita', '2021-08-10', 'Excellent selection of products.'),
    (123,'Delhi', '201301', 'vikash', '2023-01-20', 'Clean and organized store.'),
    (124,'Delhi', '400001', 'Rakesh', '2020-05-05', 'Good prices and helpful staff.');


-- product table
CREATE TABLE product (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    current_price DECIMAL(10, 2),
    old_price DECIMAL(10, 2),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    expiry_date DATE
);


--product table data
INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date)
VALUES
    ('Apple iPhone 14 Pro', 134999, 134999, '2023-03-01', NULL, '2025-12-31'),
    ('Sony WH-1000XM5 Headphones', 29990, 29990, '2023-03-01', NULL, '2025-12-31'),
    ('Samsung 55-inch 4K QLED TV', 84990, 84990, '2023-03-01', NULL, '2025-12-31'),
    ('Dell XPS 13 Laptop', 124999, 124999, '2023-03-01', NULL, '2025-12-31'),
    ('Apple Watch Series 9', 45900, 45900, '2023-03-01', NULL, '2025-12-31'),
    ('Prestige Induction Cooktop', 4990, 4990, '2023-03-01', NULL, '2025-12-31'),
    ('Dyson V12 Vacuum Cleaner', 65900, 65900, '2023-03-01', NULL, '2025-12-31'),
    ('Ray-Ban Aviator Sunglasses', 9990, 9990, '2023-03-01', NULL, '2025-12-31'),
    ('Tissot PRX Watch', 49500, 49500, '2023-03-01', NULL, '2025-12-31'),
    ('Nike Air Max Sneakers', 12999, 12999, '2023-03-01', NULL, '2025-12-31'),
    ('Clinique Moisture Surge Cream', 2700, 2700, '2023-03-01', NULL, '2025-12-31'),
    ('Dyson Supersonic Hair Dryer', 32900, 32900, '2023-03-01', NULL, '2025-12-31'),
    ('Amul 85% Dark Chocolate', 450, 450, '2023-03-01', NULL, '2025-12-31'),
    ('India Gate Basmati Rice', 700, 700, '2023-03-01', NULL, '2025-12-31'),
    ('Davidoff Coffee Beans', 1100, 1100, '2023-03-01', NULL, '2025-12-31'),
    ('Powermax Adjustable Dumbbell Set', 14999, 14999, '2023-03-01', NULL, '2025-12-31'),
    ('Cosco Resistance Band', 350, 350, '2023-03-01', NULL, '2025-12-31');



--sales team table
CREATE TABLE sales_team (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    manager_id INT,
    is_manager CHAR(1),
    address VARCHAR(255),
    pincode VARCHAR(10),
    joining_date DATE
);


--sales team data
INSERT INTO sales_team (first_name, last_name, manager_id, is_manager, address, pincode, joining_date)
VALUES
    ('Rahul', 'Verma', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Priya', 'Singh', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Amit', 'Sharma', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Sneha', 'Gupta', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Neha', 'Kumar', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Vijay', 'Yadav', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Anita', 'Malhotra', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Alok', 'Rajput', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Monica', 'Jain', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Rajesh', 'Gupta', 10, 'Y', 'Delhi', '122007', '2020-05-01');





--s3 bucket table
CREATE TABLE s3_bucket_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bucket_name VARCHAR(255),
    file_location VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(20)
);


--s3 bucket data
INSERT INTO s3_bucket_info (bucket_name, status, created_date, updated_date)
VALUES ('youtube-project-testing', 'active', NOW(), NOW());


--Data Mart customer
CREATE TABLE customers_data_mart (
    customer_id INT ,
    full_name VARCHAR(100),
    address VARCHAR(200),
    phone_number VARCHAR(20),
    sales_date_month DATE,
    total_sales DECIMAL(10, 2)
);


--sales mart table
CREATE TABLE sales_team_data_mart (
    store_id INT,
    sales_person_id INT,
    full_name VARCHAR(255),
    sales_month VARCHAR(10),
    total_sales DECIMAL(10, 2),
    incentive DECIMAL(10, 2)
);