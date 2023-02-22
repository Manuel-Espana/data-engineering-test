/* Jesus Manuel España Tzec */

/* Create the necessary tables */
CREATE TABLE `Pruebas.Customer` (
  customer_id INT64 NOT NULL,
  first_name STRING,
  last_name STRING,
  phone_number STRING,
  curp STRING,
  rfc STRING,
  address_info STRING
);

CREATE TABLE `Pruebas.Items` (
  item_id INT64 NOT NULL,
  item_name STRING,
  item_price FLOAT64
);

CREATE TABLE `Pruebas.Items_Bought` (
  customer_id INT64 NOT NULL,
  item_id INT64 NOT NULL,
  order_number STRING,
  date DATE,
  price FLOAT64,
  comments STRING
);

CREATE TABLE `Pruebas.dwh_customer_info`(
  customer_id INT64 NOT NULL,
  first_name STRING,
  last_name STRING,
  phone_number STRING,
  curp STRING,
  rfc STRING,
  address_info STRING,
  item_id INT64 NOT NULL,
  item_name STRING,
  item_price FLOAT64,
  order_number STRING,
  date DATE,
  price FLOAT64,
  comments STRING
);

CREATE TABLE `Pruebas.dwh_customers_data_nosql` (
  _id STRING NOT NULL,
  firstname STRING,
  lastname STRING
);

CREATE TABLE `Pruebas.dwh_items_data_nosql` (
  _id STRING NOT NULL,
  title STRING,
  price FLOAT64
);

/* Insert some data to the tables */
INSERT INTO `Pruebas.Customer` (
  customer_id,
  first_name,
  last_name,
  phone_number,
  curp,
  rfc,
  address_info
)
VALUES 
  (1, 'Peter', 'Parker', '2345424956', 'PAPR700111HYNSZS03', 'PAPR7001119H0', 'Nueva York, Manhattan Calle 89'),
  (2, 'Steve', 'Rogers', '6792349401', 'ROSG600828HYNSZS01', 'ROSG6008283K1', 'Nueva York Manhattan Calle 53'),
  (3, 'Thor', 'Odinson', '9078831374', 'ODTH030331HYNSZS03', 'ODTH0303315X2', 'Asgard Castillo de Odin Cuarto 3');

INSERT INTO `Pruebas.Items` (
  item_id,
  item_name,
  item_price
)
VALUES 
  (1, 'TV', 899.99),
  (2, 'Smartphone', 299.99),
  (3, 'Earphone', 50.00);

INSERT INTO `Pruebas.Items_Bought` (
  customer_id,
  item_id,
  order_number,
  date,
  price,
  comments
)
VALUES 
  (1, 2, '0000000001', '2023-02-12', 299.99, 'Marca Samsung color blanco'),
  (2, 1, '0000000002', '1889-09-11', 899.99, 'Marca LG 60 pulgadas'),
  (3, 3, '0000000003', '2023-01-01', 50.0, 'Marca JBL');


/* Stored Procedure that do some data cleansing */
CREATE PROCEDURE `Pruebas.data_cleansing`(OUT p_msg STRING)
OPTIONS (strict_mode=false)
BEGIN

/* =============================================
 * Description: Insert data from three customer information tables to a data warehouse table.
 *
 * Parameters:
 *  OUT p_msg: STRING
 ============================================= */ 
------------- Declarations -----------------
DECLARE lsErrorMsg STRING DEFAULT NULL;

BEGIN
  /* Insert data to data warehouse */
  INSERT INTO
    `Pruebas.dwh_customer_info` (
      customer_id,
      first_name,
      last_name,
      phone_number,
      curp,
      rfc,
      address_info,
      item_id,
      item_name,
      item_price,
      order_number,
      date,
      price,
      comments

    )
  /* Create temporary tables to join them using their primary keys and then made some data cleansing */
    WITH items_bought AS (
      SELECT
        customer_id,
        item_id,
        order_number,
        date,
        price,
        comments
      FROM 
        `Pruebas.Items_Bought`
    ),
    customer AS (
      SELECT
        customer_id,
        first_name,
        last_name,
        phone_number,
        curp,
        rfc,
        address_info
      FROM 
        `Pruebas.Customer`
    ),
    items AS (
      SELECT
        item_id,
        item_name,
        item_price
      FROM 
        `Pruebas.Items` 
    )
    SELECT 
      c.customer_id,
      /* In case there are null values, change the value */
      COALESCE(first_name, 'Unknown') AS first_name,
      COALESCE(last_name, 'Unknown') AS last_name,
      /* I ensure that the length is not greater or less than the real one */
      CASE 
        WHEN LENGTH(phone_number) > 10 THEN '0000000000'
        WHEN LENGTH(phone_number) < 10 THEN '0000000000'
        ELSE phone_number
      END AS phone_number,
      CASE 
        WHEN LENGTH(curp) > 18 THEN '000000000000000000'
        WHEN LENGTH(curp) < 18 THEN '000000000000000000'
        ELSE curp
      END AS curp,
      CASE
        WHEN LENGTH(rfc) > 13 THEN '0000000000000'
        WHEN LENGTH(rfc) < 13 THEN '0000000000000'
        ELSE rfc
      END AS rfc,
      address_info,
      i.item_id,
      item_name,
      item_price,
      COALESCE(order_number, 'Unknown') AS order_number,
      /* In case date is not valid, change the value */
      IF(date < '2000-01-01', '2000-01-01', date) AS date,
      price,
      comments
    FROM items_bought AS ib
    LEFT JOIN customer AS c
    ON ib.customer_id = c.customer_id
    LEFT JOIN items AS i
    ON ib.item_id = i.item_id;

  /* In case there is a error, return the message error */
  EXCEPTION
    WHEN ERROR THEN
    SET
        lsErrorMsg = @@error.message;
    END;

    IF lsErrorMsg IS NULL THEN
      SET p_msg = 'Ok';
    ELSE
      SET p_msg = lsErrorMsg;
    END IF;
      
END;


/* Create the collections of the NoSQL database */

/*use business_data
db.customers_data.insertMany([
  { firstname: 'Bruce', lastname: 'Wayne'},
  { firstname: 'Clark', lastname: 'Kent'},
  { firstname: 'Tony', lastname: 'Stark'}
]);
db.customers_data.find({ lastname: 'Kent' });

db.items_data.insertMany([
  { title: 'USM', price: 10.2},
  { title: 'Mouse', price: 12.23},
  { title: 'Monitor', price: 199.99}
]);

db.items_data.find({ price: 199.99 });
*/