-- CREATE TABLE SEGMENT
CREATE TABLE IF NOT EXISTS segment(
    id SERIAL PRIMARY KEY,
    segment_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS country(
    id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS product(
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    manufacturing_price INT NOT NULL,
    sale_price INT NOT NULL
);

CREATE TABLE IF NOT EXISTS discount_band(
    id SERIAL PRIMARY KEY,
    discount_band_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales(
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    segment_id INT NOT NULL,
    country_id INT NOT NULL,
    product_id INT NOT NULL,
    units_sold INT NOT NULL,
    discount_band_id INT NOT NULL,
    FOREIGN KEY (segment_id) REFERENCES segment(id),
    FOREIGN KEY (country_id) REFERENCES country(id),
    FOREIGN KEY (product_id) REFERENCES product(id),
    FOREIGN KEY (discount_band_id) REFERENCES discount_band(id)
);

CREATE TABLE IF NOT EXISTS olap_sales_data(
    id SERIAL PRIMARY KEY,
    sales_id INT NOT NULL,
    FOREIGN KEY (sales_id) REFERENCES sales(id),
    gross_sales INT NOT NULL,
    discounts INT NOT NULL,
    sales INT NOT NULL,
    cogs INT NOT NULL,
    profit INT NOT NULL
)