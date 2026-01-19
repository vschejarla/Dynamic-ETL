CREATE TABLE dim_store_master (
    store_id                NUMBER(10)
                                CONSTRAINT pk_dim_store_master PRIMARY KEY,
    store_name              VARCHAR2(50)  NOT NULL,
    store_address_lane_1    VARCHAR2(150) NOT NULL,
    store_address_lane_2    VARCHAR2(100),
    store_city              VARCHAR2(25)  NOT NULL,
    store_zip               VARCHAR2(10),
    store_state             VARCHAR2(50)  NOT NULL,
    store_class_of_trade    VARCHAR2(50),
    is_chain                CHAR(1) NOT NULL
                                CONSTRAINT chk_is_chain
                                CHECK (is_chain IN ('Y','N')),
    chain_name              VARCHAR2(50),
    CONSTRAINT chk_chain_name
        CHECK (
            (is_chain = 'Y' AND chain_name IS NOT NULL)
            OR
            (is_chain = 'N' AND chain_name IS NULL)
        )
);
 

CREATE TABLE dim_product (
    product_id     NUMBER(10) PRIMARY KEY,          
    product_name   VARCHAR2(50) NOT NULL,           
    category       VARCHAR2(20)  NOT NULL,           
    sub_category   VARCHAR2(20),                     
    brand          VARCHAR2(50)  NOT NULL,          

    flavour        VARCHAR2(20),                     
    product_size   VARCHAR2(20)  NOT NULL,           
    sqc            VARCHAR2(20)  NOT NULL,           
    uom            VARCHAR2(10)  NOT NULL,         

    unit_price     NUMBER(10,2)  NOT NULL 
);



CREATE TABLE dim_distributor (
    distributor_id     NUMBER(10) PRIMARY KEY,      
    distributor_name   VARCHAR2(50) NOT NULL,       
    distributor_type   VARCHAR2(30) NOT NULL,       
    city               VARCHAR2(30),                
    state              VARCHAR2(30),
    onboarding_date    DATE,
    active_flag CHAR(1) NOT NULL
    CONSTRAINT chk_distributor_active
    CHECK (active_flag IN ('Y','N'))
);

CREATE TABLE dim_date(
    date_id        NUMBER(8) PRIMARY KEY,   
    full_date       DATE        NOT NULL,    

    day             NUMBER(2)   NOT NULL,    
    day_name        VARCHAR2(10) NOT NULL,   

    week_of_year    NUMBER(2)   NOT NULL,    
    month           NUMBER(2)   NOT NULL,    
    month_name      VARCHAR2(10) NOT NULL,   

    quarter         NUMBER(1)   NOT NULL,    
    year            NUMBER(4)   NOT NULL,    

    is_weekend      CHAR(1)     NOT NULL     
        CONSTRAINT chk_is_weekend
        CHECK (is_weekend IN ('Y','N'))
);

CREATE TABLE dim_time (
    time_id        NUMBER(6) PRIMARY KEY,   
    full_time       VARCHAR2(8) NOT NULL,    

    hour            NUMBER(2)   NOT NULL,    
    minute          NUMBER(2)   NOT NULL,    
    second          NUMBER(2)   NOT NULL,    

    am_pm           VARCHAR2(2) NOT NULL,    
    time_slot       VARCHAR2(20),            

    is_peak_hour    CHAR(1) NOT NULL         
        CONSTRAINT chk_is_peak_hour
        CHECK (is_peak_hour IN ('Y','N'))
);

CREATE TABLE fact_sales (
    sales_id        NUMBER(10) PRIMARY KEY,   

    date_id        NUMBER(8) NOT NULL,                

    store_id        NUMBER(10) NOT NULL,       
    product_id      NUMBER(10) NOT NULL,       
    distributor_id  NUMBER(10) NOT NULL,       

    quantity_sold   NUMBER(10) NOT NULL,       
    unit_price      NUMBER(10,2) NOT NULL,     
    gross_amount    NUMBER(12,2) NOT NULL,     
    discount_amount NUMBER(10,2) DEFAULT 0,    
    net_amount      NUMBER(12,2) NOT NULL,     

    -- Foreign Key Constraints
    CONSTRAINT fk_sales_date
        FOREIGN KEY (date_id)
        REFERENCES dim_date(date_id),

    CONSTRAINT fk_sales_store
        FOREIGN KEY (store_id)
        REFERENCES dim_store_master(store_id),

    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_id)
        REFERENCES dim_product(product_id),

    CONSTRAINT fk_sales_distributor
        FOREIGN KEY (distributor_id)
        REFERENCES dim_distributor(distributor_id)
);


SELECT * FROM dim_date;
SELECT * FROM dim_store_master;
SELECT * FROM dim_product;
SELECT * FROM dim_distributor;
select * from fact_sales;

SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_store_master;
select count(*) from dim_date;

SELECT
    -- Fact table
    fs.sales_id,
    fs.quantity_sold,
    fs.unit_price AS sales_unit_price,
    fs.gross_amount,
    fs.discount_amount,
    fs.net_amount,

    -- Store dimension
    ds.store_name,
    ds.store_address_lane_1,
    ds.store_address_lane_2,
    ds.store_city,
    ds.store_zip,
    ds.store_state,
    ds.store_class_of_trade,
    ds.is_chain,
    ds.chain_name,

    -- Product dimension
    dp.product_name,
    dp.category,
    dp.sub_category,
    dp.brand,
    dp.flavour,
    dp.product_size,
    dp.sqc,
    dp.uom,
    dp.unit_price AS product_unit_price,

    -- Distributor dimension
    dd.distributor_name,
    dd.city AS distributor_city,
    dd.state AS distributor_state,
    dd.distributor_type,
    dd.onboarding_date,
    dd.active_flag,

    -- Date dimension
    dt.full_date,
    dt.day,
    dt.day_name,
    dt.week_of_year,
    dt.month,
    dt.month_name,
    dt.quarter,
    dt.year,
    dt.is_weekend

FROM fact_sales fs
INNER JOIN dim_store_master ds
    ON fs.store_id = ds.store_id
INNER JOIN dim_product dp
    ON fs.product_id = dp.product_id
INNER JOIN dim_distributor dd
    ON fs.distributor_id = dd.distributor_id
INNER JOIN dim_date dt
    ON fs.date_id = dt.date_id;

select * from dim_product;



