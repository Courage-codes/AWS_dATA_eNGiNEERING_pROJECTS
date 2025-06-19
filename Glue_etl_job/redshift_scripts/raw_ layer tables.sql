CREATE SCHEMA raw_layer;

CREATE TABLE raw_layer.apartments (
    id integer ENCODE az64,
    title character varying(500) ENCODE lzo,
    source character varying(100) ENCODE lzo,
    price double precision ENCODE raw,
    currency character varying(10) ENCODE lzo,
    listing_created_on character varying(65535) ENCODE lzo,
    is_active character varying(65535) ENCODE lzo,
    last_modified_timestamp character varying(65535) ENCODE lzo
) DISTSTYLE AUTO
SORTKEY
    (id);

-- apartment_attributes table
CREATE TABLE raw_layer.apartment_attributes (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    body VARCHAR(1000),
    amenities VARCHAR(500),
    bathrooms INT,
    bedrooms INT,
    fee FLOAT,
    has_photo VARCHAR(10),
    pets_allowed VARCHAR(50),
    price_display VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(500),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);

-- bookings table
CREATE TABLE raw_layer.bookings (
    booking_id INT PRIMARY KEY,
    user_id INT,
    apartment_id INT,
    booking_date VARCHAR(20),
    checkin_date VARCHAR(20),
    checkout_date VARCHAR(20),
    total_price FLOAT,
    currency VARCHAR(10),
    booking_status VARCHAR(50)
);

-- user_viewing table
CREATE TABLE raw_layer.user_viewing (
    user_id INT,
    apartment_id INT,
    viewed_at VARCHAR(20),
    is_wishlisted VARCHAR(10),
    call_to_action VARCHAR(255),
    PRIMARY KEY (user_id, apartment_id)
);
