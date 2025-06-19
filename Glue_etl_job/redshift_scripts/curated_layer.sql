CREATE SCHEMA curated_layer;

CREATE TABLE curated_layer.apartment_attributes (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    body VARCHAR(1000),
    amenities VARCHAR(500),
    bathrooms INT,
    bedrooms INT,
    fee FLOAT,
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display_usd VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(500),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
)
DISTSTYLE AUTO;

CREATE TABLE curated_layer.user_viewing (
    user_id INT,
    apartment_id INT,
    viewed_at DATE,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(255),
    PRIMARY KEY (user_id, apartment_id),
    CONSTRAINT fk_user_viewing_apartments FOREIGN KEY (apartment_id) REFERENCES raw_layer.apartments(id)
)
DISTSTYLE KEY
DISTKEY (apartment_id)
SORTKEY (user_id, apartment_id);





CREATE TABLE curated_layer.apartments (
    id INT PRIMARY KEY,
    title VARCHAR(65535),
    source VARCHAR(65535),
    price DOUBLE PRECISION,
    currency VARCHAR(10),
    listing_created_on DATE,
    is_active BOOLEAN,
    last_modified_timestamp DATE,
    price_usd DOUBLE PRECISION
)
DISTSTYLE AUTO
SORTKEY (id);


CREATE TABLE curated_layer.bookings (
    booking_id INT PRIMARY KEY,
    user_id INT,
    apartment_id INT,
    booking_date DATE,
    checkin_date DATE,
    checkout_date DATE,
    total_price DOUBLE PRECISION,
    currency VARCHAR(10),
    booking_status VARCHAR(50),
    total_price_usd DOUBLE PRECISION
)
DISTSTYLE KEY
DISTKEY (apartment_id)
SORTKEY (booking_date);
