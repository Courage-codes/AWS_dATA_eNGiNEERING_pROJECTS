-- Drop tables if they exist
DROP TABLE IF EXISTS presentation_layer.avg_listing_price_weekly;
DROP TABLE IF EXISTS presentation_layer.most_popular_locations_weekly;
DROP TABLE IF EXISTS presentation_layer.top_performing_listings_weekly;
DROP TABLE IF EXISTS presentation_layer.total_bookings_per_user_weekly;
DROP TABLE IF EXISTS presentation_layer.avg_booking_duration_weekly;
DROP TABLE IF EXISTS presentation_layer.repeat_customer_rate_weekly;
DROP TABLE IF EXISTS presentation_layer.booked_nights_by_booking_month;

-- Create tables

CREATE TABLE presentation_layer.avg_listing_price_weekly (
    week DATE ,
    average_listing_price DOUBLE PRECISION
);


CREATE TABLE presentation_layer.most_popular_locations_weekly (
    week DATE ,
    cityname VARCHAR(255),
    bookings_count BIGINT
);

CREATE TABLE presentation_layer.top_performing_listings_weekly (
    week TIMESTAMP,
    apartment_id INTEGER,
    total_revenue DOUBLE PRECISION
);

CREATE TABLE presentation_layer.total_bookings_per_user_weekly (
    week DATE ,
    user_id BIGINT,
    total_bookings BIGINT
);

CREATE TABLE presentation_layer.avg_booking_duration_weekly (
    week DATE,
    avg_booking_duration_days BIGINT
);

CREATE TABLE presentation_layer.repeat_customer_rate_weekly (
    week TIMESTAMP ,
    repeat_customer_rate_percentage BIGINT
);

CREATE TABLE presentation_layer.booked_nights_by_booking_month (
    booking_year INT ,
    booking_month INT ,
    total_booked_nights BIGINT
);
