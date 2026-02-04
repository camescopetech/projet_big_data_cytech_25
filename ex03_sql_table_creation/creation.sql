-- =============================================================================
-- Exercice 3 : Création du modèle dimensionnel (Snowflake Schema)
-- NYC Yellow Taxi Data Warehouse
-- =============================================================================
-- Modèle choisi : FLOCON (Snowflake)
-- Justification :
--   - Normalisation des dimensions pour réduire la redondance
--   - Meilleure intégrité des données de référence
--   - Adapté aux données NYC TLC avec plusieurs niveaux de hiérarchie
-- =============================================================================

-- Suppression des tables existantes (ordre inverse des dépendances)
DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_borough CASCADE;
DROP TABLE IF EXISTS dim_vendor CASCADE;
DROP TABLE IF EXISTS dim_rate_code CASCADE;
DROP TABLE IF EXISTS dim_payment_type CASCADE;

-- =============================================================================
-- DIMENSIONS DE NIVEAU 2 (Flocon - tables normalisées)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table: dim_borough (sous-dimension de location)
-- Description: Arrondissements de New York
-- -----------------------------------------------------------------------------
CREATE TABLE dim_borough (
    borough_id      SERIAL PRIMARY KEY,
    borough_name    VARCHAR(50) NOT NULL UNIQUE,
    description     VARCHAR(255)
);

COMMENT ON TABLE dim_borough IS 'Arrondissements (boroughs) de New York City';

-- =============================================================================
-- DIMENSIONS DE NIVEAU 1
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table: dim_vendor
-- Description: Fournisseurs de systèmes TPEP (Technology Provider)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_vendor (
    vendor_id       INTEGER PRIMARY KEY,
    vendor_name     VARCHAR(50) NOT NULL,
    description     VARCHAR(255)
);

COMMENT ON TABLE dim_vendor IS 'Fournisseurs de systèmes de paiement électronique';

-- -----------------------------------------------------------------------------
-- Table: dim_rate_code
-- Description: Codes tarifaires NYC TLC
-- -----------------------------------------------------------------------------
CREATE TABLE dim_rate_code (
    rate_code_id    INTEGER PRIMARY KEY,
    rate_code_name  VARCHAR(50) NOT NULL,
    description     VARCHAR(255)
);

COMMENT ON TABLE dim_rate_code IS 'Codes tarifaires officiels NYC TLC';

-- -----------------------------------------------------------------------------
-- Table: dim_payment_type
-- Description: Types de paiement acceptés
-- -----------------------------------------------------------------------------
CREATE TABLE dim_payment_type (
    payment_type_id INTEGER PRIMARY KEY,
    payment_name    VARCHAR(50) NOT NULL,
    description     VARCHAR(255)
);

COMMENT ON TABLE dim_payment_type IS 'Modes de paiement des courses';

-- -----------------------------------------------------------------------------
-- Table: dim_location
-- Description: Zones de taxi NYC (TLC Taxi Zones)
-- Relation flocon avec dim_borough
-- -----------------------------------------------------------------------------
CREATE TABLE dim_location (
    location_id     INTEGER PRIMARY KEY,
    zone_name       VARCHAR(100) NOT NULL,
    borough_id      INTEGER REFERENCES dim_borough(borough_id),
    service_zone    VARCHAR(50)
);

COMMENT ON TABLE dim_location IS 'Zones de taxi définies par NYC TLC';

CREATE INDEX idx_location_borough ON dim_location(borough_id);

-- -----------------------------------------------------------------------------
-- Table: dim_date
-- Description: Dimension temporelle (date)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_id         INTEGER PRIMARY KEY,  -- Format YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month           INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name      VARCHAR(20) NOT NULL,
    week_of_year    INTEGER NOT NULL,
    day_of_month    INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE
);

COMMENT ON TABLE dim_date IS 'Dimension calendrier pour analyse temporelle';

CREATE INDEX idx_date_year_month ON dim_date(year, month);

-- -----------------------------------------------------------------------------
-- Table: dim_time
-- Description: Dimension temporelle (heure) - Flocon
-- -----------------------------------------------------------------------------
CREATE TABLE dim_time (
    time_id         INTEGER PRIMARY KEY,  -- Format HHMM (0000-2359)
    hour            INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    minute          INTEGER NOT NULL CHECK (minute BETWEEN 0 AND 59),
    hour_label      VARCHAR(20) NOT NULL,  -- "00:00", "00:30", etc.
    period_of_day   VARCHAR(20) NOT NULL,  -- Morning, Afternoon, Evening, Night
    is_rush_hour    BOOLEAN NOT NULL,
    is_business_hour BOOLEAN NOT NULL
);

COMMENT ON TABLE dim_time IS 'Dimension heure pour analyse intra-journalière';

-- =============================================================================
-- TABLE DE FAITS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table: fact_trips
-- Description: Table de faits centrale - courses de taxi
-- Grain: Une ligne = Une course de taxi
-- -----------------------------------------------------------------------------
CREATE TABLE fact_trips (
    trip_id                 BIGSERIAL PRIMARY KEY,

    -- Clés étrangères vers les dimensions
    vendor_id               INTEGER NOT NULL REFERENCES dim_vendor(vendor_id),
    rate_code_id            INTEGER NOT NULL REFERENCES dim_rate_code(rate_code_id),
    payment_type_id         INTEGER NOT NULL REFERENCES dim_payment_type(payment_type_id),
    pickup_location_id      INTEGER REFERENCES dim_location(location_id),
    dropoff_location_id     INTEGER REFERENCES dim_location(location_id),
    pickup_date_id          INTEGER NOT NULL REFERENCES dim_date(date_id),
    pickup_time_id          INTEGER NOT NULL REFERENCES dim_time(time_id),
    dropoff_date_id         INTEGER NOT NULL REFERENCES dim_date(date_id),
    dropoff_time_id         INTEGER NOT NULL REFERENCES dim_time(time_id),

    -- Timestamps originaux (pour référence)
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP NOT NULL,

    -- Mesures de la course
    passenger_count         INTEGER NOT NULL CHECK (passenger_count BETWEEN 1 AND 9),
    trip_distance           DECIMAL(10,2) NOT NULL CHECK (trip_distance >= 0),
    trip_duration_minutes   DECIMAL(10,2) NOT NULL CHECK (trip_duration_minutes > 0),

    -- Mesures financières
    fare_amount             DECIMAL(10,2) NOT NULL CHECK (fare_amount >= 0),
    extra                   DECIMAL(10,2) DEFAULT 0,
    mta_tax                 DECIMAL(10,2) DEFAULT 0,
    tip_amount              DECIMAL(10,2) DEFAULT 0 CHECK (tip_amount >= 0),
    tolls_amount            DECIMAL(10,2) DEFAULT 0 CHECK (tolls_amount >= 0),
    improvement_surcharge   DECIMAL(10,2) DEFAULT 0,
    congestion_surcharge    DECIMAL(10,2) DEFAULT 0,
    airport_fee             DECIMAL(10,2) DEFAULT 0,
    total_amount            DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),

    -- Flag additionnel
    store_and_fwd_flag      CHAR(1) CHECK (store_and_fwd_flag IN ('Y', 'N'))
);

COMMENT ON TABLE fact_trips IS 'Table de faits - Courses de taxi NYC (grain: 1 course)';

-- Index pour optimiser les requêtes analytiques courantes
CREATE INDEX idx_fact_trips_pickup_date ON fact_trips(pickup_date_id);
CREATE INDEX idx_fact_trips_vendor ON fact_trips(vendor_id);
CREATE INDEX idx_fact_trips_payment ON fact_trips(payment_type_id);
CREATE INDEX idx_fact_trips_pickup_location ON fact_trips(pickup_location_id);
CREATE INDEX idx_fact_trips_dropoff_location ON fact_trips(dropoff_location_id);
CREATE INDEX idx_fact_trips_datetime ON fact_trips(pickup_datetime);

-- =============================================================================
-- VUES ANALYTIQUES
-- =============================================================================

-- Vue pour faciliter les requêtes avec les dimensions jointes
CREATE OR REPLACE VIEW v_trips_analysis AS
SELECT
    f.trip_id,
    f.pickup_datetime,
    f.dropoff_datetime,
    v.vendor_name,
    r.rate_code_name,
    p.payment_name,
    pl.zone_name AS pickup_zone,
    pb.borough_name AS pickup_borough,
    dl.zone_name AS dropoff_zone,
    db.borough_name AS dropoff_borough,
    d.full_date AS pickup_date,
    d.day_name AS pickup_day,
    d.month_name AS pickup_month,
    t.period_of_day AS pickup_period,
    t.is_rush_hour AS pickup_rush_hour,
    f.passenger_count,
    f.trip_distance,
    f.trip_duration_minutes,
    f.fare_amount,
    f.tip_amount,
    f.total_amount
FROM fact_trips f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
JOIN dim_rate_code r ON f.rate_code_id = r.rate_code_id
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
LEFT JOIN dim_location pl ON f.pickup_location_id = pl.location_id
LEFT JOIN dim_borough pb ON pl.borough_id = pb.borough_id
LEFT JOIN dim_location dl ON f.dropoff_location_id = dl.location_id
LEFT JOIN dim_borough db ON dl.borough_id = db.borough_id
JOIN dim_date d ON f.pickup_date_id = d.date_id
JOIN dim_time t ON f.pickup_time_id = t.time_id;

COMMENT ON VIEW v_trips_analysis IS 'Vue dénormalisée pour analyse des courses';

-- =============================================================================
-- FIN DU SCRIPT DE CRÉATION
-- =============================================================================
