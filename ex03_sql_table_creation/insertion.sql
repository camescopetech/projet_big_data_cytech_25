-- =============================================================================
-- Exercice 3 : Insertion des données de référence
-- NYC Yellow Taxi Data Warehouse
-- =============================================================================
-- Ce script insère les données de référence (lookup data) fournies par NYC TLC
-- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- =============================================================================

-- =============================================================================
-- DIMENSION: dim_vendor
-- Source: NYC TLC Data Dictionary
-- =============================================================================
INSERT INTO dim_vendor (vendor_id, vendor_name, description) VALUES
(1, 'Creative Mobile Technologies', 'CMT - Creative Mobile Technologies, LLC'),
(2, 'VeriFone Inc.', 'VTS - VeriFone Inc.');

-- =============================================================================
-- DIMENSION: dim_rate_code
-- Source: NYC TLC Data Dictionary
-- =============================================================================
INSERT INTO dim_rate_code (rate_code_id, rate_code_name, description) VALUES
(1, 'Standard rate', 'Tarif standard calculé au compteur'),
(2, 'JFK', 'Forfait JFK Airport - $52 flat rate'),
(3, 'Newark', 'Newark Airport - tarif négocié'),
(4, 'Nassau or Westchester', 'Courses vers Nassau ou Westchester County'),
(5, 'Negotiated fare', 'Tarif négocié entre chauffeur et passager'),
(6, 'Group ride', 'Course partagée / Group ride');

-- =============================================================================
-- DIMENSION: dim_payment_type
-- Source: NYC TLC Data Dictionary
-- =============================================================================
INSERT INTO dim_payment_type (payment_type_id, payment_name, description) VALUES
(1, 'Credit card', 'Paiement par carte de crédit'),
(2, 'Cash', 'Paiement en espèces'),
(3, 'No charge', 'Pas de frais - course gratuite'),
(4, 'Dispute', 'Litige - montant contesté'),
(5, 'Unknown', 'Type de paiement inconnu'),
(6, 'Voided trip', 'Course annulée');

-- =============================================================================
-- DIMENSION: dim_borough
-- Arrondissements de New York City
-- =============================================================================
INSERT INTO dim_borough (borough_id, borough_name, description) VALUES
(1, 'Manhattan', 'Borough central de NYC - centre financier et touristique'),
(2, 'Bronx', 'Borough nord de NYC'),
(3, 'Brooklyn', 'Borough est de NYC - le plus peuplé'),
(4, 'Queens', 'Borough est de NYC - inclut JFK et LaGuardia'),
(5, 'Staten Island', 'Borough sud de NYC - île séparée'),
(6, 'EWR', 'Newark Airport - New Jersey'),
(7, 'Unknown', 'Zone non identifiée');

-- =============================================================================
-- DIMENSION: dim_location
-- Zones de taxi NYC (échantillon des zones principales)
-- Source: TLC Taxi Zone Lookup Table
-- =============================================================================
INSERT INTO dim_location (location_id, zone_name, borough_id, service_zone) VALUES
-- Manhattan (borough_id = 1)
(1, 'Newark Airport', 6, 'EWR'),
(4, 'Alphabet City', 1, 'Yellow Zone'),
(12, 'Battery Park', 1, 'Yellow Zone'),
(13, 'Battery Park City', 1, 'Yellow Zone'),
(24, 'Bloomingdale', 1, 'Yellow Zone'),
(41, 'Central Harlem', 1, 'Boro Zone'),
(42, 'Central Harlem North', 1, 'Boro Zone'),
(43, 'Central Park', 1, 'Yellow Zone'),
(45, 'Chinatown', 1, 'Yellow Zone'),
(48, 'Clinton East', 1, 'Yellow Zone'),
(50, 'Clinton West', 1, 'Yellow Zone'),
(68, 'East Chelsea', 1, 'Yellow Zone'),
(79, 'East Village', 1, 'Yellow Zone'),
(87, 'Financial District North', 1, 'Yellow Zone'),
(88, 'Financial District South', 1, 'Yellow Zone'),
(90, 'Flatiron', 1, 'Yellow Zone'),
(100, 'Garment District', 1, 'Yellow Zone'),
(107, 'Gramercy', 1, 'Yellow Zone'),
(113, 'Greenwich Village North', 1, 'Yellow Zone'),
(114, 'Greenwich Village South', 1, 'Yellow Zone'),
(125, 'Hudson Sq', 1, 'Yellow Zone'),
(127, 'Inwood', 1, 'Boro Zone'),
(128, 'Inwood Hill Park', 1, 'Boro Zone'),
(137, 'Kips Bay', 1, 'Yellow Zone'),
(140, 'Lenox Hill East', 1, 'Yellow Zone'),
(141, 'Lenox Hill West', 1, 'Yellow Zone'),
(142, 'Lincoln Square East', 1, 'Yellow Zone'),
(143, 'Lincoln Square West', 1, 'Yellow Zone'),
(144, 'Little Italy/NoLiTa', 1, 'Yellow Zone'),
(148, 'Lower East Side', 1, 'Yellow Zone'),
(151, 'Manhattan Valley', 1, 'Yellow Zone'),
(152, 'Manhattanville', 1, 'Boro Zone'),
(153, 'Marble Hill', 1, 'Boro Zone'),
(158, 'Meatpacking/West Village West', 1, 'Yellow Zone'),
(161, 'Midtown Center', 1, 'Yellow Zone'),
(162, 'Midtown East', 1, 'Yellow Zone'),
(163, 'Midtown North', 1, 'Yellow Zone'),
(164, 'Midtown South', 1, 'Yellow Zone'),
(166, 'Morningside Heights', 1, 'Boro Zone'),
(170, 'Murray Hill', 1, 'Yellow Zone'),
(186, 'Penn Station/Madison Sq West', 1, 'Yellow Zone'),
(209, 'Randalls Island', 1, 'Yellow Zone'),
(211, 'Roosevelt Island', 1, 'Boro Zone'),
(224, 'SoHo', 1, 'Yellow Zone'),
(229, 'Stuyvesant Town/PCV', 1, 'Yellow Zone'),
(230, 'Sutton Place/Turtle Bay North', 1, 'Yellow Zone'),
(231, 'Sutton Place/Turtle Bay South', 1, 'Yellow Zone'),
(232, 'Times Sq/Theatre District', 1, 'Yellow Zone'),
(233, 'TriBeCa/Civic Center', 1, 'Yellow Zone'),
(234, 'Two Bridges/Seward Park', 1, 'Yellow Zone'),
(236, 'UN/Turtle Bay South', 1, 'Yellow Zone'),
(237, 'Union Sq', 1, 'Yellow Zone'),
(238, 'Upper East Side North', 1, 'Yellow Zone'),
(239, 'Upper East Side South', 1, 'Yellow Zone'),
(243, 'Upper West Side North', 1, 'Yellow Zone'),
(244, 'Upper West Side South', 1, 'Yellow Zone'),
(246, 'Washington Heights North', 1, 'Boro Zone'),
(249, 'Washington Heights South', 1, 'Boro Zone'),
(261, 'World Trade Center', 1, 'Yellow Zone'),
(262, 'Yorkville East', 1, 'Yellow Zone'),
(263, 'Yorkville West', 1, 'Yellow Zone'),
-- Aéroports (Queens - borough_id = 4)
(132, 'JFK Airport', 4, 'Airports'),
(138, 'LaGuardia Airport', 4, 'Airports'),
-- Brooklyn (borough_id = 3)
(14, 'Bay Ridge', 3, 'Boro Zone'),
(22, 'Bensonhurst West', 3, 'Boro Zone'),
(25, 'Boerum Hill', 3, 'Boro Zone'),
(26, 'Borough Park', 3, 'Boro Zone'),
(29, 'Brighton Beach', 3, 'Boro Zone'),
(35, 'Brooklyn Heights', 3, 'Boro Zone'),
(36, 'Brooklyn Navy Yard', 3, 'Boro Zone'),
(37, 'Brownsville', 3, 'Boro Zone'),
(39, 'Bushwick North', 3, 'Boro Zone'),
(40, 'Bushwick South', 3, 'Boro Zone'),
(52, 'Clinton Hill', 3, 'Boro Zone'),
(54, 'Cobble Hill', 3, 'Boro Zone'),
(61, 'Crown Heights North', 3, 'Boro Zone'),
(63, 'Cypress Hills', 3, 'Boro Zone'),
(65, 'Downtown Brooklyn/MetroTech', 3, 'Boro Zone'),
(66, 'DUMBO/Vinegar Hill', 3, 'Boro Zone'),
(71, 'East Flatbush/Farragut', 3, 'Boro Zone'),
(76, 'East New York', 3, 'Boro Zone'),
(80, 'East Williamsburg', 3, 'Boro Zone'),
(85, 'Erasmus', 3, 'Boro Zone'),
(89, 'Flatbush/Ditmas Park', 3, 'Boro Zone'),
(91, 'Flatlands', 3, 'Boro Zone'),
(97, 'Fort Greene', 3, 'Boro Zone'),
(106, 'Gowanus', 3, 'Boro Zone'),
(108, 'Gravesend', 3, 'Boro Zone'),
(111, 'Greenpoint', 3, 'Boro Zone'),
(123, 'Homecrest', 3, 'Boro Zone'),
(149, 'Madison', 3, 'Boro Zone'),
(177, 'Ocean Hill', 3, 'Boro Zone'),
(178, 'Ocean Parkway South', 3, 'Boro Zone'),
(181, 'Park Slope', 3, 'Boro Zone'),
(188, 'Prospect Heights', 3, 'Boro Zone'),
(189, 'Prospect-Lefferts Gardens', 3, 'Boro Zone'),
(190, 'Prospect Park', 3, 'Boro Zone'),
(195, 'Red Hook', 3, 'Boro Zone'),
(210, 'Sheepshead Bay', 3, 'Boro Zone'),
(217, 'South Williamsburg', 3, 'Boro Zone'),
(225, 'South Slope', 3, 'Boro Zone'),
(227, 'Sunset Park', 3, 'Boro Zone'),
(228, 'Sunset Park West', 3, 'Boro Zone'),
(255, 'Williamsburg (North Side)', 3, 'Boro Zone'),
(256, 'Williamsburg (South Side)', 3, 'Boro Zone'),
(257, 'Windsor Terrace', 3, 'Boro Zone'),
-- Bronx (borough_id = 2)
(3, 'Allerton/Pelham Gardens', 2, 'Boro Zone'),
(18, 'Bedford Park', 2, 'Boro Zone'),
(20, 'Belmont', 2, 'Boro Zone'),
(31, 'Bronxdale', 2, 'Boro Zone'),
(32, 'Bronx Park', 2, 'Boro Zone'),
(46, 'City Island', 2, 'Boro Zone'),
(47, 'Claremont/Bathgate', 2, 'Boro Zone'),
(51, 'Co-Op City', 2, 'Boro Zone'),
(58, 'Concourse', 2, 'Boro Zone'),
(59, 'Concourse Village', 2, 'Boro Zone'),
(60, 'Crotona Park', 2, 'Boro Zone'),
(69, 'East Concourse/Concourse Village', 2, 'Boro Zone'),
(78, 'East Tremont', 2, 'Boro Zone'),
(81, 'Eastchester', 2, 'Boro Zone'),
(94, 'Fordham South', 2, 'Boro Zone'),
(119, 'Highbridge', 2, 'Boro Zone'),
(126, 'Hunts Point', 2, 'Boro Zone'),
(136, 'Kingsbridge Heights', 2, 'Boro Zone'),
(147, 'Longwood', 2, 'Boro Zone'),
(159, 'Melrose South', 2, 'Boro Zone'),
(167, 'Morrisania/Melrose', 2, 'Boro Zone'),
(168, 'Mott Haven/Port Morris', 2, 'Boro Zone'),
(169, 'Mount Hope', 2, 'Boro Zone'),
(174, 'Norwood', 2, 'Boro Zone'),
(182, 'Parkchester', 2, 'Boro Zone'),
(183, 'Pelham Bay', 2, 'Boro Zone'),
(184, 'Pelham Bay Park', 2, 'Boro Zone'),
(199, 'Riverdale/North Riverdale/Fieldston', 2, 'Boro Zone'),
(200, 'Rikers Island', 2, 'Boro Zone'),
(208, 'Schuylerville/Edgewater Park', 2, 'Boro Zone'),
(212, 'Soundview/Bruckner', 2, 'Boro Zone'),
(213, 'Soundview/Castle Hill', 2, 'Boro Zone'),
(220, 'Spuyten Duyvil/Kingsbridge', 2, 'Boro Zone'),
(235, 'University Heights/Morris Heights', 2, 'Boro Zone'),
(240, 'Van Cortlandt Park', 2, 'Boro Zone'),
(241, 'Van Cortlandt Village', 2, 'Boro Zone'),
(242, 'Van Nest/Morris Park', 2, 'Boro Zone'),
(247, 'Wakefield', 2, 'Boro Zone'),
(248, 'Westchester Village/Unionport', 2, 'Boro Zone'),
(250, 'West Concourse', 2, 'Boro Zone'),
(254, 'West Farms/Bronx River', 2, 'Boro Zone'),
(259, 'Woodlawn/Wakefield', 2, 'Boro Zone'),
-- Queens (borough_id = 4)
(2, 'Astoria', 4, 'Boro Zone'),
(7, 'Astoria Park', 4, 'Boro Zone'),
(8, 'Auburndale', 4, 'Boro Zone'),
(9, 'Baisley Park', 4, 'Boro Zone'),
(10, 'Bayside', 4, 'Boro Zone'),
(15, 'Bay Terrace/Fort Totten', 4, 'Boro Zone'),
(16, 'Beechhurst', 4, 'Boro Zone'),
(17, 'Bellerose', 4, 'Boro Zone'),
(28, 'Briarwood/Jamaica Hills', 4, 'Boro Zone'),
(33, 'Broad Channel', 4, 'Boro Zone'),
(38, 'Cambria Heights', 4, 'Boro Zone'),
(49, 'College Point', 4, 'Boro Zone'),
(53, 'Corona', 4, 'Boro Zone'),
(64, 'Douglaston', 4, 'Boro Zone'),
(70, 'East Elmhurst', 4, 'Boro Zone'),
(73, 'East Flushing', 4, 'Boro Zone'),
(82, 'Elmhurst', 4, 'Boro Zone'),
(83, 'Elmhurst/Maspeth', 4, 'Boro Zone'),
(86, 'Far Rockaway', 4, 'Boro Zone'),
(92, 'Flushing', 4, 'Boro Zone'),
(93, 'Flushing Meadows-Corona Park', 4, 'Boro Zone'),
(95, 'Forest Hills', 4, 'Boro Zone'),
(96, 'Forest Park/Highland Park', 4, 'Boro Zone'),
(98, 'Fresh Meadows', 4, 'Boro Zone'),
(101, 'Glen Oaks', 4, 'Boro Zone'),
(102, 'Glendale', 4, 'Boro Zone'),
(117, 'Hammels/Arverne', 4, 'Boro Zone'),
(121, 'Hillcrest/Pomonok', 4, 'Boro Zone'),
(122, 'Hollis', 4, 'Boro Zone'),
(124, 'Howard Beach', 4, 'Boro Zone'),
(129, 'Jackson Heights', 4, 'Boro Zone'),
(130, 'Jamaica', 4, 'Boro Zone'),
(131, 'Jamaica Estates', 4, 'Boro Zone'),
(133, 'Kew Gardens', 4, 'Boro Zone'),
(134, 'Kew Gardens Hills', 4, 'Boro Zone'),
(139, 'Laurelton', 4, 'Boro Zone'),
(145, 'Little Neck', 4, 'Boro Zone'),
(146, 'Long Island City/Hunters Point', 4, 'Boro Zone'),
(157, 'Maspeth', 4, 'Boro Zone'),
(160, 'Middle Village', 4, 'Boro Zone'),
(171, 'Murray Hill-Queens', 4, 'Boro Zone'),
(173, 'North Corona', 4, 'Boro Zone'),
(179, 'Old Astoria', 4, 'Boro Zone'),
(180, 'Ozone Park', 4, 'Boro Zone'),
(191, 'Queens Village', 4, 'Boro Zone'),
(193, 'Queensboro Hill', 4, 'Boro Zone'),
(196, 'Rego Park', 4, 'Boro Zone'),
(197, 'Richmond Hill', 4, 'Boro Zone'),
(198, 'Ridgewood', 4, 'Boro Zone'),
(201, 'Rockaway Park', 4, 'Boro Zone'),
(202, 'Rosedale', 4, 'Boro Zone'),
(205, 'Saint Albans', 4, 'Boro Zone'),
(207, 'South Jamaica', 4, 'Boro Zone'),
(215, 'South Ozone Park', 4, 'Boro Zone'),
(216, 'South Richmond Hill', 4, 'Boro Zone'),
(218, 'Springfield Gardens North', 4, 'Boro Zone'),
(219, 'Springfield Gardens South', 4, 'Boro Zone'),
(223, 'Steinway', 4, 'Boro Zone'),
(226, 'Sunnyside', 4, 'Boro Zone'),
(252, 'Whitestone', 4, 'Boro Zone'),
(253, 'Willets Point', 4, 'Boro Zone'),
(258, 'Woodhaven', 4, 'Boro Zone'),
(260, 'Woodside', 4, 'Boro Zone'),
-- Staten Island (borough_id = 5)
(5, 'Arden Heights', 5, 'Boro Zone'),
(6, 'Arrochar/Fort Wadsworth', 5, 'Boro Zone'),
(23, 'Bloomfield/Emerson Hill', 5, 'Boro Zone'),
(44, 'Charleston/Tottenville', 5, 'Boro Zone'),
(84, 'Eltingville/Annadale/Prince''s Bay', 5, 'Boro Zone'),
(99, 'Freshkills Park', 5, 'Boro Zone'),
(109, 'Great Kills', 5, 'Boro Zone'),
(110, 'Great Kills Park', 5, 'Boro Zone'),
(115, 'Grymes Hill/Clifton', 5, 'Boro Zone'),
(116, 'Heartland Village/Todt Hill', 5, 'Boro Zone'),
(118, 'Howland Hook', 5, 'Boro Zone'),
(156, 'Mariners Harbor', 5, 'Boro Zone'),
(172, 'New Dorp/Midland Beach', 5, 'Boro Zone'),
(176, 'Oakwood', 5, 'Boro Zone'),
(187, 'Port Richmond', 5, 'Boro Zone'),
(204, 'Rossville/Woodrow', 5, 'Boro Zone'),
(206, 'Saint George/New Brighton', 5, 'Boro Zone'),
(214, 'South Beach/Dongan Hills', 5, 'Boro Zone'),
(221, 'Stapleton', 5, 'Boro Zone'),
(245, 'Westerleigh', 5, 'Boro Zone'),
(251, 'West New Brighton', 5, 'Boro Zone'),
-- Unknown (borough_id = 7)
(264, 'Unknown', 7, 'N/A'),
(265, 'Unknown', 7, 'N/A');

-- =============================================================================
-- DIMENSION: dim_time
-- Granularité: 30 minutes (48 créneaux par jour)
-- =============================================================================
INSERT INTO dim_time (time_id, hour, minute, hour_label, period_of_day, is_rush_hour, is_business_hour)
SELECT
    (h * 100 + m) AS time_id,
    h AS hour,
    m AS minute,
    LPAD(h::text, 2, '0') || ':' || LPAD(m::text, 2, '0') AS hour_label,
    CASE
        WHEN h BETWEEN 6 AND 11 THEN 'Morning'
        WHEN h BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN h BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS period_of_day,
    CASE
        WHEN (h BETWEEN 7 AND 9) OR (h BETWEEN 17 AND 19) THEN TRUE
        ELSE FALSE
    END AS is_rush_hour,
    CASE
        WHEN h BETWEEN 9 AND 17 THEN TRUE
        ELSE FALSE
    END AS is_business_hour
FROM generate_series(0, 23) AS h
CROSS JOIN (SELECT 0 AS m UNION SELECT 30) AS mins
ORDER BY time_id;

-- =============================================================================
-- DIMENSION: dim_date
-- Génération automatique pour l'année 2024 (données de janvier 2024)
-- =============================================================================
INSERT INTO dim_date (date_id, full_date, year, quarter, month, month_name, week_of_year, day_of_month, day_of_week, day_name, is_weekend, is_holiday)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_id,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE
        WHEN d = '2024-01-01' THEN TRUE  -- New Year's Day
        WHEN d = '2024-01-15' THEN TRUE  -- MLK Day
        ELSE FALSE
    END AS is_holiday
FROM generate_series('2024-01-01'::date, '2024-12-31'::date, '1 day'::interval) AS d;

-- =============================================================================
-- VÉRIFICATION DES INSERTIONS
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE '=== Vérification des données de référence ===';
    RAISE NOTICE 'dim_vendor: % lignes', (SELECT COUNT(*) FROM dim_vendor);
    RAISE NOTICE 'dim_rate_code: % lignes', (SELECT COUNT(*) FROM dim_rate_code);
    RAISE NOTICE 'dim_payment_type: % lignes', (SELECT COUNT(*) FROM dim_payment_type);
    RAISE NOTICE 'dim_borough: % lignes', (SELECT COUNT(*) FROM dim_borough);
    RAISE NOTICE 'dim_location: % lignes', (SELECT COUNT(*) FROM dim_location);
    RAISE NOTICE 'dim_time: % lignes', (SELECT COUNT(*) FROM dim_time);
    RAISE NOTICE 'dim_date: % lignes', (SELECT COUNT(*) FROM dim_date);
    RAISE NOTICE '=== Insertion terminée avec succès ===';
END $$;

-- =============================================================================
-- FIN DU SCRIPT D'INSERTION
-- =============================================================================
