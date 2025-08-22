-- emission_factors (harmonized versioned table)
CREATE TABLE IF NOT EXISTS emission_factors (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    activity_id TEXT,
    activity_name TEXT,
    scope TEXT,
    sector TEXT,
    region TEXT,
    unit TEXT,
    value FLOAT,
    uncertainty FLOAT,
    valid_from DATE,
    valid_to DATE,
    is_approved BOOLEAN DEFAULT TRUE,
    version INT DEFAULT 1,
    modified_by TEXT,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- edit_requests (manual override proposals)
CREATE TABLE IF NOT EXISTS edit_requests (
    id SERIAL PRIMARY KEY,
    factor_id INT REFERENCES emission_factors(id),
    proposed_value FLOAT,
    proposed_by TEXT,
    reason TEXT,
    status TEXT CHECK (status IN ('pending', 'approved', 'rejected')) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP
);

-- approval_logs (audit trail of accepted changes)
CREATE TABLE IF NOT EXISTS approval_logs (
    id SERIAL PRIMARY KEY,
    factor_id INT,
    previous_value FLOAT,
    new_value FLOAT,
    approved_by TEXT,
    approved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason TEXT
);

-- agribalyse_synthesis
CREATE TABLE IF NOT EXISTS agribalyse_synthesis (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    product TEXT,
    sector TEXT,
    element TEXT,
    country TEXT,
    delivery TEXT,
    packaging TEXT,
    ef FLOAT,
    unit TEXT
);

-- agribalyse_steps
CREATE TABLE IF NOT EXISTS agribalyse_steps (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    product TEXT,
    sector TEXT,
    element TEXT,
    country TEXT,
    ef_raw_material FLOAT,
    ef_transformation FLOAT,
    ef_transport FLOAT,
    ef_packaging FLOAT,
    ef_processing FLOAT,
    ef_distribution FLOAT,
    ef_consumption FLOAT,
    unit TEXT
);

-- climatiq_estimates
CREATE TABLE IF NOT EXISTS climatiq_estimates (
    id SERIAL PRIMARY KEY,
    activity_id TEXT NOT NULL,
    activity_name TEXT,
    sector TEXT,
    activity_value FLOAT,
    ef FLOAT,
    country TEXT,
    unit TEXT
);

-- base_carbone
CREATE TABLE IF NOT EXISTS base_carbone (
    id SERIAL PRIMARY KEY,
    code_element TEXT,
    nom TEXT,
    category TEXT,
    under_cat_1 TEXT,
    under_cat_2 TEXT,
    under_cat_3 TEXT,
    under_cat_4 TEXT,
    detail TEXT,
    comment TEXT,
    ef FLOAT,
    unit TEXT
);
