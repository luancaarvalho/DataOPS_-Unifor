CREATE TABLE IF NOT EXISTS raw_reviews (
    review_id INT,
    product_id INT,
    user_id INT,
    review_text TEXT,
    score INT,
    review_date DATE
);

CREATE TABLE IF NOT EXISTS raw_annotations (
    review_id INT,
    sentimento TEXT,
    tema_principal TEXT,
    fonte_anotacao TEXT
);

CREATE TABLE IF NOT EXISTS fact_review_annotations (
    review_id INT,
    product_id INT,
    review_text TEXT,
    sentimento TEXT,
    tema_principal TEXT,
    score INT,
    review_date DATE
);

CREATE TABLE IF NOT EXISTS bi_monitoramento_pipeline (
    data_execucao TIMESTAMP,
    rows_fact INT
);
