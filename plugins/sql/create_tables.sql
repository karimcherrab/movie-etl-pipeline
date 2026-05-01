CREATE TABLE IF NOT EXISTS Dim_Movie (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    original_title VARCHAR(255),
    status VARCHAR(50),
    tagline TEXT,
    homepage VARCHAR(255),
    adult BOOLEAN);

CREATE TABLE IF NOT EXISTS Dim_Date (

    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL
);
CREATE TABLE IF NOT EXISTS Dim_Genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Dim_Actor (
    actor_id INT PRIMARY KEY,
    actor_name VARCHAR(255) NOT NULL,
    gender VARCHAR(20),
    popularity FLOAT
);


CREATE TABLE IF NOT EXISTS Dim_Company (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    origin_country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Dim_Keyword (
    keyword_id INT PRIMARY KEY,
    keyword_name VARCHAR(100) NOT NULL UNIQUE
);
-- =========================
-- TABLE DE FAITS
-- =========================

CREATE TABLE IF NOT EXISTS Fact_Movie (
    fact_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    movie_id INT NOT NULL,
    date_id INT NOT NULL,
    budget NUMERIC(20,2),
    revenue NUMERIC(20,2),
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INT,
    runtime INT,

    CONSTRAINT fk_fact_movie
        FOREIGN KEY (movie_id)
        REFERENCES Dim_Movie(movie_id),

    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_id)
        REFERENCES Dim_Date(date_id)
);
-- =========================
-- TABLES DE LIAISON (BRIDGE)
-- =========================

CREATE TABLE IF NOT EXISTS Bridge_Movie_Genre (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (movie_id, genre_id),

    CONSTRAINT fk_bmg_movie
        FOREIGN KEY (movie_id)
        REFERENCES Dim_Movie(movie_id),

    CONSTRAINT fk_bmg_genre
        FOREIGN KEY (genre_id)
        REFERENCES Dim_Genre(genre_id)
);

CREATE TABLE IF NOT EXISTS Bridge_Movie_Actor (
    movie_id INT NOT NULL,
    actor_id INT NOT NULL,
    character_name VARCHAR(255),
    cast_order INT,
    PRIMARY KEY (movie_id, actor_id),

    CONSTRAINT fk_bma_movie
        FOREIGN KEY (movie_id)
        REFERENCES Dim_Movie(movie_id),

    CONSTRAINT fk_bma_actor
        FOREIGN KEY (actor_id)
        REFERENCES Dim_Actor(actor_id)
);

CREATE TABLE IF NOT EXISTS Bridge_Movie_Keyword (
    movie_id INT NOT NULL,
    keyword_id INT NOT NULL,
    PRIMARY KEY (movie_id, keyword_id),

    CONSTRAINT fk_bmk_movie
        FOREIGN KEY (movie_id)
        REFERENCES Dim_Movie(movie_id),

    CONSTRAINT fk_bmk_keyword
        FOREIGN KEY (keyword_id)
        REFERENCES Dim_Keyword(keyword_id)
);

-- =========================
-- INDEX POUR PERFORMANCE
-- =========================

CREATE INDEX IF NOT EXISTS idx_fact_movie_movie ON Fact_Movie(movie_id);
CREATE INDEX IF NOT EXISTS idx_fact_movie_date ON Fact_Movie(date_id);

CREATE INDEX IF NOT EXISTS idx_bmg_genre ON Bridge_Movie_Genre(genre_id);
CREATE INDEX IF NOT EXISTS idx_bma_actor ON Bridge_Movie_Actor(actor_id);
CREATE INDEX IF NOT EXISTS idx_bmk_keyword ON Bridge_Movie_Keyword(keyword_id);