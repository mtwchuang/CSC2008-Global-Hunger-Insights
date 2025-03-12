import duckdb

def create_schema_tables(conn):
    """Create final tables to hold normalised data"""
    conn.execute("""
        DROP TABLE IF EXISTS region_dietary_fact;
        DROP TABLE IF EXISTS country_dietary_fact;
        DROP TABLE IF EXISTS ghi_fact;
        DROP TABLE IF EXISTS malnutrition_fact;
        DROP TABLE IF EXISTS ghi_dim;
        DROP TABLE IF EXISTS malnutrition_dim;
        DROP TABLE IF EXISTS nutrition_dim;
        DROP TABLE IF EXISTS food_dim;
        DROP TABLE IF EXISTS region_dim;
  
        DROP TABLE IF EXISTS country_dim;
    """)

    # Create the country dimension table
    conn.execute("""
        DROP TABLE IF EXISTS country_dim;
        CREATE TABLE country_dim(
            country_id CHAR(3) PRIMARY KEY,
            country_name CHAR(50) NOT NULL
        );
    """)

    # Create the region dimension table
    conn.execute("""
        DROP TABLE IF EXISTS region_dim;
        CREATE TABLE region_dim(
            region_id INTEGER PRIMARY KEY,
            region_name CHAR(50) NOT NULL
        );
    """)

    # Create the food dimension table
    conn.execute("""
        DROP TABLE IF EXISTS food_dim;
        CREATE TABLE food_dim(
            food_id INTEGER PRIMARY KEY,
            food_name CHAR(100) NOT NULL
        );
    """)
    # Create the nutrition dimension table
    conn.execute("""
        DROP TABLE IF EXISTS nutrition_dim;
        CREATE TABLE nutrition_dim(
            food_id INT NOT NULL,
            protein_per_kcal FLOAT NOT NULL,
            carbs_per_kcal FLOAT NOT NULL,
            fat_per_kcal FLOAT NOT NULL,
            FOREIGN KEY(food_id) REFERENCES food_dim(food_id)
        );
    """)
    # Create the malnutrition dimension table
    conn.execute("""
        DROP TABLE IF EXISTS malnutrition_dim;
        CREATE TABLE malnutrition_dim(
            malnutrition_id INTEGER PRIMARY KEY,
            malnutrition_type CHAR(50) NOT NULL
        );
    """)
    # Create the malnutrition fact table
    conn.execute("""
        DROP TABLE IF EXISTS children_malnutrition_fact;
        CREATE TABLE children_malnutrition_fact(
            country_id CHAR(3) NOT NULL,
            year INT NOT NULL,
            malnutrition_id INT NOT NULL,
            malnutrition_rate DOUBLE NOT NULL,
            FOREIGN KEY (country_id) REFERENCES country_dim(country_id),
            FOREIGN KEY (malnutrition_id) REFERENCES malnutrition_dim(malnutrition_id)
        );
    """)

    # Create the dietary composition by country fact table
    conn.execute("""
        DROP TABLE IF EXISTS country_dietary_fact;
        CREATE TABLE country_dietary_fact(
            country_id CHAR(3) NOT NULL,
            year INT NOT NULL, 
            food_id INT NOT NULL,
            kcal_per_day DOUBLE NOT NULL,
            FOREIGN KEY (country_id) REFERENCES country_dim(country_id),
            FOREIGN KEY (food_id) REFERENCES food_dim(food_id)
        );
    """)

    # Create the dietary composition by region fact table
    conn.execute("""
        DROP TABLE IF EXISTS region_dietary_fact;
        CREATE TABLE region_dietary_fact(
            region_id INT NOT NULL,
            year INT NOT NULL, 
            food_id INT NOT NULL,
            kcal_per_day DOUBLE NOT NULL,
            FOREIGN KEY (region_id) REFERENCES region_dim(region_id),
            FOREIGN KEY (food_id) REFERENCES food_dim(food_id)
        );
    """)

    # Create the global hunger index dimension table
    conn.execute("""
        DROP TABLE IF EXISTS ghi_fact;
        CREATE TABLE ghi_fact(
            country_id CHAR(3) NOT NULL,
            year INT NOT NULL,
            ghi DOUBLE NOT NULL,
            FOREIGN KEY (country_id) REFERENCES country_dim(country_id)
        );
    """)