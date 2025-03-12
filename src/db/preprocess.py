import duckdb

def load_raw_data(conn):
    """Load raw data into the database"""
    # Creation of Dietary Composition Raw Table
    conn.execute("""
        DROP TABLE IF EXISTS dietary_composition_raw;
        CREATE TABLE dietary_composition_raw AS
        SELECT "Entity" AS country_name,
            "Code" AS country_id,
            "Year" AS year,
            "Miscellaneous group | 00002928 || Food available for consumption | 0664pc || kilocalories per day per capita" AS miscellanous_group,
            "Alcoholic Beverages | 00002924 || Food available for consumption | 0664pc || kilocalories per day per capita" AS alcoholic_beverages,
            "Animal fats group | 00002946 || Food available for consumption | 0664pc || kilocalories per day per capita" AS animal_fats,
            "Vegetable Oils | 00002914 || Food available for consumption | 0664pc || kilocalories per day per capita" AS vegetable_oils,
            "Oilcrops | 00002913 || Food available for consumption | 0664pc || kilocalories per day per capita" AS oil_crops,
            "Fish and seafood | 00002960 || Food available for consumption | 0664pc || kilocalories per day per capita" AS fish_and_seafood,
            "Sugar crops | 00002908 || Food available for consumption | 0664pc || kilocalories per day per capita" AS sugar_crops,
            "Sugar & Sweeteners | 00002909 || Food available for consumption | 0664pc || kilocalories per day per capita" AS sugar_and_sweeteners,
            "Starchy Roots | 00002907 || Food available for consumption | 0664pc || kilocalories per day per capita" AS starchy_roots,
            "Meat, Other | 00002735 || Food available for consumption | 0664pc || kilocalories per day per capita" AS other_meats,
            "Meat, sheep and goat | 00002732 || Food available for consumption | 0664pc || kilocalories per day per capita" AS sheep_and_goat,
            "Pork | 00002733 || Food available for consumption | 0664pc || kilocalories per day per capita" AS pork,
            "Meat, poultry | 00002734 || Food available for consumption | 0664pc || kilocalories per day per capita" AS poultry,
            "Meat, beef | 00002731 || Food available for consumption | 0664pc || kilocalories per day per capita" AS beef,
            "Eggs | 00002949 || Food available for consumption | 0664pc || kilocalories per day per capita" AS eggs,
            "Milk | 00002948 || Food available for consumption | 0664pc || kilocalories per day per capita" AS milk,
            "Nuts | 00002551 || Food available for consumption | 0664pc || kilocalories per day per capita" AS nuts,
            "Fruit | 00002919 || Food available for consumption | 0664pc || kilocalories per day per capita" AS fruit,
            "Vegetables | 00002918 || Food available for consumption | 0664pc || kilocalories per day per capita" AS vegetables,
            "Pulses | 00002911 || Food available for consumption | 0664pc || kilocalories per day per capita" AS pulses,
            "Cereals, Other | 00002520 || Food available for consumption | 0664pc || kilocalories per day per capita" AS other_cereals,
            "Barley | 00002513 || Food available for consumption | 0664pc || kilocalories per day per capita" AS barley,
            "Maize | 00002514 || Food available for consumption | 0664pc || kilocalories per day per capita" AS maize,
            "Rice | 00002807 || Food available for consumption | 0664pc || kilocalories per day per capita" AS rice,
            "Wheat | 00002511 || Food available for consumption | 0664pc || kilocalories per day per capita" AS wheat
        FROM read_csv_auto('data/raw/dietary-composition-by-country.csv', HEADER=TRUE);
    """)

    # Creation of Food Nutrients Raw Table
    conn.execute("""
        DROP TABLE IF EXISTS food_nutrients_raw;
        CREATE TABLE food_nutrients_raw AS 
        SELECT column0 AS food_name, 
            column1 AS fat_per_kcal, 
            column2 AS carbs_per_kcal, 
            column3 AS protein_per_kcal
        FROM read_csv_auto('data/raw/food-nutrients-by-food-type.csv', HEADER=FALSE, DELIM=';');
    """)

    # Creation and Population of Global Hunger Index Raw Table
    conn.execute("""
        DROP TABLE IF EXISTS global_hunger_index_raw;
        CREATE TABLE global_hunger_index_raw AS 
            SELECT "Entity" AS country_name, 
            "Code" AS country_id, 
            "Year" AS year, 
            "Global Hunger Index (2021)" AS ghi_score
        FROM read_csv_auto('data/raw/global-hunger-index.csv', HEADER=TRUE);
    """)

    # Creation of Underweight Children Raw Table
    conn.execute("""
        DROP TABLE IF EXISTS underweight_children_raw;
        CREATE TABLE underweight_children_raw AS 
            SELECT "Entity" AS country_name, 
            "Code" AS country_id, 
            "Year" AS year, 
            "Prevalence of underweight, weight for age (% of children under 5)" AS prevalence
        FROM read_csv_auto('data/raw/share-of-children-underweight.csv', HEADER=TRUE);
    """)

    # Creation of Wasting Children Raw Table
    conn.execute("""
        DROP TABLE IF EXISTS wasting_children_raw;
        CREATE TABLE wasting_children_raw AS 
            SELECT "Entity" AS country_name, 
            "Code" AS country_id, 
            "Year" AS year, 
            "Prevalence of wasting, weight for height (% of children under 5)" as prevalence
        FROM read_csv_auto('data/raw/share-of-children-with-a-weight-too-low-for-their-height-wasting.csv', HEADER=TRUE);
    """)

    # Creation of Stunting Children Raw Table
    conn.execute("""
        DROP TABLE IF EXISTS stunting_children_raw;
        CREATE TABLE stunting_children_raw AS 
            SELECT "Entity" AS country_name, 
            "Code" AS country_id, 
            "Year" AS year, 
            "Prevalence of stunting, height for age (% of children under 5)" as prevalence
        FROM read_csv_auto('data/raw/share-of-children-younger-than-5-who-suffer-from-stunting.csv', HEADER=TRUE);
    """)

def process_dietary_composition(conn):
    """Process and normalize dietary composition data"""
    # Create temp table, holding data starting from 2000, and without Miscellaneous group
    conn.execute("""
        DROP TABLE IF EXISTS dietary_composition_filtered;
        CREATE TABLE dietary_composition_filtered AS
            SELECT country_name,
                country_id,
                year,
                alcoholic_beverages,
                animal_fats,
                vegetable_oils,
                oil_crops,
                fish_and_seafood,
                sugar_crops,
                sugar_and_sweeteners,
                starchy_roots,
                other_meats,
                sheep_and_goat,
                pork,
                poultry,
                beef,
                eggs,
                milk,
                nuts,
                fruit,
                vegetables,
                pulses,
                other_cereals,
                barley,
                maize,
                rice,
                wheat
            FROM dietary_composition_raw
            WHERE year >= 2000
    """)
    # Create temp table, to hold unpivoted data, with food columns mapped into rows using a CTE
    conn.execute("""
        -- Create a food mapping CTE, to map column names to food names
        DROP TABLE IF EXISTS food_map;
        CREATE TABLE food_map AS (
            SELECT UNNEST([
                'alcoholic_beverages', 'animal_fats', 'vegetable_oils', 'oil_crops', 'fish_and_seafood',
                'sugar_crops', 'sugar_and_sweeteners', 'starchy_roots', 'other_meats', 'sheep_and_goat', 'pork',
                'poultry', 'beef', 'eggs', 'milk', 'nuts', 'fruit', 'vegetables', 'pulses', 'other_cereals', 'barley',
                'maize', 'rice', 'wheat']) AS "column_name",
                UNNEST(['alcoholic beverages', 'animal fats', 'vegetable oils', 'oil crops', 'fish and seafood',
                'sugar crops', 'sugar and sweeteners', 'starchy roots', 'other meats', 'sheep and goat', 'pork',
                'poultry', 'beef', 'eggs', 'milk', 'nuts', 'fruit', 'vegetables', 'pulses', 'other cereals', 'barley',
                'maize', 'rice', 'wheat']) AS "food_name"
        );
        -- Map food columns mapped into rows, using food_map CTE
        DROP TABLE IF EXISTS dietary_composition_unpivoted;
        CREATE TABLE dietary_composition_unpivoted AS
        SELECT 
            fd.country_name,
            fd.country_id,
            fd.year,
            fm.food_name,
            CASE fm.column_name
                WHEN 'alcoholic_beverages' THEN fd.alcoholic_beverages
                WHEN 'animal_fats' THEN fd.animal_fats
                WHEN 'vegetable_oils' THEN fd.vegetable_oils
                WHEN 'oil_crops' THEN fd.oil_crops
                WHEN 'fish_and_seafood' THEN fd.fish_and_seafood
                WHEN 'sugar_crops' THEN fd.sugar_crops
                WHEN 'sugar_and_sweeteners' THEN fd.sugar_and_sweeteners
                WHEN 'starchy_roots' THEN fd.starchy_roots
                WHEN 'other_meats' THEN fd.other_meats
                WHEN 'sheep_and_goat' THEN fd.sheep_and_goat
                WHEN 'pork' THEN fd.pork
                WHEN 'poultry' THEN fd.poultry
                WHEN 'beef' THEN fd.beef
                WHEN 'eggs' THEN fd.eggs
                WHEN 'milk' THEN fd.milk
                WHEN 'nuts' THEN fd.nuts
                WHEN 'fruit' THEN fd.fruit
                WHEN 'vegetables' THEN fd.vegetables
                WHEN 'pulses' THEN fd.pulses
                WHEN 'other_cereals' THEN fd.other_cereals
                WHEN 'barley' THEN fd.barley
                WHEN 'maize' THEN fd.maize
                WHEN 'rice' THEN fd.rice
                WHEN 'wheat' THEN fd.wheat
            END AS kcal_per_day
        FROM dietary_composition_filtered fd
        CROSS JOIN food_map fm
        WHERE 
            CASE fm.column_name
                WHEN 'alcoholic_beverages' THEN fd.alcoholic_beverages
                WHEN 'animal_fats' THEN fd.animal_fats
                WHEN 'vegetable_oils' THEN fd.vegetable_oils
                WHEN 'oil_crops' THEN fd.oil_crops
                WHEN 'fish_and_seafood' THEN fd.fish_and_seafood
                WHEN 'sugar_crops' THEN fd.sugar_crops
                WHEN 'sugar_and_sweeteners' THEN fd.sugar_and_sweeteners
                WHEN 'starchy_roots' THEN fd.starchy_roots
                WHEN 'other_meats' THEN fd.other_meats
                WHEN 'sheep_and_goat' THEN fd.sheep_and_goat
                WHEN 'pork' THEN fd.pork
                WHEN 'poultry' THEN fd.poultry
                WHEN 'beef' THEN fd.beef
                WHEN 'eggs' THEN fd.eggs
                WHEN 'milk' THEN fd.milk
                WHEN 'nuts' THEN fd.nuts
                WHEN 'fruit' THEN fd.fruit
                WHEN 'vegetables' THEN fd.vegetables
                WHEN 'pulses' THEN fd.pulses
                WHEN 'other_cereals' THEN fd.other_cereals
                WHEN 'barley' THEN fd.barley
                WHEN 'maize' THEN fd.maize
                WHEN 'rice' THEN fd.rice
                WHEN 'wheat' THEN fd.wheat
            END IS NOT NULL;
    """)
    # Insert data into the region_dim
    conn.execute("""
        INSERT INTO region_dim(region_id, region_name)
        SELECT ROW_NUMBER() OVER () AS region_id, country_name
        FROM (SELECT DISTINCT country_name FROM dietary_composition_unpivoted WHERE country_id IS NULL);
    """)
    # Insert data into the food_dim
    conn.execute("""
        INSERT INTO food_dim(food_id, food_name)
        SELECT ROW_NUMBER() OVER () AS food_id, food_name
        FROM (SELECT DISTINCT food_name FROM dietary_composition_unpivoted);
    """)
    # Insert data into the country-dim table
    conn.execute("""
        INSERT INTO country_dim(country_id, country_name)
        SELECT DISTINCT country_id, country_name
        FROM dietary_composition_unpivoted
        WHERE country_id IS NOT NULL;
    """)
    # Insert data into the region_dietary_fact
    conn.execute("""
        INSERT INTO region_dietary_fact
        SELECT r.region_id,
            fd.year,
            f.food_id,
            fd.kcal_per_day
        FROM dietary_composition_unpivoted fd
        INNER JOIN region_dim r
        ON fd.country_name = r.region_name
        INNER JOIN food_dim f
        USING (food_name)
        WHERE fd.country_id IS NULL
        ORDER BY r.region_id, fd.year, f.food_id ASC;
    """)              
    # Insert into country_dietary_fact table
    conn.execute("""
        INSERT INTO country_dietary_fact
        SELECT c.country_id,
            fd.year,
            f.food_id,
            fd.kcal_per_day
        FROM dietary_composition_unpivoted fd
        INNER JOIN country_dim c
        USING (country_name)
        INNER JOIN food_dim f
        USING (food_name)
        WHERE fd.country_id IS NOT NULL
        ORDER BY c.country_id, fd.year, f.food_id ASC;
    """)

def process_food_nutrition(conn):
    """Process and normalize food nutrition data"""
    # Rename columns
    conn.execute("""
        DROP TABLE IF EXISTS food_nutrients_cleaned;
        CREATE TABLE food_nutrients_cleaned AS
        SELECT 
            CASE food_name
                WHEN 'meat_generic' THEN 'other meats'
                WHEN 'cereal_generic' THEN 'other cereals'
                ELSE TRIM(REPLACE(REPLACE(food_name, '_',' '), 'generic', ''))
            END AS food_name,
            fat_per_kcal,
            carbs_per_kcal,
            protein_per_kcal
        FROM food_nutrients_raw;
    """)
    # Insert into nutrition_dim table
    conn.execute("""
        INSERT INTO nutrition_dim
        SELECT f.food_id,
            fnc.fat_per_kcal,
            fnc.carbs_per_kcal,
            fnc.protein_per_kcal
        FROM food_nutrients_cleaned fnc
        INNER JOIN food_dim f
        USING (food_name)
        
    """)

def process_global_hunger_index(conn):
    """Process and normalize global hunger index data"""
    # Insert new countries entries from global hunger in Country
    conn.execute("""
        INSERT INTO country_dim(country_id, country_name)
        SELECT DISTINCT country_id, country_name
        FROM global_hunger_index_raw
        WHERE country_id NOT IN (SELECT country_id FROM country_dim);
    """)
    # Insert into global_hunger_index table
    conn.execute("""
        INSERT INTO ghi_fact
        SELECT c.country_id,
            ghi.year,
            ghi.ghi_score
        FROM global_hunger_index_raw ghi
        INNER JOIN country_dim c
        USING (country_name)
        ORDER BY c.country_id, ghi.year ASC;
    """)

def process_children_malnutrition_prevance_data(conn):
    """Process and normalize children malnutrition prevalence data"""
    # Combine all tables together
    conn.execute("""
        DROP TABLE IF EXISTS children_malnutrition_combined;
        CREATE TABLE children_malnutrition_combined AS
        SELECT country_name, 
            country_id, 
            year, 
            'underweight' AS malnutrition_type, 
            prevalence
        FROM underweight_children_raw
        UNION
        SELECT country_name, 
            country_id, 
            year, 
            'wasting' AS malnutrition_type, 
            prevalence
        FROM wasting_children_raw
        UNION
        SELECT country_name, 
            country_id, 
            year, 
            'stunting' AS malnutrition_type, 
            prevalence
        FROM stunting_children_raw;
    """)
    # Insert new countries entries from children_malnutrition_combined in Country
    conn.execute("""
        INSERT INTO country_dim(country_id, country_name)
        SELECT DISTINCT country_id, country_name
        FROM children_malnutrition_combined
        WHERE country_id NOT IN (SELECT country_id FROM country_dim);
    """)
    # Insert into malnutrition_dim table
    conn.execute("""
        INSERT INTO malnutrition_dim(malnutrition_id, malnutrition_type)
        SELECT ROW_NUMBER() OVER () AS malnutrition_id, malnutrition_type
        FROM (SELECT DISTINCT malnutrition_type FROM children_malnutrition_combined);
    """)
    # Insert into children_malnutrition_fact table
    conn.execute("""
        INSERT INTO children_malnutrition_fact
        SELECT c.country_id,
            cmc.year,
            m.malnutrition_id,
            cmc.prevalence
        FROM children_malnutrition_combined cmc
        INNER JOIN country_dim c
        USING (country_name)
        INNER JOIN malnutrition_dim m
        USING (malnutrition_type)
        ORDER BY c.country_id, cmc.year, m.malnutrition_id ASC;
    """)