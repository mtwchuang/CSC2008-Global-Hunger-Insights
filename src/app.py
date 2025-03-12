import db.connection
import db.preprocess
import db.models
import streamlit as st



# Set up connection
conn = db.connection.get_connection()
db.preprocess.load_raw_data(conn)
db.models.create_schema_tables(conn)
db.preprocess.process_dietary_composition(conn)
db.preprocess.process_food_nutrition(conn)
db.preprocess.process_global_hunger_index(conn)
db.preprocess.process_children_malnutrition_prevance_data(conn)