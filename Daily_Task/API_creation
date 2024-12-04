import pandas as pd
from flask import Flask, jsonify
from sqlalchemy import create_engine, text

# Initialize the Flask application
app = Flask(__name__)

DATABASE_TYPE= 'postgresql'

PUBLIC_IP = "18.132.73.146"
USERNAME = "consultants"
PASSWORD = "WelcomeItc@2022"
DB_NAME = "testdb"
PORT = "5432"
#ENCODED_PASSWORD = quote_plus(PASSWORD)

#database_url = f'{DATABASE_TYPE}://{USERNAME}:{ENCODED_PASSWORD}@{PUBLIC_IP}:{PORT}/{DB_NAME}'
# Create a SQLAlchemy engine
engine = create_engine('postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb')
# print(database_url)
#engine = create_engine(database_url, echo=False)

db1 = pd.read_sql("bank", con=engine)
df =db1.to_dict(orient='records')

@app.route('/', methods=['GET'])
def get_data():
    data = df
    if data is not None:
        return jsonify(data), 200
    else:
        return jsonify({"error": "Unable to fetch data from database"}), 500

if __name__ == '__main__':
    # Run the app
  app.run(host='0.0.0.0', port=5329, debug=True)