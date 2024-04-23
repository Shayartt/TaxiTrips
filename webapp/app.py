from flask import Flask, render_template
from datetime import datetime
import os 
import sys

# Second Level import :
from models import CassandraReader, OpenSearchReader, AnalyticsGenerator

app = Flask(__name__)

# Init Cassandra Handler
cassandra_handler = CassandraReader()
opensearch_handler = OpenSearchReader()

@app.route('/')
def index():
    analytics_generated = AnalyticsGenerator(cassandra_handler, opensearch_handler)
    
    my_dashboard_variables = analytics_generated.load_dashboard_variables()
    print("Variables loaded are : " + str(my_dashboard_variables))
    
    return render_template('home/index.html', my_variable = my_dashboard_variables)  # Assuming you have an index.html in your Docusaurus build

if __name__ == '__main__':
    app.run(debug=True)