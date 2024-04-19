from flask import Flask, render_template
from datetime import datetime


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('home/index.html')  # Assuming you have an index.html in your Docusaurus build

if __name__ == '__main__':
    app.run(debug=True)