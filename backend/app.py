from flask import Flask
from routes.trip_routes import trip_bp
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Register Blueprint
app.register_blueprint(trip_bp, url_prefix="/api/trips")

@app.route("/")
def home():
    return {"message": "NYC Taxi Trip API is running"}

if __name__ == "__main__":
    app.run(debug=True)

