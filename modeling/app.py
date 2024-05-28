from flask import Flask, render_template, request, jsonify
from predict import predict_flight_delays

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['GET'])
def search_flights():
    # Retrieve the form data
    departure_city = request.args.get('departure-city')
    destination = request.args.get('destination')
    start_date = request.args.get('start-date')
    end_date = request.args.get('end-date')

    print([departure_city,destination,start_date,end_date])

    # Call prediction model to get flight predictions
    flights = predict_flight_delays(departure_city, destination, start_date, end_date)

    # Sort flights by expected delay time
    #sorted_flights = sorted(flights, key=lambda x: x['delay_time'])

    # Return sorted flights as JSON response
    return jsonify(flights)

if __name__ == '__main__':
    app.run(debug=True)
