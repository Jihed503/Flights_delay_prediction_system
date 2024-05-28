def predict_flight_delays(origin:str, destination:str, start_date, end_date) -> list[dict]:
    return [
                {'date': start_date, 'from': origin, 'to': destination, 'aircraft': 'XYZ456', 'flight_time':'02:30', 'std':'09:30', 'atd':'09:40', 'sta':'12:00', 'status':'1'},
                {'date': end_date, 'from': origin, 'to': destination, 'aircraft': 'XYZ456', 'flight_time':'02:30', 'std':'09:30', 'atd':'09:40', 'sta':'12:00', 'status':'0'},
            ]
