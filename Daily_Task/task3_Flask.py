# Using flask to make an api
# import necessary libraries and functions
from flask import Flask, jsonify, request

# creating a Flask app
app = Flask(__name__)

# Error Handler for General Exceptions
@app.errorhandler(Exception)
def handle_exception(e):
    # Catch all unexpected errors
    return jsonify({"error": str(e), "message": "An unexpected error occurred"}), 500

# Error Handler for 404 Not Found
@app.errorhandler(404)
def not_found_error(e):
    return jsonify({"error": "Not Found", "message": "The requested URL was not found on the server"}), 404

# Error Handler for 405 Method Not Allowed
@app.errorhandler(405)
def method_not_allowed_error(e):
    return jsonify({"error": "Method Not Allowed", "message": "The method is not allowed for the requested URL"}), 405

# Error Handler for Type Errors or Invalid Inputs
@app.errorhandler(ValueError)
def value_error(e):
    return jsonify({"error": "Value Error", "message": str(e)}), 400

# Root Route: Returns hello world when using GET
@app.route('/', methods=['GET', 'POST'])
def home():
    try:
        if request.method == 'GET':
            data = "hello world"
            return jsonify({'data': data})
        elif request.method == 'POST':
            # Example: Simulate error handling for invalid data in POST
            json_data = request.get_json()
            if not json_data:
                raise ValueError("Invalid JSON data sent in request")
            return jsonify({'received_data': json_data})
    except ValueError as ve:
        # Handle ValueError explicitly
        return jsonify({"error": "Invalid Request", "message": str(ve)}), 400
    except Exception as e:
        # Catch other unexpected errors
        return jsonify({"error": "Server Error", "message": str(e)}), 500

# Route to Calculate Square of a Number
@app.route('/home/<int:num>', methods=['GET'])
def disp(num):
    try:
        if num < 0:
            raise ValueError("Number must be non-negative")
        return jsonify({'data': num**2})
    except ValueError as ve:
        return jsonify({"error": "Invalid Input", "message": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": "Server Error", "message": str(e)}), 500

# Driver function
if __name__ == '__main__':
    try:
        app.run(debug=True)
    except Exception as e:
        print(f"Error starting the server: {e}")
