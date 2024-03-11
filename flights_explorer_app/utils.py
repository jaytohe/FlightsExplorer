from functools import wraps
from flask import request
from flask.json import jsonify

#Thanks to https://arunmozhi.in/2019/07/26/simplifying-json-parsing-in-flask-routes-using-decorators/
def json_required_params(required):
    def decorator(fn):
        """
        Decorator that checks if a request is JSON and has the required parameters

        Exmaple Usage: 
            @route(...)
            @json_required_params({"name": str, "age": int, "married": bool})
            def ...
        """
 
        @wraps(fn)
        def wrapper(*args, **kwargs):

            if request.is_json is False:
                response = {
                    "status": "error",
                    "message": "A JSON request was expected with the following params",
                    "missing" : [r for r in required.keys()]
                }
                return jsonify(response), 400
            
            _json = request.get_json()
            missing = [r for r in required.keys()
                       if r not in _json]
            if missing:
                response = {
                    "status": "error",
                    "message": "Request JSON is missing some required params",
                    "missing": missing
                }
                return jsonify(response), 400
            wrong_types = [r for r in required.keys()
                           if not isinstance(_json[r], required[r])]
            if wrong_types:
                response = {
                    "status": "error",
                    "message": "Data types in the request JSON doesn't match the required format",
                    "param_types": {k: str(v) for k, v in required.items()}
                }
                return jsonify(response), 400
            return fn(*args, **kwargs)
        return wrapper
    return decorator