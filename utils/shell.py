import subprocess


def sh(func=None, **default_kwargs):
    if func is not None:
        def wrapper(*args, **kwargs):
            command = func(*args, **kwargs)
            merged = {**default_kwargs, **kwargs}
            return subprocess.run(command, shell=True, capture_output=True, text=True, **merged)
        return wrapper
    else:
        def decorator(f):
            def wrapper(*args, **kwargs):
                command = f(*args, **kwargs)
                merged = {**default_kwargs, **kwargs}
                return subprocess.run(command, shell=True, capture_output=True, text=True, **merged)
            return wrapper
        return decorator
