from functools import wraps


def load(func):

    @wraps(func)
    def do(*args, **kwargs):
        pass
    return do
