import os

from helo import db


def db_url():
    return os.getenv(db.ENV_KEY)
