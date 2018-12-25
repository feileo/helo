import hashlib
import os
import time
import urllib.parse as urlparse

import qiniu
import requests

from trod.extra.config import (
    ACCESS_KEY, SECRET_KEY, BUCKET_NAME,
    DOMAIN, TEMP_FILES_DIR, TASK_PATH
)
from trod.extra.logger import find_eventname, Logger


class TempFiles:
    __dir = TEMP_FILES_DIR

    def __init__(self, filename):
        self.status = False
        taskname = find_eventname(TASK_PATH)
        path = os.path.sep.join([TempFiles.__dir, taskname])
        if not os.path.exists(path):
            os.makedirs(path)
        self.filename = os.path.sep.join([path, filename])

    def save(self, method, *args, **kwargs):
        with open(self.filename, 'wb') as f:
            method(f, *args, **kwargs)
        self.status = True

    def remove(self):
        if self.status is True:
            os.remove(self.filename)


class SaveFiles:

    def __init__(self, base_path, relative_path):
        full_path = os.path.abspath(os.path.join(base_path, relative_path))
        dirname = os.path.dirname(full_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.filename = full_path

    def save(self, method, *args, **kwargs):
        with open(self.filename, 'wb') as f:
            method(f, *args, **kwargs)


def qiniu_fetch_file(purl):
    max_retry = 5

    if not is_url(purl):
        Logger.warning(task='qiniu_fetch', message='input url:%s' % purl)
        return ''
    purl = transform_to_http(purl)
    q_auth = qiniu.Auth(ACCESS_KEY, SECRET_KEY)
    bucket_path = qiniu.BucketManager(q_auth)
    for n in range(max_retry):
        ret = bucket_path.fetch(purl, BUCKET_NAME)
        if ret is None:
            continue
        elif isinstance(ret, tuple) and ret[0] is None:
            continue
        else:
            key = ret[0]['key']
            url = DOMAIN + str(key)
            obj = urlparse.urlparse(url)
            return obj.geturl()
    else:
        Logger.error(task='qiniu_fetch', message='max retry exceed')
        return purl


def rename(old_name):
    post_fix = old_name.split('.')[-1]
    fullname = old_name.split('.')[0]
    salt = 'fewihsdhwidw'
    all_name = '{}-{}-{}'.format(fullname, time.time(), salt)
    sha_obj = hashlib.sha1(all_name.encode('utf-8'))
    new_fullname = qiniu.urlsafe_base64_encode(
        sha_obj.digest()
    ).replace('=', '')
    final_name = '.'.join([new_fullname, post_fix])
    return final_name


def get_hash(byte_stream):
    sha_obj = hashlib.sha256(byte_stream)
    hash_code = qiniu.urlsafe_base64_encode(
        sha_obj.digest()
    ).replace('=', '')
    return hash_code


def is_url(url):
    if url is None:
        return False
    if url.find('http') == -1:
        return False
    return True


def transform_to_http(url):
    obj_res = urlparse.urlparse(url)
    if obj_res.scheme == 'https':
        return url.replace('https', 'http')
    return url


def save_qiniu(name, img_dir):
    q_auth = qiniu.Auth(ACCESS_KEY, SECRET_KEY)
    # bucket_name = BUCKET_NAME
    key = name
    token = q_auth.upload_token(BUCKET_NAME, key, 12000)
    localfile = os.path.sep.join([img_dir, name])
    ret, info = qiniu.put_file(token, key, localfile)
    assert ret['key'] == key
    assert ret['hash'] == qiniu.etag(localfile)
    return DOMAIN+key


def save_pic(file, responce):
    for chunk in responce.iter_content(chunk_size=1024):
        if chunk:
            file.write(chunk)


def qiniu_upload_file(responce):
    file_name = get_hash(responce.content)
    store = TempFiles(file_name)
    store.save(save_pic, responce)
    try:
        result_url = save_qiniu(file_name, os.path.dirname(store.filename))
    except Exception as e:
        Logger.error(error=e, task='qiniu_upload_file')
        result_url = str(responce.url)
    store.remove()
    return result_url


def save_to_qiniu_by_url(url):
    if not is_url(url):
        return ''
    new_url = transform_to_http(url)
    try:
        response = requests.get(new_url)
    except ConnectionError as e:
        Logger.error(error=e, task='save_to_qiniu_by_url')
    if response.status_code != 200:
        Logger.error('response status_code: {}'.format(response.status_code),
                     task='save_to_qiniu_by_url')
        return str(url)
    return qiniu_upload_file(response)
