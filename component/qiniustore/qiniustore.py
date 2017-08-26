#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import qiniu,hashlib,time,os
import urllib.parse as urlparse
from component.logger import EventLogger
from .tempfile import TempFiles
from .const import BUCKET_NAME,ACCESS_KEY,SECRET_KEY,DOMAIN


def qiniu_fetch_file(purl):
    max_retry = 5

    if not is_url(purl):
        EventLogger.warning(task='qiniu_fetch',message='input url:%s' % purl)
        return ''
    purl = transform_to_http(purl)
    q = qiniu.Auth(ACCESS_KEY, SECRET_KEY)
    Bucket_path = qiniu.BucketManager(q)
    for n in range(max_retry):
        ret = Bucket_path.fetch(purl, BUCKET_NAME)
        if ret is None:
            continue
        elif isinstance(ret,tuple) and ret[0] is None:
            continue
        else:
            key = ret[0]['key']
            url = DOMAIN + str(key)
            obj = urlparse.urlparse(url)
            return obj.geturl()
    else:
        EventLogger.error(task='qiniu_fetch', message='max retry exceed')
        return purl


def rename(old_name):
    post_fix = old_name.split('.')[-1]
    fullname = old_name.split('.')[0]
    salt = 'ouououou'
    all_name = '%s-%s-%s' % (fullname,time.time(),salt)
    sha_obj = hashlib.sha1(all_name.encode('utf-8'))
    new_fullname = qiniu.urlsafe_base64_encode(sha_obj.digest()).replace('=','')
    final_name = '.'.join([new_fullname,post_fix])
    return final_name


def get_hash(byte_stream):
    sha_obj = hashlib.sha256(byte_stream)
    hash_code = qiniu.urlsafe_base64_encode(sha_obj.digest()).replace('=','')
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
        return url.replace('https','http')
    return url


def save_qiniu(name,img_dir):
    q = qiniu.Auth(ACCESS_KEY, SECRET_KEY)
    bucket_name = BUCKET_NAME
    key = name
    token = q.upload_token(BUCKET_NAME, key, 12000)
    localfile = os.path.sep.join([img_dir,name])
    ret, info = qiniu.put_file(token, key, localfile)
    assert ret['key'] == key
    assert ret['hash'] == qiniu.etag(localfile)
    return DOMAIN+key


def save_pic(file,responce):
    for chunk in responce.iter_content(chunk_size=1024):
        if chunk:
            file.write(chunk)


def qiniu_upload_file(responce):
    file_name = get_hash(responce.content)
    store = TempFiles(file_name)
    store.save(save_pic, responce)
    try:
        result_url = save_qiniu(file_name,os.path.dirname(store.filename))
    except Exception as e:
        EventLogger.error(error=e,task='qiniu_upload_file')
        result_url = str(responce.url)
    store.remove()
    return result_url
