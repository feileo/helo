#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import requests
from .qiniustore import qiniu_fetch_file,is_url,transform_to_http,qiniu_upload_file,save_pic
from requests.exceptions import Timeout,ConnectionError
from component import EventLogger
from .tempfile import SaveFiles


def save_to_qiniu_by_url(url):
    if not is_url(url):
        return ''
    new_url = transform_to_http(url)
    try:
        responce = requests.get(new_url)
    except ConnectionError as e:
        EventLogger.error(error=e,task='save_to_qiniu_by_url')
    if response.status_code != 200:
    	EventLogger.error('response status_code: {}'.format(response.status_code),
    		task='save_to_qiniu_by_url')
    	return str(url)
    return qiniu_upload_file(responce)
