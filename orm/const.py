#!/usr/bin/env python3
# -*- coding:utf-8 -*-

def create_args_string(num):
    l = []
    for n in range(num):
        l.append('%s')
    return ', '.join(l)

space = '   |=> '
