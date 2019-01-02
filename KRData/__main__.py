#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/2 0002 17:41
# @Author  : Hadrianl 
# @File    : __main__
import click
from .IBTradeRecorder import save_ib_trade

@click.group()
def cli():
    click.echo('KRData ToolKit!')

cli.add_command(save_ib_trade)