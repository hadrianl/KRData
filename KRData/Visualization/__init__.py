#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/11 0011 16:07
# @Author  : Hadrianl 
# @File    : __init__.py

import sys
from PyQt5 import QtGui, QtWidgets, QtCore
import os
import platform
import ctypes
from .widget import KLineWidget

def create_qapp(app_name: str = "KaiRui Visualization"):
    """
    Create Qt Application.
    """

    vapp = QtWidgets.QApplication([])


    font = QtGui.QFont("Arial", 12)
    vapp.setFont(font)

    icon = QtGui.QIcon(os.path.join(os.path.dirname(__file__), "kairuiLOGO.png"))
    vapp.setWindowIcon(icon)

    if "Windows" in platform.uname():
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            app_name
        )

    return vapp

