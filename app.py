# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 10:58:46 2024

@author: user
"""

from flask import Flask
import main

app = Flask(__name__)

@app.route("/")
def _main():
    return str(main.main())

@app.route("/v")
def Visualization():
    return str(main.Visualize())




if __name__ == '__main__':
   app.run()