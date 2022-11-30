#! /usr/bin/python3
# -*- coding: utf-8 -*-

#
# Author: Jan Fuchs <fuky@asu.cas.cz>
#
# Copyright (C) 2022 Astronomical Institute, Academy Sciences of the Czech Republic, v.v.i.
#
# AD4RS
# https://cdn.papouch.com/data/user-content/old_eshop/files/AD4RS_U_1/ad4rs.pdf
# https://cdn.papouch.com/data/user-content/old_eshop/files/AD4RS_U_1/ad4-drak4-spinel.pdf
#

import os
import sys
import time
import serial

from random import random, randint

class PecnyMeteoSimulator:

    def __init__(self):
        self.hw_serial = serial.Serial(
            port="/dev/ttyS1",
            baudrate=9600,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=5)

        self.hw_serial.reset_input_buffer()
        self.hw_serial.reset_output_buffer()

        self.loop()
        self.hw_serial.close()

    def make_rain(self):
        self.rain = randint(0, 100)

    def get_values(self):
        values = []
        for idx in range(0, 4):
            values.append("%.3f" % (random() * 100))

        return " ".join(values)

    def loop(self):
        self.make_rain()

        while 1:
            answer = ""
            data = self.hw_serial.read_until(b"\r").decode("ascii").strip()
            print("read '%s'" % data)

            if not data:
                continue
            # 1. measuring converter "*B10 1 2.156 12.457 5.002 0.001"
            elif data == "*B1MR0":
                answer = "*B10 1 %s" % self.get_values()
            # 2. measuring converter "*B20 2 0.002 0.000 0.001 5.002"
            elif data == "*B2MR0":
                answer = "*B20 2 %s" % self.get_values()
            # 3. measuring converter "*B30 3 2.156 12.457 0.002 0.001"
            elif data == "*B3MR0":
                answer = "*B30 3 %s" % self.get_values()
            # get rain
            elif data == "*B0CR01":
                answer = "*B00%03i" % self.rain
            # clean rain
            elif data.startswith("*B0CD01"):
                answer = "*B00"
                self.make_rain()
            # soil moisture power on
            elif data == "*B0OS1H":
                answer = "*B00"
            # soil moisture power off
            elif data == "*B0OS1L":
                answer = "*B00"

            if answer:
                packet = b"%s\r" % answer.encode()
                self.hw_serial.write(packet)

def main():
    pecny_meteo_simulator = PecnyMeteoSimulator()

if __name__ == '__main__':
    main()
