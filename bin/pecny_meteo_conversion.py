#! /usr/bin/python3
# -*- coding: utf-8 -*-

#
# Author: Jan Fuchs <fuky@asu.cas.cz>
#
# Copyright (C) 2023 Astronomical Institute, Academy Sciences of the Czech Republic, v.v.i.
#

import os
import numpy

class PecnyMeteoConversion:

    def __init__(self):
        data_dir = "/home/fuky/kunzak/pecny/"
        output_dir = "/home/fuky/kunzak/pecny/human"

        for year in os.listdir(data_dir):
            year_dir = os.path.join(data_dir, year)
            for month in os.listdir(year_dir):
                month_dir = os.path.join(year_dir, month)
                for filename in os.listdir(month_dir):
                    input_fn = os.path.join(month_dir, filename)
                    if not os.path.isfile(input_fn) or not input_fn.endswith(".txt"):
                        continue

                    month_output_dir = os.path.join(output_dir, year, month)
                    output_fn = os.path.join(month_output_dir, filename)
                    os.makedirs(month_output_dir, exist_ok=True)

                    with open(output_fn, "w") as output_fo:
                        self.run_conversion(input_fn, output_fo)

    def run_conversion(self, input_fn, output_fo):
        with open(input_fn, "r") as fo:
            lines = fo.readlines()

        COLUMN_ID = 0
        CONSTANT_ID = 1
        LINEAR_ID = 2

        # value = constant + linear * raw_value
        # sm = soil_moisture
        # surface_el = surface_elevation
        # surface_d = surface_depth
        conversion = {
            "      date": [1, 0, 0],
            "    time": [2, 0, 0],
            "temperature": [6, -57.52, 0.01375],
            "humidity": [9, -35.12, 0.0125],
            "barometer": [12, 697.45, 0.05],
            "rain_sum": [42, 0.0, 0.1],
            "sm_14cm": [[46, 59], 0.0, 0.02],
            "sm_47cm": [[49, 62], 0.0, 0.02],
            "sm_87cm": [[52, 65], 0.0, 0.02],
            "surface_el": [28, 619.42, 0.003125],
            "surface_d": [28, 26.39, -0.003125],
            "wind_dir": [32, 90.0, 0.045],
            "wind_speed": [35, -7.50, 0.00375],
        }

        init = True
        # empty string is auto header
        values_header_str = "      date     time rain_sum temperature humidity barometer surface_el sm_14cm sm_47cm sm_87cm surface_d wind_dir wind_speed"
        # empty string is auto format
        values_format_str = "%(      date)10s %(    time)8s %(rain_sum)8s %(temperature)11s %(humidity)8s %(barometer)9s %(surface_el)10s %(sm_14cm)7s %(sm_47cm)7s %(sm_87cm)7s %(surface_d)9s %(wind_dir)8s %(wind_speed)10s"
        values_header = []
        values_format = []

        # 2022-12-25 23:47:00 0 1 80 4352 2 80 9998 3 80 4924 4 80 0 0 1 80 0 2 80 0 3 80 0 4 80 7722 0 1 80 5848 2 80 2412 3 80 0 4 80 0 00 0 1 80 1354 2 80 1323 3 80 13936 4 80 7723 0 1 80 1331 2 80 1348 3 80 13934 4 80 7723
        for line in lines:
            line = line.strip()
            items = line.split()
            if len(items) != 68:
                print("WARNING: Skipping line %s" % line)
                continue

            data = {}

            for key in conversion:
                c = conversion[key]

                if init:
                    values_header.append(key)
                    values_format.append("%%(%s)%is" % (key, len(key)))

                if key.startswith("sm_"):
                    raw_values_tmp = []
                    for idx in c[COLUMN_ID]:
                        number = items[idx-1]
                        if number == "?":
                            raw_value = "?"
                            break
                        raw_values_tmp.append(float(number))
                    else:
                        raw_value = numpy.mean(raw_values_tmp)
                else:
                    raw_value = items[c[COLUMN_ID]-1]

                if raw_value == "?":
                    data[key] = raw_value
                if key in ["      date", "    time"]:
                    data[key] = raw_value.replace(":", " ").replace("-", " ")
                else:
                    value = c[CONSTANT_ID] + c[LINEAR_ID] * float(raw_value)
                    if key == "wind_speed" and value < 0:
                        value = 0
                    data[key] = "%.2f" % value
            else:
                if init:
                    if not values_format_str:
                        values_format_str = " ".join(values_format)
                        #print(values_format_str)
                    if not values_header_str:
                        values_header_str = " ".join(values_header)
                    output_fo.write("%s\n" % values_header_str)

                init = False
                output_fo.write("%s\n" % (values_format_str % data))

def main():
    pecny_meteo_conversion = PecnyMeteoConversion()

if __name__ == '__main__':
    main()
