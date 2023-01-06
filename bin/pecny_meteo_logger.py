#! /usr/bin/python3
# -*- coding: utf-8 -*-

#
# Author: Jan Fuchs <fuky@asu.cas.cz>
#
# Copyright (C) 2022-2023 Astronomical Institute, Academy Sciences of the Czech Republic, v.v.i.
#
# AD4RS
# https://cdn.papouch.com/data/user-content/old_eshop/files/AD4RS_U_1/ad4rs.pdf
# https://cdn.papouch.com/data/user-content/old_eshop/files/AD4RS_U_1/ad4-drak4-spinel.pdf
#

import os
import sys
import time
import numpy
import serial
import schedule
import traceback
import logging
import sdnotify
import configparser

from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

SCRIPT_PATH = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))

PECNY_METEO_LOGGER_CFG = "%s/../etc/pecny_meteo_logger.cfg" % SCRIPT_PATH
PECNY_METEO_LOGGER_LOG = "%s/../log/pecny_meteo_logger.log" % SCRIPT_PATH

def init_logger(logger, filename):
    formatter = logging.Formatter("%(asctime)s - %(name)s[%(process)d] - %(levelname)s - %(message)s")

    fh = RotatingFileHandler(filename, maxBytes=10*1024**2, backupCount=10)
    #fh.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    #logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fh)

class PecnyMeteoLogger:

    def __init__(self, logger):
        self.logger = logger
        self.sd_notifier = sdnotify.SystemdNotifier()
        self.hw_serial = None
        self.empty_values = " ".join(6 * ["?"])
        self.error_counter = 0

        self.logger.info("Starting pecny_meteo_logger")

        self.load_cfg()

        self.hw_serial = serial.Serial(
            port=self.cfg["serial"]["port"],
            baudrate=self.cfg["serial"]["baudrate"],
            bytesize=self.cfg["serial"]["bytesize"],
            parity=self.cfg["serial"]["parity"],
            stopbits=self.cfg["serial"]["stopbits"],
            timeout=self.cfg["serial"]["timeout"])

        self.hw_serial.reset_input_buffer()
        self.hw_serial.reset_output_buffer()

    def load_cfg(self):
        rcp = configparser.ConfigParser()
        rcp.read(PECNY_METEO_LOGGER_CFG)

        self.cfg = {
            "logger": {},
            "serial": {},
        }

        callbacks = {
            "output_dir": rcp.get,
        }
        self.run_cfg_callbacks("logger", callbacks)


        callbacks = {
            "port": rcp.get,
            "baudrate": rcp.getint,
            "bytesize": rcp.getint,
            "parity": rcp.get,
            "stopbits": rcp.getint,
            "timeout": rcp.getint,
        }
        self.run_cfg_callbacks("serial", callbacks)

        for section in self.cfg:
            for item in self.cfg[section]:
                self.logger.debug("%s.%s = %s" % (section, item, self.cfg[section][item]))

    def run_cfg_callbacks(self, section, callbacks):
        for key in callbacks:
            self.cfg[section][key] = callbacks[key](section, key)

    def cmd_execute(self, cmd):
        data = ""
        prefix = cmd[:3]
        packet = b"%s\r" % cmd.encode()

        self.logger.debug("write %s" % packet)
        self.hw_serial.write(packet)

        for i in range(2):
            data = self.hw_serial.read_until(b"\r")
            self.logger.debug("read %s" % data)

            data = data.decode("ascii").strip()
            if data:
                if data.startswith(prefix):
                    data = data[3:]
                    break
                else:
                    self.logger.error("unknown data '%s'" % data)
            else:
                self.logger.error("no data")

        return data

    def cmd_mr_execute(self, idx):
        result = self.empty_values

        try:
            result = self.cmd_execute("*B%iMR0" % idx)
        except:
            self.error_counter += 1
            self.logger.exception("cmd_mr_execute(idx=%i) failed" % idx)

        return result

    def get_rain(self):
        rain = "?"

        try:
            cmd = "*B0CR01"
            rain = self.cmd_execute(cmd)
            if rain[0] != "0":
                self.logger.error("cmd '%s' (get rain) failed => '%s'" % (cmd, rain))

            cmd = "*B0CD01%s" % rain[-3:]
            result = self.cmd_execute(cmd)
            if result[0] != "0":
                self.logger.error("cmd '%s' (reset rain) failed => '%s'" % (cmd, result))
        except:
            self.error_counter += 1
            self.logger.exception("get_rain() failed")

        return rain

    def get_soil_moisture(self):
        self.logger.debug("get_soil_moisture")
        soil_moisture = [self.empty_values, self.empty_values]

        try:
            for idx in range(2):
                cmd = "*B0OS1H" # soil moisture power on
                result = self.cmd_execute(cmd)
                if result[0] != "0":
                    self.logger.error("cmd '%s' (soil moisture power on) failed => '%s'" % (cmd, result))

                time.sleep(2)

                cmd = "*B2MR0"
                soil_moisture[idx] = self.cmd_execute(cmd)
                if soil_moisture[idx][0] != "0":
                    self.logger.error("cmd '%s' (get soil_moisture) failed => '%s'" % (cmd, soil_moisture[idx]))

                cmd = "*B0OS1L" # soil moisture power off
                result = self.cmd_execute(cmd)
                if result[0] != "0":
                    self.logger.error("cmd '%s' (reset soil_moisture) failed => '%s'" % (cmd, result))

                time.sleep(15)
        except:
            self.error_counter += 1
            self.logger.exception("get_soil_moisture() failed")

        return soil_moisture

    def write_data(self, filename, human_filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, "a") as fo:
            line = "%(dt_str)s %(temp_humidity_barometer)s %(water_level)s %(wind)s %(rain)s %(soil_moisture1)s %(soil_moisture2)s" % self.data
            fo.write("%s\n" % line)

        try:
            init = not os.path.isfile(human_filename)

            os.makedirs(os.path.dirname(human_filename), exist_ok=True)

            with open(human_filename, "a") as output_fo:
                self.write_human_data(line, output_fo, init)
        except:
            self.logger.exception("write_human_data() failed")

    def write_human_data(self, line, output_fo, init):
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

        values_header_str = "      date     time rain_sum temperature humidity barometer surface_el sm_14cm sm_47cm sm_87cm surface_d wind_dir wind_speed"
        values_format_str = "%(      date)10s %(    time)8s %(rain_sum)8s %(temperature)11s %(humidity)8s %(barometer)9s %(surface_el)10s %(sm_14cm)7s %(sm_47cm)7s %(sm_87cm)7s %(surface_d)9s %(wind_dir)8s %(wind_speed)10s"

        # 2022-12-25 23:47:00 0 1 80 4352 2 80 9998 3 80 4924 4 80 0 0 1 80 0 2 80 0 3 80 0 4 80 7722 0 1 80 5848 2 80 2412 3 80 0 4 80 0 00 0 1 80 1354 2 80 1323 3 80 13936 4 80 7723 0 1 80 1331 2 80 1348 3 80 13934 4 80 7723
        items = line.split()
        if len(items) != 68:
            self.logger.warning("Skipping bad line %s" % line)
            return

        data = {}

        for key in conversion:
            c = conversion[key]

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
                output_fo.write("%s\n" % values_header_str)

            init = False
            output_fo.write("%s\n" % (values_format_str % data))

    def get_data(self, soil_moisture=False):
        record_flag = True
        dt = datetime.now(tz=timezone.utc)
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        dt_filename = dt.strftime("%Y-%m-%d.txt")
        dt_dirname = dt.strftime("%Y/%m")
        filename = os.path.join(self.cfg["logger"]["output_dir"], dt_dirname, dt_filename)
        human_filename = os.path.join(self.cfg["logger"]["output_dir"], "human", dt_dirname, dt_filename)

        self.clean_data(dt_str, soil_moisture=soil_moisture)

        if soil_moisture:
            soil_moisture1, soil_moisture2 = self.get_soil_moisture()
            self.data["soil_moisture1"] = soil_moisture1
            self.data["soil_moisture2"] = soil_moisture2
        else:
            for idx, key in [[1, "temp_humidity_barometer"], [2, "water_level"], [3, "wind"]]:
                result = self.cmd_mr_execute(idx)
                self.data[key] = result

            rain = self.get_rain()
            self.data["rain"] = rain

        if dt.minute in [0, 10, 20, 30, 40, 50] and not soil_moisture:
            record_flag = False

        if record_flag:
            self.write_data(filename, human_filename)

    def clean_data(self, dt_str, soil_moisture=False):

        for key in self.data:
            if not soil_moisture and not key.startswith("soil_moisture"):
               self.data[key] = self.empty_values
            elif soil_moisture and key.startswith("soil_moisture"):
               self.data[key] = self.empty_values

        self.data["dt_str"] = dt_str

    def clean_error_counter(self):
        self.logger.info("error_counter = %i, clean error_counter" % self.error_counter)
        self.error_counter = 0

    def run(self):
        self.sd_notifier.notify("READY=1")

        self.data = {
            "temp_humidity_barometer": self.empty_values,
            "water_level": self.empty_values,
            "wind": self.empty_values,
            "rain": "?",
            "soil_moisture1": self.empty_values,
            "soil_moisture2": self.empty_values,
            #"temperature": "?",
            #"humidity": "?",
            #"barometer": "?",
            #"water_level": "?",
            #"wind_speed": "?",
            #"wind_direction": "?",
        }

        # DBG
        #self.get_data()
        #self.get_data(soil_moisture=True)
        #schedule.every(1).minutes.do(self.clean_error_counter)
        #return

        schedule.every(1).hours.do(self.clean_error_counter)
        schedule.every().minute.at(":00").do(self.get_data)

        for minute in [":00", ":10", ":20", ":30", ":40", ":50"]:
            schedule.every().hour.at(minute).do(self.get_data, soil_moisture=True)

        while 1:
            if self.error_counter < 30:
                self.sd_notifier.notify("WATCHDOG=1")

            schedule.run_pending()
            self.logger.debug("sleep 1")
            time.sleep(1)

    def stop(self):
        if self.hw_serial is not None:
            self.hw_serial.close()

def main():
    logger = None
    pecny_meteo_logger = None

    try:
        logger = logging.getLogger("pecny_meteo_logger")
        init_logger(logger, PECNY_METEO_LOGGER_LOG)

        pecny_meteo_logger = PecnyMeteoLogger(logger)
        pecny_meteo_logger.run()
    except:
        traceback.print_exc()

        if logger is not None:
            logger.exception("main() exception")
    finally:
        if pecny_meteo_logger is not None:
            pecny_meteo_logger.stop()

if __name__ == '__main__':
    main()
