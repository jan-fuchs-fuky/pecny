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

    def write_data(self, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, "a") as fo:
            fo.write("%(dt_str)s %(temp_humidity_barometer)s %(water_level)s %(wind)s %(rain)s %(soil_moisture1)s %(soil_moisture2)s\n" % self.data)

    def get_data(self, soil_moisture=False):
        record_flag = True
        dt = datetime.now(tz=timezone.utc)
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        dt_filename = dt.strftime("%Y-%m-%d.txt")
        dt_dirname = dt.strftime("%Y/%m")
        filename = os.path.join(self.cfg["logger"]["output_dir"], dt_dirname, dt_filename)

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
            self.write_data(filename)

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
