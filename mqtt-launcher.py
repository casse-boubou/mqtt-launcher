#!/usr/bin/env python

# Copyright (c) 2014 Jan-Piet Mens <jpmens()gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of mosquitto nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__    = 'Jan-Piet Mens <jpmens()gmail.com>'
__copyright__ = 'Copyright 2014 Jan-Piet Mens'

import json
import logging
import os
import socket
import string
import subprocess
import sys
import time
from configparser import ConfigParser, NoOptionError, NoSectionError
from configparser import Error as ParserError

import paho.mqtt.client as paho # pip install paho-mqtt

CONFIG=os.getenv('MQTTLAUNCHERCONFIG', 'launcher.conf')





class Config:
    """This class is used to access config file."""
    def __init__(self, filename=CONFIG):
        self.parser = ConfigParser(
            inline_comment_prefixes="#",
            interpolation=None
        )
        self.read_config_file(filename)


    def read_config_file(self, configfile):
        """Read the config file."""
        try:
            with open(configfile, encoding='utf-8') as f:
                self.parser.read_file(f)
                self.parser.read(f)
        except UnicodeDecodeError as unidecerr:
            print(f"[INFO]: Cannot decode '{configfile}' with 'UTF-8' codec : {str(unidecerr)}")
            sys.exit(2)
        except ParserError as parserr:
            print(f"[INFO]: Cannot load configuration from file '{configfile}': {str(parserr)}")
            sys.exit(2)
        except FileNotFoundError as nofilerr:
            print(f"[INFO]: Cannot load configuration from file '{configfile}': {str(nofilerr)}")
            sys.exit(2)





cf = Config()

def get_value(section, option, default=None):
    """Get the value of an option, if it exists.
    Return the default value otherwise.
    """
    ret = default
    try:
        ret = cf.parser.get(section, option)
    except NoOptionError:
        print(
            f"[INFO]: A requested '{option}' option was not found. Use default Value: '{default}'"
        )
    except NoSectionError:
        print(
            f"[INFO]: A requested '{section}' section was not found. Use default Value: '{default}'"
        )

    return ret

def get_int_value(section, option, default=0):
    """Get the int value of an option, if it exists.
    Return the default value otherwise.
    """
    try:
        return cf.parser.getint(section, option)
    except NoOptionError:
        print(
            f"[INFO]: A requested '{option}' option was not found. Use default Value: '{default}'"
        )
        return int(default)
    except NoSectionError:
        print(
            f"[INFO]: A requested '{section}' section was not found. Use default Value: '{default}'"
        )
        return int(default)

def get_bool_value(section, option, default=False):
    """Get the bool value of an option, if it exists.
    Return the default value otherwise.
    """
    try:
        return cf.parser.getboolean(section, option)
    except NoOptionError:
        print(
            f"[INFO]: A requested '{option}' option was not found. Use default Value: '{default}'"
        )
        return bool(default)
    except NoSectionError:
        print(
            f"[INFO]: A requested '{section}' section was not found. Use default Value: '{default}'"
        )
        return bool(default)

def get_option_as_json(section, option):
    """Return options from section as a JSON."""
    option_as_json = get_value(section, option, {})
    try:
        option_as_json = json.loads(option_as_json)
    except json.decoder.JSONDecodeError as jsondecerr:
        line = str(jsondecerr.lineno-1) + "-" + str(jsondecerr.lineno)
        print(f"""[ERROR]: {str(jsondecerr)}
              Error when convert '{option}' in JSON.
              Maybe you added or missed a comma \',\'
              Maybe you are using single-quote (\') instead of double (\")
              Please look carefully at line '{line}' in the following""")
        print(f"{jsondecerr.doc}")
        sys.exit(2)
    except TypeError as jsontyperr:
        print(f"[ERROR]: {str(jsontyperr)}")
        print(f"""[INFO]: A requested '{option}' option was not found.
              PLEASE PROVIDE A TOPICLIST""")

    return option_as_json





DEFAULT_TOPIC = get_value('MQTT_TOPICS', 'client_topic', 'clients/mqtt-launcher')
QOS = get_int_value('MQTT_BROKER', 'mqtt_qos', '2')
LOGFILE = get_value('LOG', 'logfile', 'logfile')
DEBUG = get_bool_value('LOG', 'debug', True)
LOGFORMAT = '%(asctime)-15s %(message)s'
if DEBUG:
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE, level=logging.INFO, format=LOGFORMAT)





logging.info("Starting")
logging.debug("DEBUG MODE")

if __name__ == '__main__':

    userdata = {}

    topiclist = get_option_as_json('MQTT_TOPICS', 'topiclist')
    if topiclist == {}:
        logging.info("No topic list. Aborting")
        sys.exit(2)

    clientid = get_value('MQTT_BROKER', 'mqtt_clientid', f'{DEFAULT_TOPIC}-{os.getpid()}')

    transportType = get_value('MQTT_BROKER', 'mqtt_transport_type', 'tcp')

    # initialise MQTT broker connection
    mqttc = paho.Client(
        paho.CallbackAPIVersion.VERSION2,
        clientid,
        clean_session=False,
        transport=transportType
    )


    def on_connect(client, userdata, flags, reason_code, properties):
        """When the connection is established."""

        if reason_code == 0:
            logging.debug("Connected to MQTT broker, subscribing to topics...")
            for topic in topiclist:
                mqttc.subscribe(topic, QOS)
                logging.debug("Subscribed to Topic \"%s\", QOS %s", topic, QOS)
        if reason_code > 0:
            logging.debug("Connected with result code: %s", reason_code)
            logging.debug("No connection. Aborting")
            sys.exit(2)


    def on_message(client, userdata, msg):
        """When a new message is received on the topic."""

        logging.debug(msg.topic+" "+str(msg.qos)+" "+msg.payload.decode('utf-8'))
        runprog(msg.topic, msg.payload.decode('utf-8'))


    def on_disconnect(client, userdata, flags, reason_code, properties):
        """When the connection is over."""

        logging.debug("OOOOPS! launcher disconnects")
        time.sleep(10)


    def runprog(topic, param=None):
        """Execute the program command from config file."""

        publish = f"{topic}/report"

        if param is not None and all(c in string.printable for c in param) is False:
            logging.debug("Param for topic %s is not printable; skipping", topic)
            return

        if topic not in topiclist:
            logging.info("Topic %s isn't configured", topic)
            return

        if param is not None and param in topiclist[topic]:
            cmd = topiclist[topic].get(param)
        else:
            if None in topiclist[topic]: ### and topiclist[topic][None] is not None:
                cmd = [p.replace('@!@', param) for p in topiclist[topic][None]]
            else:
                logging.info("No matching param (%s) for %s", param, topic)
                return

        logging.debug("Running t=%s: %s", topic, cmd)

        try:
            res = subprocess.check_output(
                cmd,
                stdin=None,
                stderr=subprocess.STDOUT,
                shell=False,
                universal_newlines=True,
                cwd='/tmp'
            )
        except subprocess.SubprocessError as subprocerr:
            res = f"*****> {str(subprocerr)}"

        payload = res.rstrip('\n')
        (res, mid) =  mqttc.publish(publish, payload, qos=QOS, retain=False)


    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect

    mqttc.will_set(DEFAULT_TOPIC, payload="Adios!", qos=0, retain=False)

    # Delays will be: 3, 6, 12, 24, 30, 30, ...
    #mqttc.reconnect_delay_set(delay=3, delay_max=30, exponential_backoff=True)

    if get_value('MQTT_BROKER', 'mqtt_username', None) is not None:
        mqttc.username_pw_set(
            get_value('MQTT_BROKER', 'mqtt_username'),
            get_value('MQTT_BROKER', 'mqtt_password')
        )

    if get_bool_value('MQTT_BROKER', 'mqtt_tls', False) is True:
        mqttc.tls_set()
        mqtt_tls_verify_value = get_bool_value('MQTT_BROKER', 'mqtt_tls_verify', True)
        mqttc.tls_insecure_set(mqtt_tls_verify_value)

    if transportType == 'websockets':
        mqttc.ws_set_options(path="/ws")

    mqttc.connect(
        get_value('MQTT_BROKER', 'mqtt_broker', 'localhost'),
        get_int_value('MQTT_BROKER', 'mqtt_port', '1883'),
        60
    )

    while True:
        try:
            mqttc.loop_forever(retry_first_connection=False)
        except socket.error:
            time.sleep(5)
        except KeyboardInterrupt:
            sys.exit(0)