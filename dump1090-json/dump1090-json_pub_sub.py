"""This file contains the dump1090PubSub class which is a child class
of BaseMQTTPubSub.  The dump1090PubSub reads data from a specified
socket and publishes it to the MQTT broker.
"""
import os
from time import sleep
import json
from datetime import datetime
from typing import *
import schedule

import coloredlogs
import logging
import requests
import pandas as pd

from base_mqtt_pub_sub import BaseMQTTPubSub

STYLES = {
    "critical": {"bold": True, "color": "red"},
    "debug": {"color": "green"},
    "error": {"color": "red"},
    "info": {"color": "white"},
    "notice": {"color": "magenta"},
    "spam": {"color": "green", "faint": True},
    "success": {"bold": True, "color": "green"},
    "verbose": {"color": "blue"},
    "warning": {"color": "yellow"},
}
coloredlogs.install(
    level=logging.INFO,
    fmt="%(asctime)s.%(msecs)03d \033[0;90m%(levelname)-8s "
    ""
    "\033[0;36m%(filename)-18s%(lineno)3d\033[00m "
    "%(message)s",
    level_styles=STYLES,
)


class dump1090PubSub(BaseMQTTPubSub):
    """This class creates a connection to the MQTT broker and to the
    SBS1 socket on a piaware or dump1090 instance and publishes
    aircraft track json messages to an MQTT topic.

    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the
            EdgeTech Core module
    """

    __mqtt_broker: str = ""
    __mqtt_port: int = 0
    __dump1090_host: str = ""
    __dump1090_http_port: str = ""

    def __init__(
        self: Any,
        dump1090_host: str,
        dump1090_http_port: str,
        send_data_topic: str,
        debug: bool = False,
        **kwargs: Any,
    ):
        """The dump1090PubSub constructor takes a dump1090 IP and
        port, and MQTT topic.

        Args:
            dump1090_host (str): host IP of the dump1090 system
            dump1090_port (int): host port of the dump1090 socket
            send_data_topic (str): MQTT topic to publish the data from
                the port to. Specified via docker-compose.
            debug (bool, optional): If the debug mode is turned on,
              log statements print to stdout. Defaults to False.
        """
        super().__init__(**kwargs)
        # Convert contructor parameters to class variables
        self.send_data_topic = send_data_topic
        self.debug = debug
        self.__dump1090_host = dump1090_host
        self.__dump1090_http_port = int(dump1090_http_port)

        # Connect to the MQTT client
        self.connect_client()
        sleep(1)

        # Publish a message after successful connection to the MQTT
        # broker
        self.publish_registration("Dump1090 Sender Registration")

    def processAircraft(self):
        url = f"http://{self.__dump1090_host}:{self.__dump1090_http_port}/skyaware/data/aircraft.json"
        keepCols = [
            "hex",
            "lat",
            "lon",
            "alt_baro",
            "alt_geom",
            "gs",
            "track",
            "flight",
            "squawk",
            "geom_rate",
            "baro_rate",
            "seen",
        ]
        try:
            data = requests.get(url).content
        except Exception as e:
            logging.error(f"Could not connect to endpoint | {e}")
            return pd.DataFrame()
        try:
            tmpData = json.loads(data)
            timestamp = tmpData["now"]
            data = pd.read_json(json.dumps(tmpData["aircraft"]))
            dataoriginal = data
            columnsOut = []
            for colname in data.columns:
                if colname in keepCols:
                    columnsOut.append(colname)
            data = data[columnsOut]
            if "lat" not in data.columns:
                return pd.DataFrame()
            data = data[~pd.isna(data.lat)]
            data = data.fillna(0)
            if "geom_rate" in data.columns:
                data.geom_rate = data.geom_rate / 60 * 0.3048
            if "baro_rate" in data.columns:
                data.baro_rate = data.baro_rate / 60 * 0.3048
            if "alt_geom" in data.columns:
                data.alt_geom = data.alt_geom * 0.3048
            if "alt_baro" in data.columns:
                data.alt_baro = data.alt_baro * 0.3048
            if "gs" in data.columns:
                data.gs = data.gs * 0.5144444
            if "squawk" in data.columns:
                data["squawk"] = data["squawk"].astype(str)
            data["time"] = float(timestamp) - data.seen
            # TODO: Add in barometric offset to get geometric altitude for those aircraft that do not report it
            # tmp = data.loc[data.alt_geom!=0,'hex'][0]
            # baro_offset = data.loc[data.hex==tmp,'alt_geom'] - data.loc[data.hex==tmp,'alt_baro']
            # print(list(baro_offset)[0])
            # data.alt_geom = data.alt_baro+baro_offset
            self.processMessages(data)
        except Exception as e:
            logging.error(f"Could not process frame | {e}")
            logging.warning(f"System failed: {data}")

    def processMessages(self, data):
        if not data.empty:
            for aircraft in data.hex:
                tmp = data.loc[data.hex == aircraft]
                if "alt_geom" not in tmp.columns:
                    continue
                dataOut = {}
                dataOut["icao"] = tmp.hex.values[0]
                dataOut["time"] = str(tmp.time.values[0])
                dataOut["latitude"] = tmp.lat.values[0]
                dataOut["longitude"] = tmp.lon.values[0]
                dataOut["altitude"] = tmp.alt_geom.values[0]
                dataOut["horizontal_velocity"] = tmp.gs.values[0]
                dataOut["track"] = tmp.track.values[0]
                if "baro_rate" in tmp.columns:
                    dataOut["vertical_velocity"] = tmp.baro_rate.values[0]
                else:
                    if "geom_rate" in tmp.columns:
                        dataOut["vertical_velocity"] = tmp.geom_rate.values[0]
                    else:
                        dataOut["vertical_velocity"] = 0
                if "flight" in tmp.columns:
                    dataOut["flight"] = tmp.flight.values[0]
                if "squawk" in tmp.columns:
                    dataOut["squawk"] = tmp.squawk.values[0]
                # dataOut["onGround"] = tmp.hex
                self._send_data(dataOut)

    def _send_data(self: Any, data: Dict[str, str]) -> bool:
        """Leverages edgetech-core functionality to publish a JSON
        payload to the MQTT broker on the topic specified in the class
        constructor.

        Args:
            data (Dict[str, str]): Dictionary payload that maps keys
                to payload

        Returns:
            bool: Returns True if successful publish else False
        """
        # TODO: Provide fields via environment or command line
        out_json = self.generate_payload_json(
            push_timestamp=str(int(datetime.utcnow().timestamp())),
            device_type="Collector",
            # TODO: Rename device_id?
            id_="SkyScan-012",
            deployment_id=f"SkyScan-Arlington-Dump1090",
            current_location="-90, -180",
            status="Debug",
            message_type="Event",
            model_version="null",
            firmware_version="v0.0.1",
            data_payload_type="ADS-B",
            data_payload=json.dumps(data),
        )

        # Publish the data as a JSON to the topic
        success = self.publish_to_topic(self.send_data_topic, out_json)

        if self.debug:
            if success:
                print(
                    f"Successfully sent data on channel {self.send_data_topic}: {json.dumps(data)}"
                )
            else:
                print(
                    f"Failed to send data on channel {self.send_data_topic}: {json.dumps(data)}"
                )
        # Return True if successful else False
        return success

    def main(self: Any) -> None:
        """Main loop and function that setup the heartbeat to keep the
        TCP/IP connection alive and publishes the data to the MQTT
        broker and keeps the main thread alive.
        """
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="Dump1090 Sender Heartbeat"
        )
        schedule.every(1).seconds.do(self.processAircraft)

        while True:
            try:
                schedule.run_pending()
                delay = 0.001
                sleep(delay)

            except KeyboardInterrupt as exception:
                # If keyboard interrupt, fail gracefully
                if self.debug:
                    print(exception)


if __name__ == "__main__":
    sender = dump1090PubSub(
        send_data_topic=str(os.environ.get("JSON_OUTPUT_TOPIC")),
        dump1090_host=str(os.environ.get("DUMP1090_HOST")),
        dump1090_http_port=str(os.environ.get("DUMP1090_HTTP_PORT")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
    )
    sender.main()
