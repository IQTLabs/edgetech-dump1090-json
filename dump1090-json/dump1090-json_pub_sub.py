"""This file contains the dump1090PubSub class which is a child class
of BaseMQTTPubSub.  The dump1090PubSub gets data from a specified
dump1090 endpoint and publishes it to the MQTT broker.
"""
from datetime import datetime
import json
import logging
import os
import sys
from time import sleep
from typing import Any, Dict

import coloredlogs
import pandas as pd
import schedule
import requests

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
    """Gets data from a specified dump1090 endpoint and publishes it
    to the MQTT broker.
    """

    def __init__(
        self: Any,
        dump1090_host: str,
        dump1090_http_port: str,
        send_data_topic: str,
        debug: bool = False,
        **kwargs: Any,
    ):
        """
        Connect to the MQTT broker, and log parameters.

        Args:
            dump1090_host (str): Host IP of the dump1090 system
            dump1090_port (str): Host port of the dump1090 socket
            send_data_topic (str): MQTT topic to publish the data from
                the port to. Specified via docker-compose.
            TODO: Remove?
            debug (bool, optional): If the debug mode is turned on,
              log statements print to stdout. Defaults to False.
        """
        super().__init__(**kwargs)
        self.dump1090_host = dump1090_host
        self.dump1090_http_port = dump1090_http_port
        self.send_data_topic = send_data_topic
        self.debug = debug

        # Connect to the MQTT client
        self.connect_client()
        sleep(1)
        self.publish_registration("Dump1090 Sender Registration")

        # Log configuration parameters
        logging.info(
            f"""dump1090PubSub initialized with parameters:
    dump1090_host = {dump1090_host}
    dump1090_http_port = {dump1090_http_port}
    send_data_topic = {send_data_topic}
    debug = {debug}
            """
        )

    def _process_response(self) -> None:
        """Process the response from the Dump1090 aircraft endpoint,
        convert to standard units, and process the resulting data.
        """
        # Get and load the response from the endpoint
        url = f"http://{self.dump1090_host}:{self.dump1090_http_port}/skyaware/data/aircraft.json"
        try:
            response = json.loads(requests.get(url).text)

        except Exception as e:
            logging.error(f"Could not connect to endpoint or load response | {e}")
            return

        # Convert to standard units
        try:
            data = pd.read_json(json.dumps(response["aircraft"]))
            if "lat" not in data.columns:
                return
            data = data[~pd.isna(data.lat)]
            data = data.fillna(0.0)
            data["timestamp"] = float(response["now"]) - data.seen
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
            # TODO: Add in barometric offset to get geometric altitude for those aircraft that do not report it
            # tmp = data.loc[data.alt_geom!=0,'hex'][0]
            # baro_offset = data.loc[data.hex==tmp,'alt_geom'] - data.loc[data.hex==tmp,'alt_baro']
            # print(list(baro_offset)[0])
            # data.alt_geom = data.alt_baro+baro_offset
            logging.debug(f"Processed data from response: {data}")

        except Exception as e:
            logging.error(f"Could not process response | {e}")

        # Process data from the response
        self._process_data(data)

    def _process_data(self, inp_data: pd.DataFrame) -> None:
        """Select and send required data selected from from the
        Dump1090 endpoint response.

        Args:
            inp_data (pd.DataFrame): Processed data from the Dump1090
                endpoint response
        """
        if inp_data.empty:
            return
        for aircraft in inp_data.hex:
            try:
                # Select the first data row for each aircraft
                vld_data = inp_data.loc[inp_data.hex == aircraft]
                if "alt_geom" not in vld_data.columns:
                    continue
                out_data = {}
                out_data["icao_hex"] = vld_data.hex.values[0]
                out_data["timestamp"] = vld_data.timestamp.values[0]
                out_data["latitude"] = vld_data.lat.values[0]
                out_data["longitude"] = vld_data.lon.values[0]
                out_data["altitude"] = vld_data.alt_geom.values[0]
                out_data["horizontal_velocity"] = vld_data.gs.values[0]
                out_data["track"] = vld_data.track.values[0]
                if "baro_rate" in vld_data.columns:
                    out_data["vertical_velocity"] = vld_data.baro_rate.values[0]
                else:
                    if "geom_rate" in vld_data.columns:
                        out_data["vertical_velocity"] = vld_data.geom_rate.values[0]
                    else:
                        out_data["vertical_velocity"] = 0
                if "flight" in vld_data.columns:
                    out_data["flight"] = vld_data.flight.values[0]
                if "squawk" in vld_data.columns:
                    out_data["squawk"] = vld_data.squawk.values[0]
                # out_data["onGround"] = vld_data.hex
            except Exception as e:
                logging.error(f"Could not select data | {e}")

            # Send selected data
            self._send_data(out_data)

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
        # Generate payload
        payload_json = self.generate_payload_json(
            push_timestamp=int(datetime.utcnow().timestamp()),
            device_type=os.getenv("DEVICE_TYPE", ""),
            id_=os.getenv("HOSTNAME", ""),
            deployment_id=os.getenv("DEPLOYMENT_ID", ""),
            current_location=os.getenv("CURRENT_LOCATION", ""),
            status="Active",
            message_type="Event",
            model_version=os.getenv("MODEL_VERSION", ""),
            firmware_version=os.getenv("FIRMWARE_VERSION", ""),
            data_payload_type="ADS-B",
            data_payload=json.dumps(data),
        )

        # Publish payload
        success = self.publish_to_topic(self.send_data_topic, payload_json)
        if success:
            logging.info(
                f"Successfully sent data: {data} on topic: {self.send_data_topic}"
            )
        else:
            logging.warning(
                f"Failed to send data: {data} on topic: {self.send_data_topic}"
            )
        return success

    def main(self: Any) -> None:
        """Schedules module heartbeat and enters main loop."""
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="Dump1090 Sender Heartbeat"
        )
        schedule.every(1).seconds.do(self._process_response)
        while True:
            try:
                schedule.run_pending()
                delay = 0.001
                sleep(delay)

            except KeyboardInterrupt as exception:
                logging.debug(exception)
                sys.exit()


if __name__ == "__main__":
    sender = dump1090PubSub(
        mqtt_ip=os.environ.get("MQTT_IP", ""),
        dump1090_host=os.environ.get("DUMP1090_HOST", ""),
        dump1090_http_port=os.environ.get("DUMP1090_HTTP_PORT", ""),
        send_data_topic=os.getenv("DUMP1090_SEND_DATA_TOPIC", ""),
    )
    sender.main()
