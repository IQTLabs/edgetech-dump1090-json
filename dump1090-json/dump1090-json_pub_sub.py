"""This file contains the Dump1090PubSub class which is a child class
of BaseMQTTPubSub.  The Dump1090PubSub gets data from a specified
dump1090 endpoint and publishes it to the MQTT broker.
"""
import ast
from datetime import datetime
import json
import logging
import os
import sys
from time import sleep, time
import traceback
from typing import Any, Dict, Union

import coloredlogs
import paho.mqtt.client as mqtt
import pandas as pd
import schedule
import requests

from base_mqtt_pub_sub import BaseMQTTPubSub


class Dump1090PubSub(BaseMQTTPubSub):
    """Gets data from a specified dump1090 endpoint and publishes it
    to the MQTT broker.
    """

    def __init__(
        self,
        dump1090_host: str,
        dump1090_http_port: str,
        json_path: str,
        update_time: float,
        ads_b_json_topic: str,
        ground_level: float,
        continue_on_exception: bool = False,
        **kwargs: Any,
    ):
        """
        Connect to the MQTT broker, and log parameters.

        Args:
            dump1090_host (str): Host IP of the dump1090 system
            dump1090_port (str): Host port of the dump1090 socket
            json_path (str): URL path to JSON file
            update_time (float): Duration between dump1090 polls
            ads_b_json_topic (str): MQTT topic to publish the data from
                the port to. Specified via docker-compose.
            ground_level (float): Altitude of the ground level
            continue_on_exception (bool): Continue on unhandled
                exceptions if True, raise exception if False (the default)
        """
        super().__init__(**kwargs)
        self.dump1090_host = dump1090_host
        self.dump1090_http_port = dump1090_http_port
        self.json_path = json_path
        self.update_time = update_time
        self.ads_b_json_topic = ads_b_json_topic
        self.ground_level = ground_level
        self.continue_on_exception = continue_on_exception

        # Connect to the MQTT client
        self.connect_client()
        sleep(1)
        self.publish_registration("Dump1090 Sender Registration")

        # Log configuration parameters
        logging.info(
            f"""Dump1090PubSub initialized with parameters:
    dump1090_host = {dump1090_host}
    dump1090_http_port = {dump1090_http_port}
    json_path = {json_path}
    ground_level = {ground_level}
    ads_b_json_topic = {ads_b_json_topic}
    continue_on_exception = {continue_on_exception}
            """
        )

    def decode_payload(
        self, msg: Union[mqtt.MQTTMessage, str], data_payload_type: str
    ) -> Dict[Any, Any]:
        """
        Decode the payload carried by a message.

        Parameters
        ----------
        payload: mqtt.MQTTMessage
            The MQTT message
        data_payload_type: str
            The data payload type

        Returns
        -------
        data : Dict[Any, Any]
            The data payload of the message payload
        """
        if type(msg) == mqtt.MQTTMessage:
            payload = msg.payload.decode()
        else:
            payload = msg
        data_payload = json.loads(payload)[data_payload_type]
        return json.loads(data_payload)


    def _process_response(self) -> None:
        """Process the response from the Dump1090 aircraft endpoint,
        convert to standard units, and process the resulting data.
        """

        request_start = time()
        # Get and load the response from the endpoint
        url = f"http://{self.dump1090_host}:{self.dump1090_http_port}{self.json_path}"
        request_time = time() - request_start
        response = json.loads(requests.get(url).text)
        
        # Convert to standard units
        data = pd.read_json(json.dumps(response["aircraft"]))
        if "lat" not in data.columns:
            return
        data = data[~pd.isna(data.lat)]
        data = data.fillna(0.0)
        data["request_time"] = request_time
        data["on_ground"] = False.astype('bool')
        data["timestamp"] = float(response["now"]) - data.seen_pos
        if "geom_rate" in data.columns:
            data['geom_rate'] = data['geom_rate'].astype(float) / 60 * 0.3048
        if "baro_rate" in data.columns:
            data['baro_rate'] = data['baro_rate'].astype(float) / 60 * 0.3048
        if "alt_geom" in data.columns:
            data.loc[data['alt_baro'] == 'ground', 'alt_geom'] = self.ground_level / 0.3048
            data['alt_geom'] = data['alt_geom'].astype(float) * 0.3048
        if "alt_baro" in data.columns:
            data.loc[data['alt_baro'] == 'ground', 'on_ground'] = True.astype('bool')
            data.loc[data['alt_baro'] == 'ground', 'alt_baro'] = self.ground_level / 0.3048
            data['alt_baro'] = data['alt_baro'].astype(float) * 0.3048
        if "gs" in data.columns:
            data['gs'] = data['gs'].astype(float) * 0.5144444
        if "squawk" in data.columns:
            data["squawk"] = data["squawk"].astype(str)
        # TODO: Add in barometric offset to get geometric altitude for those aircraft that do not report it
        # tmp = data.loc[data.alt_geom!=0,'hex'][0]
        # baro_offset = data.loc[data.hex==tmp,'alt_geom'] - data.loc[data.hex==tmp,'alt_baro']
        # print(list(baro_offset)[0])
        # data.alt_geom = data.alt_baro+baro_offset
        logging.debug(f"Processed data from response: {data}")
        
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
            # Select the first data row for each aircraft
            vld_data = inp_data.loc[inp_data.hex == aircraft]
            if "alt_geom" not in vld_data.columns:
                continue
            out_data = {}
            out_data["icao_hex"] = vld_data.hex.values[0]
            out_data["timestamp"] = vld_data.timestamp.values[0]
            out_data["request_time"] = vld_data.request_time.values[0]
            out_data["latitude"] = vld_data.lat.values[0]
            out_data["longitude"] = vld_data.lon.values[0]
            out_data["altitude"] = vld_data.alt_geom.values[0]
            out_data["on_ground"] = vld_data.on_ground.values[0]
            out_data["horizontal_velocity"] = vld_data.gs.values[0]
            out_data["track"] = float(vld_data.track.values[0])
            if "geom_rate" in vld_data.columns and "baro_rate" in vld_data.columns and vld_data.geom_rate.values[0] != 0.0:
                out_data["vertical_velocity"] = vld_data.geom_rate.values[0]
            else: 
                if "baro_rate" in vld_data.columns:
                    out_data["vertical_velocity"] = vld_data.baro_rate.values[0]
                else:
                    out_data["vertical_velocity"] = 0
            if "flight" in vld_data.columns:
                out_data["flight"] = vld_data.flight.values[0]
            if "squawk" in vld_data.columns:
                out_data["squawk"] = vld_data.squawk.values[0]
            # out_data["onGround"] = vld_data.hex

            # Send selected data
            self._send_data(out_data)

    def _send_data(self, data: Dict[str, str]) -> bool:
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
        success = self.publish_to_topic(self.ads_b_json_topic, payload_json)
        if success:
            logging.debug(
                f"Successfully sent data: {data} on topic: {self.ads_b_json_topic}"
            )
        else:
            logging.warning(
                f"Failed to send data: {data} on topic: {self.ads_b_json_topic}"
            )
        return success

    def main(self) -> None:
        """Schedules module heartbeat and enters main loop."""
        # Schedule module heartbeat and response processing
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="Dump1090 Sender Heartbeat"
        )
        schedule.every(self.update_time).seconds.do(self._process_response)

        logging.info("System initialized and running")
        while True:
            try:
                schedule.run_pending()
                delay = 0.001
                sleep(delay)

            except KeyboardInterrupt as exception:
                # If keyboard interrupt, fail gracefully
                logging.warning("Received keyboard interrupt: exiting gracefully")
                sys.exit()

            except Exception as exception:
                # Optionally continue on exception
                if self.continue_on_exception:
                    traceback.print_exc()
                else:
                    raise


def make_dump1090() -> Dump1090PubSub:
    """Instantiate Dump1090PubSub."""
    return Dump1090PubSub(
        mqtt_ip=os.environ.get("MQTT_IP", ""),
        dump1090_host=os.environ.get("DUMP1090_HOST", ""),
        dump1090_http_port=os.environ.get("DUMP1090_HTTP_PORT", ""),
        json_path=os.environ.get("JSON_PATH", "/skyaware/data/aircraft.json"),
        update_time=float(os.getenv("DUMP1090_UPDATE_TIME", 1)),
        ads_b_json_topic=os.getenv("ADS_B_JSON_TOPIC", ""),
        ground_level=float(os.getenv("GROUND_LEVEL", os.getenv("ALT", 0))),
        continue_on_exception=ast.literal_eval(
            os.environ.get("CONTINUE_ON_EXCEPTION", "False")
        ),
    )


if __name__ == "__main__":
    # Instantiate Dump1090PubSub and execute
    dump1090 = make_dump1090()
    dump1090.main()
