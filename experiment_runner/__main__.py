#!/usr/bin/python3
import sys
from pathlib import Path
import subprocess
import json
import os
import asyncio
from shutil import which, copy
import argparse
from datetime import datetime
from importlib import resources as impressources
from typing import List, Literal
import logging
import functools
import regex
import sqlite3
import aiomqtt

from experiment_runner import configuration

from . import resources


logging.basicConfig(level=logging.DEBUG)
# import aiodebug.log_slow_callbacks

# aiodebug.log_slow_callbacks.enable(2)

# import aiodebug.hang_inspection

# dumper = aiodebug.hang_inspection.start('./debug', interval = 5)  # 0.25 is the default

resources_path = impressources.files(resources)

logging.basicConfig(level=logging.DEBUG)
CREATE_SQL = (resources_path / "experiment.db.sql").read_text()
REMOTE_BASH_SCRIPT_PATH = resources_path / "remote_bash.sh"

parser = argparse.ArgumentParser(description="Experiment test runner")
parser.add_argument(
    "--attach",
    nargs="?",
    const=0,
    default=None,
    help="attach to an already running experiment",
)
parser.add_argument("--dont-make", action="store_true", help="dont make binaries")
parser.add_argument("--dont-upload", action="store_true", help="dont upload binaries")
parser.add_argument("--dont-run", action="store_true", help="don't run the experiment")
parser.add_argument(
    "-c",
    "--config",
    type=Path,
    default=Path.cwd() / "experiment.json",
    help="Path to configuration file. Default: experiment.json",
)
parser.add_argument(
    "-s",
    "--secrets",
    type=Path,
    default=Path.cwd() / "secrets.json",
    help="path to json file that contains secrets like the mqtt api key, and the appkey for devices",
)
parser.add_argument(
    "experiment_folder",
    type=Path,
    help="folder containing to contain binaries, data and database files",
)
parser.add_argument("-d", "--debug", action="store_true")
cli_args = parser.parse_args()



def subprocess_run(*args, **kwargs):
    if cli_args.debug:
        logging.debug("running subprocess_run: %s", " ".join(*args))
    return subprocess.run(*args, **kwargs)


async def asyncio_create_subprocess_exec(*args, **kwargs):
    if cli_args.debug:
        logging.debug("running asyncio_create_subprocess_exec: %s", " ".join((str(arg) for arg in args)))
    return await asyncio.create_subprocess_exec(*args, **kwargs)


EXPERIMENT_ID = (
    cli_args.attach
)  # None if we shouldnt attach, 0 if we should, but don't know the id, and anything else if we do know the id

EXPERIMENT_FOLDER: Path = cli_args.experiment_folder
DATA_FOLDER = EXPERIMENT_FOLDER / "data"
EXPERIMENT_CONFIG_PATH = cli_args.config
CONFIG = configuration.configuration_from_json(EXPERIMENT_CONFIG_PATH.read_text())
SECRETS = json.loads(cli_args.secrets.read_text())
assert all(node.site for node in CONFIG.nodes)  # make sure all nodes have a site
SITES: set[str] = {node.site for node in CONFIG.nodes}  # type: ignore[misc] # we know after this that all nodes have a site
USER = CONFIG.user
NODES_BY_SITE = {
    site: list(filter(lambda n: n.site == site, CONFIG.nodes)) for site in SITES
}


# RIOT info
SRC_PATH = CONFIG.src_path


# make firmwares


def make_and_assign_firmware(node: configuration.Node):
    logging.info("make firmware for device: %s", node.deveui)
    env = os.environ.copy()
    env["EXECUTION_EPOCH_S"] = str(CONFIG.execution_epoch_s)
    env["BOARD"] = node.riot_board
    env["DEVEUI"] = node.deveui
    env["APPEUI"] = node.appeui
    env["APPKEY"] = SECRETS["LORAWAN"]["APPKEY"]
    if node.sensors is not None:
        env["SENSOR_NAMES"] = " ".join([sensor.name for sensor in node.sensors])
        env["SENSOR_TYPES"] = " ".join([sensor.type for sensor in node.sensors])

    clear_flash_src_path = SRC_PATH / ".."
    if node.riot_board == "b-l072z-lrwan1":
        logging.info(f"board is b-l072z-lrwan1, making eeprom clear...")
        clear_flash_src_path /= "clear_eeprom"
    elif node.riot_board == "samr34-xpro":
        logging.info(f"board is samr34-xpro, making flash clear...")
        clear_flash_src_path /= "clear_flash"
    elif node.riot_board == "esp32-ttgo-t-beam":
        clear_flash_src_path = None

    if clear_flash_src_path is not None:
        subprocess_run(["make", "all"], cwd=clear_flash_src_path, env=env, check=True)
        p = subprocess_run(
            ["make", "info-build-json"],
            cwd=clear_flash_src_path,
            env=env,
            capture_output=True,
            check=True,
        )
        build_info = json.loads(p.stdout)
        flash_file = Path(build_info["FLASHFILE"])
        clear_flash_bin_path = (
            EXPERIMENT_FOLDER / f"{node.deveui}_clear_config{flash_file.suffix}"
        )
        copy(flash_file, clear_flash_bin_path)
        node.clear_flash_path = clear_flash_bin_path
        p = subprocess_run(["make", "all"], cwd=SRC_PATH, env=env, check=True)
        ## find flash file
        p = subprocess_run(
            ["make", "info-build-json"],
            cwd=SRC_PATH,
            env=env,
            capture_output=True,
            check=True,
        )
        build_info = json.loads(p.stdout)
        flash_file = Path(build_info["FLASHFILE"])
    firmware_path = EXPERIMENT_FOLDER / f"{node.deveui}_terra{flash_file.suffix}"
    copy(flash_file, firmware_path)
    node.terra_path = firmware_path


def make_and_assign_all_firmware(nodes: List[configuration.Node]):
    for node in nodes:
        make_and_assign_firmware(node)


def find_and_assign_all_firmware(nodes: List[configuration.Node]):
    for firmware_path in EXPERIMENT_FOLDER.glob("*.elf"):
        for node in nodes:
            if firmware_path.stem.lower() == f"{node.deveui.lower()}_terra":
                node.terra_path = firmware_path
            elif firmware_path.stem.lower() == f"{node.deveui.lower()}_clear_flash":
                node.clear_flash_path = firmware_path


def has_prerequisites(prerequisites=("iotlab", "parallel", "ssh", "scp")):
    for prerequisite in prerequisites:
        if not which(prerequisite):
            logging.error(
                "%s not installed. Please install %s", prerequisite, prerequisite
            )
            return False
    return True


## check if passwordless ssh is set up
async def has_passwordless_ssh_access(user: str, site: str):
    p = await asyncio_create_subprocess_exec(
        "ssh", "-oNumberOfPasswordPrompts=0", f"{user}@{site}", "hostname"
    )
    return await p.wait() == 0


async def register_experiment(nodes: List[configuration.Node]) -> int:
    logging.info("Registering experiment... ")
    node_strings = []
    for node in CONFIG.nodes:
        node_strings.extend(
            [
                "-l",
                f"1,archi={node.archi}+site={node.site},,{node.profile}",
            ]
        )

    p = await asyncio_create_subprocess_exec(
        "iotlab",
        "experiment",
        "submit",
        "--site-association",
        f"{','.join(SITES)},script={str(REMOTE_BASH_SCRIPT_PATH)}",
        "-d",
        str(CONFIG.duration),
        *node_strings,
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, _ = await p.communicate()
    exp_id = int(json.loads(stdout.decode("utf-8")).get("id"))
    if exp_id != None:
        logging.info("successfully registered experiment! Got id %s", exp_id)
        return exp_id
    else:
        logging.error("Failed to create experiment. Error: %s", stdout)
        exit()


async def upload_firmware(
    node: configuration.Node, firmware: Literal["terra", "clear_flash"]
):
    global EXPERIMENT_ID
    if node.failed:
        return
    # check node contains info we need
    if node.terra_path is None:
        logging.error("Node %s has no firmware", node.deveui)
        return
    if node.node_string_by_id is None:
        logging.error("Node %s has no node string", node.deveui)
        return

    p = await asyncio_create_subprocess_exec(
        "iotlab",
        "node",
        "-i",
        str(EXPERIMENT_ID),
        "-l",
        node.node_string_by_id,
        "-fl",
        str(
            node.terra_path.absolute()
            if firmware == "terra"
            else node.clear_flash_path.absolute()
        ),
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, _ = await p.communicate()

    ## check to see if we succeeded
    result = json.loads(stdout.decode("utf-8"))
    if "0" not in result:
        logging.error("no result for node %s with string %s. Marking as failed", node.deveui, node.node_string_by_id)
        node.failed = True
    elif result["0"][0] != node.network_address:
        logging.error("Upload failed for node %s", node.deveui)

    # # stop node after upload, but only if terra
    # if firmware == "terra" and not node.failed:
    #     p = await asyncio_create_subprocess_exec(
    #         "iotlab",
    #         "node",
    #         "--stop",
    #         "-i",
    #         str(EXPERIMENT_ID),
    #         "-l",
    #         node.node_string_by_id,
    #         stdout=asyncio.subprocess.PIPE,
    #     )
    #     stdout, _ = await p.communicate()
    #     if not json.loads(stdout.decode("utf-8"))["0"][0] == node.network_address:
    #         logging.error("Stop failed for node %s", node.deveui)
    #         sys.exit()


async def serial_aggregation_coroutine(
    site_url: str, db_con: sqlite3.Connection
):
    global EXPERIMENT_ID
    nodes_by_id = {node.node_id: node for node in CONFIG.nodes}
    p = None  # initialize p to None so we in finally can check and terminate if it has been set
    try:
        logging.info("Starting serial aggregation collection")
        p = await asyncio_create_subprocess_exec(
            "ssh",
            f"{USER}@{site_url}",
            "serial_aggregator",
            "-i",
            str(EXPERIMENT_ID),
            stdout=asyncio.subprocess.PIPE,
        )
        while True:
            if p.stdout is None:
                await asyncio.sleep(1)
                continue
            line = await p.stdout.readline()
            record = line.decode("utf-8")
            record = record.split(";")
            if len(record) <= 2:  # probably something like <time>;Aggregator started
                continue

            time_unix_s = float(record[0])
            node = nodes_by_id[record[1]]
            msg = record[2]
            db_con.execute(
                "INSERT INTO Trace (node_id, timestamp, message) VALUES (?,?,?)",
                (node.deveui, datetime.fromtimestamp(time_unix_s), msg),
            )
    except asyncio.CancelledError:
        logging.info("Stopping serial aggregation collection")
        if p:
            p.terminate()
        raise


async def radio_coroutine(
    site_url: str, oml_files: List[str], db_con: sqlite3.Connection
):
    global EXPERIMENT_ID
    p = None  # initialize p to None so we in the except block can check and terminate if it has been set
    nodes_by_oml_name = {node.oml_name: node for node in CONFIG.nodes}
    try:
        await asyncio.sleep(60)
        logging.info("starting radio collection")
        ## use GNU Parallel to run multiple processes through a single ssh connection and collect the results in 1 stdout stream
        p = await asyncio_create_subprocess_exec(
            "parallel",
            "--jobs",
            "0",
            "--line-buffer",
            "--tag",
            "--controlmaster",
            "-S",
            f"{USER}@{site_url}",
            "--workdir",
            f"/senslab/users/berthels/.iot-lab/{EXPERIMENT_ID}/radio",
            "tail -F",
            ":::",
            *oml_files,
            stdout=asyncio.subprocess.PIPE,
        )
        ## matches strings from the .oml files prepended with the name of the file.
        ## The regex is made to match lines of (newlines is whitespace):
        # "
        # <node name :str>
        # <run time in second since experiment start :float>
        # <oml schema? :int>
        # <some counter :int>
        # <seconds part of timestamp :int>
        # <microsecond part of timestamp :int>
        # <power measurement :float>
        # <voltage measurement :float>
        # <current measurement :float>
        matcher = regex.compile(
            r"^(?P<node_name>\S+)\s+(?P<exp_runtime>\d+(\.\d*)?)\s+(?P<schema>\d+)\s+(?P<cnmc>\d+)\s+(?P<timestamp_s>\d+)\s+(?P<timestamp_us>\d+)\s+(?P<channel>\d+)\s+(?P<rssi>\-?\d+)$"
        )

        while True:
            line = await p.stdout.readline()
            record = matcher.match(line.decode("utf-8"))
            if record is None:
                continue
            timestamp = int(
                int(record["timestamp_s"]) * 1e6 + int(record["timestamp_us"])
            )
            db_con.execute(
                "INSERT INTO Radio (node_id, timestamp, channel, rssi) VALUES (?, ?, ?, ?)",
                (
                    nodes_by_oml_name[record["node_name"]].deveui,
                    datetime.fromtimestamp(timestamp / 1e6),
                    int(record["channel"]),
                    int(record["rssi"]),
                ),
            )
    except asyncio.CancelledError:
        # stop collection when task is cancelled
        logging.info("Stopping radio collection")
        if p:
            p.terminate()
        raise


async def mqtt_collect_coroutine(db_con: sqlite3.Connection):
    global EXPERIMENT_ID

    def from_str_to_datetime(s):
        ## annoyingly fromisoformat does not support arbitrary iso 8601 formatted strings
        ## so we have to manually strip some information and convert Z into +00:00
        ## this is fixed in python 3.11 but we use 3.10

        return datetime.fromisoformat(s[:26] + "+00:00")

    logging.info("starting mqtt collection")
    try:
        async with aiomqtt.Client(
            hostname=CONFIG.mqtt.address,
            port=CONFIG.mqtt.port,
            username=CONFIG.mqtt.username,
            password=SECRETS["MQTT"]["PASSWORD"],
            clean_session=False,
            client_id=str(EXPERIMENT_ID),
        ) as client:
            async with client.messages() as messages:
                await client.subscribe(CONFIG.mqtt.topic, qos=2)
                logging.info("subscribed to topic %s", CONFIG.mqtt.topic)

                async for msg in messages:
                    # print(msg.topic, msg.payload.decode("utf-8"))
                    # region topics:
                    # v3/{application id}@{tenant id}/devices/{device id}/join
                    # v3/{application id}@{tenant id}/devices/{device id}/up
                    # v3/{application id}@{tenant id}/devices/{device id}/down/queued
                    # v3/{application id}@{tenant id}/devices/{device id}/down/sent
                    # v3/{application id}@{tenant id}/devices/{device id}/down/ack
                    # v3/{application id}@{tenant id}/devices/{device id}/down/nack
                    # v3/{application id}@{tenant id}/devices/{device id}/down/failed
                    # v3/{application id}@{tenant id}/devices/{device id}/service/data
                    # v3/{application id}@{tenant id}/devices/{device id}/location/solved
                    # endregion
                    parsed_msg = json.loads(msg.payload.decode("utf-8"))
                    msg_topic = msg.topic.value
                    ## skip if simulated
                    if parsed_msg.get("simulated"):
                        continue
                    try:
                        # formats taken from https://www.thethingsindustries.com/docs/the-things-stack/concepts/data-formats/
                        ## insert the message into the database. we wrap with a transaction
                        match msg_topic.split("/")[4:]:
                            case ["join"]:
                                # region example
                                # {
                                #     "end_device_ids" : {
                                #         "device_id" : "dev1",                      // Device ID
                                #         "application_ids" : {
                                #         "application_id" : "app1"                  // Application ID
                                #         },
                                #         "dev_eui" : "0004A30B001C0530",            // DevEUI of the end device
                                #         "join_eui" : "800000000000000C",           // JoinEUI of the end device (also known as AppEUI in LoRaWAN versions below 1.1)
                                #         "dev_addr" : "00BCB929"                    // Device address known by the Network Server
                                #     },
                                #     "correlation_ids" : [ "as:up:01..." ],         // Correlation identifiers of the message
                                #     "received_at" : "2020-02-12T15:15..."          // ISO 8601 UTC timestamp at which the message has been received by the Application Server
                                #     "join_accept" : {
                                #         "session_key_id" : "AXBSH1Pk6Z0G166...",   // Join Server issued identifier for the session keys
                                #         "received_at" : "2020-02-17T07:49..."      // ISO 8601 UTC timestamp at which the uplink has been received by the Network Server
                                #     }
                                # }
                                # endregion

                                ## extract relevant info from mqtt payload
                                dev_eui = parsed_msg["end_device_ids"]["dev_eui"]
                                app_received_at = from_str_to_datetime(
                                    parsed_msg["received_at"]
                                )
                                network_received_at = from_str_to_datetime(
                                    parsed_msg["join_accept"]["received_at"]
                                )

                                res = db_con.execute(
                                    "INSERT INTO Message (related_node, network_received_at) VALUES (?,?) RETURNING message_id",
                                    (dev_eui, network_received_at),
                                ).fetchone()
                                message_id = res[0]
                                db_con.execute(
                                    "INSERT INTO Join_Message (join_message_id, app_received_at) VALUES (?,?)",
                                    (message_id, app_received_at),
                                )
                            case ["up"]:
                                # region example
                                # {
                                # "end_device_ids" : {
                                #     "device_id" : "dev1",                    // Device ID
                                #     "application_ids" : {
                                #     "application_id" : "app1"              // Application ID
                                #     },
                                #     "dev_eui" : "0004A30B001C0530",          // DevEUI of the end device
                                #     "join_eui" : "800000000000000C",         // JoinEUI of the end device (also known as AppEUI in LoRaWAN versions below 1.1)
                                #     "dev_addr" : "00BCB929"                  // Device address known by the Network Server
                                # },
                                # "correlation_ids" : [ "as:up:01...", ... ],// Correlation identifiers of the message
                                # "received_at" : "2020-02-12T15:15..."      // ISO 8601 UTC timestamp at which the message has been received by the Application Server
                                # "uplink_message" : {
                                #     "session_key_id": "AXA50...",            // Join Server issued identifier for the session keys used by this uplink
                                #     "f_cnt": 1,                              // Frame counter
                                #     "f_port": 1,                             // Frame port
                                #     "frm_payload": "gkHe",                   // Frame payload (Base64)
                                #     "decoded_payload" : {                    // Decoded payload object, decoded by the device payload formatter
                                #     "temperature": 1.0,
                                #     "luminosity": 0.64
                                #     },
                                #     "rx_metadata": [{                        // A list of metadata for each antenna of each gateway that received this message
                                #     "gateway_ids": {
                                #         "gateway_id": "gtw1",                // Gateway ID
                                #         "eui": "9C5C8E00001A05C4"            // Gateway EUI
                                #     },
                                #     "time": "2020-02-12T15:15:45.787Z",    // ISO 8601 UTC timestamp at which the uplink has been received by the gateway
                                #     "timestamp": 2463457000,               // Timestamp of the gateway concentrator when the message has been received
                                #     "rssi": -35,                           // Received signal strength indicator (dBm)
                                #     "channel_rssi": -35,                   // Received signal strength indicator of the channel (dBm)
                                #     "snr": 5.2,                            // Signal-to-noise ratio (dB)
                                #     "uplink_token": "ChIKEA...",           // Uplink token injected by gateway, Gateway Server or fNS
                                #     "channel_index": 2                     // Index of the gateway channel that received the message
                                #     "location": {                          // Gateway location metadata (only for gateways with location set to public)
                                #         "latitude": 37.97155556731436,       // Location latitude
                                #         "longitude": 23.72678801175413,      // Location longitude
                                #         "altitude": 2,                       // Location altitude
                                #         "source": "SOURCE_REGISTRY"          // Location source. SOURCE_REGISTRY is the location from the Identity Server.
                                #     }
                                #     }],
                                #     "settings": {                            // Settings for the transmission
                                #     "data_rate": {                         // Data rate settings
                                #         "lora": {                            // LoRa modulation settings
                                #         "bandwidth": 125000,               // Bandwidth (Hz)
                                #         "spreading_factor": 7              // Spreading factor
                                #         }
                                #     },
                                #     "coding_rate": "4/6",                  // LoRa coding rate
                                #     "frequency": "868300000",              // Frequency (Hz)
                                #     },
                                #     "received_at": "2020-02-12T15:15..."     // ISO 8601 UTC timestamp at which the uplink has been received by the Network Server
                                #     "consumed_airtime": "0.056576s",         // Time-on-air, calculated by the Network Server using payload size and transmission settings
                                #     "locations": {                           // End device location metadata
                                #     "user": {
                                #         "latitude": 37.97155556731436,       // Location latitude
                                #         "longitude": 23.72678801175413,      // Location longitude
                                #         "altitude": 10,                      // Location altitude
                                #         "source": "SOURCE_REGISTRY"          // Location source. SOURCE_REGISTRY is the location from the Identity Server.
                                #     }
                                #     },
                                #     "version_ids": {                          // End device version information
                                #         "brand_id": "the-things-products",    // Device brand
                                #         "model_id": "the-things-uno",         // Device model
                                #         "hardware_version": "1.0",            // Device hardware version
                                #         "firmware_version": "quickstart",     // Device firmware version
                                #         "band_id": "EU_863_870"               // Supported band ID
                                #     },
                                #     "network_ids": {                          // Network information
                                #     "net_id": "000013",                     // Network ID
                                #     "tenant_id": "tenant1",                 // Tenant ID
                                #     "cluster_id": "eu1"                     // Cluster ID
                                #     }
                                # },
                                # "simulated": true,                         // Signals if the message is coming from the Network Server or is simulated.
                                # }
                                # endregion
                                ## extract needed info from mqtt payload
                                dev_eui = parsed_msg["end_device_ids"]["dev_eui"]
                                app_recieved_at = from_str_to_datetime(
                                    parsed_msg["received_at"]
                                )
                                gateway_recieved_at = from_str_to_datetime(
                                    parsed_msg["uplink_message"]["rx_metadata"][0][
                                        "time"
                                    ]
                                )
                                network_received_at = from_str_to_datetime(
                                    parsed_msg["uplink_message"]["rx_metadata"][0][
                                        "received_at"
                                    ]
                                )
                                frame_counter = (
                                    parsed_msg["uplink_message"]["f_cnt"]
                                    if parsed_msg["uplink_message"].get("f_cnt")
                                    else 0
                                )
                                frame_port = parsed_msg["uplink_message"]["f_port"]
                                frame_payload = parsed_msg["uplink_message"][
                                    "frm_payload"
                                ]
                                rx_metadata = parsed_msg["uplink_message"][
                                    "rx_metadata"
                                ][0]
                                gateway_deveui = rx_metadata["gateway_ids"][
                                    "gateway_id"
                                ]
                                rssi = rx_metadata["rssi"]
                                snr = rx_metadata.get("snr")

                                bandwidth = parsed_msg["uplink_message"]["settings"][
                                    "data_rate"
                                ]["lora"]["bandwidth"]
                                spreading_factor = f'SF{parsed_msg["uplink_message"]["settings"]["data_rate"]["lora"]["spreading_factor"]}'
                                frequency = parsed_msg["uplink_message"]["settings"][
                                    "frequency"
                                ]
                                coding_rate = parsed_msg["uplink_message"]["settings"][
                                    "data_rate"
                                ]["lora"]["coding_rate"]
                                consumed_airtime_s = parsed_msg["uplink_message"][
                                    "consumed_airtime"
                                ][
                                    :-1
                                ]  # remove s from number

                                # add to db
                                ## check if gateway exists and if not, create it
                                res = db_con.execute(
                                    "SELECT * FROM Gateway WHERE gateway_id = ?",
                                    (gateway_deveui,),
                                ).fetchone()
                                if res is None:
                                    db_con.execute(
                                        "INSERT INTO Gateway (gateway_id) VALUES (?)",
                                        (gateway_deveui,),
                                    )
                                ## create message
                                res = db_con.execute(
                                    "INSERT INTO Message (related_node, network_received_at) VALUES (?,?) RETURNING message_id",
                                    (dev_eui, network_received_at),
                                ).fetchone()
                                message_id = res[0]
                                ## create content_message
                                res = db_con.execute(
                                    "INSERT INTO Content_Message VALUES (?,?,?,?) RETURNING content_message_id",
                                    (
                                        message_id,
                                        frame_counter,
                                        frame_port,
                                        frame_payload,
                                    ),
                                ).fetchone()
                                content_message_id = res[0]
                                ## create uplink_message
                                db_con.execute(
                                    "INSERT INTO Uplink_Message VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                    (
                                        content_message_id,
                                        gateway_deveui,
                                        gateway_recieved_at,
                                        app_recieved_at,
                                        rssi,
                                        snr,
                                        bandwidth,
                                        frequency,
                                        consumed_airtime_s,
                                        spreading_factor,
                                        coding_rate,
                                    ),
                                )
                            case ["down", "failed"]:
                                logging.debug("downlink failed")
                                # region example
                                # {
                                # "end_device_ids" : {
                                #     "device_id" : "dev1",                    // Device ID
                                #     "application_ids" : {
                                #     "application_id" : "app1"              // Application ID
                                #     }
                                # },
                                # "correlation_ids" : [ "as:downlink:..." ], // Correlation identifiers of the message
                                # "downlink_failed" : {
                                #     "downlink" : {                           // Downlink that which triggered the failure
                                #     "f_port" : 15,                         // Frame port
                                #     "frm_payload" : "YWFhYWFhY...",        // Frame payload (Base64)
                                #     "confirmed" : true,                    // If the downlink expects a confirmation from the device or not
                                #     "priority" : "NORMAL",                 // Priority of the message in the downlink queue
                                #     "correlation_ids" : [ "as:downli..." ] // Correlation identifiers of the message
                                #     },
                                #     "error" : {                              // Error that was encountered while sending the downlink
                                #     "namespace" : "pkg/networkserver",     // Component in which the error occurred
                                #     "name" : "application_downlink_to...", // Error ID
                                #     "message_format" : "application ...",  // Error message
                                #     "correlation_id" : "2e7f786912e94...", // Correlation identifiers of the error
                                #     "code" : 3                             // gRPC error code
                                #     }
                                # }
                                # }
                                # endregion

                                # Extract needed info from mqtt payload
                                error_namespace = parsed_msg["downlink_failed"][
                                    "error"
                                ]["namespace"]
                                error_id = parsed_msg["downlink_failed"]["error"][
                                    "name"
                                ]
                                error_message = parsed_msg["downlink_failed"]["error"][
                                    "message_format"
                                ]
                                error_code = parsed_msg["downlink_failed"]["error"][
                                    "code"
                                ]
                                ## find the related Downlink event message
                                ## loop through all error correlation id's and find one that matches
                                ## we assume there is only one match so we break when found
                                error_correlation_ids = parsed_msg["correlation_ids"]
                                logging.error(f"parsed_msg{parsed_msg}")
                                downlink_event_message_id = None
                                for corr_id in error_correlation_ids:
                                    logging.error(f"corr_id: {corr_id}")
                                    result =db_con.execute(
                                        """
                                        SELECT DISTINCT Downlink_Event_Message.downlink_event_message_id 
                                        FROM Downlink_Event_Message, json_each(Downlink_Event_Message.correlation_ids) 
                                        WHERE json_each.value = ?
                                        """,
                                        [corr_id],
                                    ).fetchone()
                                    if result is not None and len(result) > 0:
                                        logging.debug("found downlink event message id: %s", result[0])
                                        downlink_event_message_id = result[0]
                                        

                                    ## create the downlink_event_error message if we found the id
                                        db_con.execute(
                                            "INSERT INTO Downlink_Event_Error_Message VALUES (?,?,?,?,?)",
                                            (
                                                downlink_event_message_id,
                                                error_namespace,
                                                error_id,
                                                error_message,
                                                error_code,
                                            )
                                        )
                                        break
                                if downlink_event_message_id is None:
                                    logging.error("downlink failed but no downlink event message found")
                                
                            case ["down", down_type]:
                                # region example
                                # {
                                # "end_device_ids" : {
                                #     "device_id" : "dev1",                    // Device ID
                                #     "application_ids" : {
                                #     "application_id" : "app1"              // Application ID
                                #     },
                                #     "dev_eui" : "0004A30B001C0530",          // DevEUI of the end device
                                #     "join_eui" : "800000000000000C",         // JoinEUI of the end device (also known as AppEUI in LoRaWAN versions below 1.1)
                                #     "dev_addr" : "00BCB929"                  // Device address known by the Network Server
                                # },
                                # "correlation_ids" : [ "as:downlink:..." ], // Correlation identifiers of the message
                                # "received_at" : "2020-02-17T10:32:24...",  // ISO 8601 UTC timestamp at which the message has been received by the Network Server
                                # "downlink_queued" : {                      // Name of the event (ack, nack, queued or sent)
                                #     "session_key_id" : "AXBSH1Pk6Z0G166...", // Join Server issued identifier for the session keys
                                #     "f_port" : 15,                           // Frame port
                                #     "f_cnt" : 1,                             // Frame counter
                                #     "frm_payload" : "vu8=",                  // Frame payload (Base64)
                                #     "confirmed" : true,                      // If the downlink expects a confirmation from the device or not
                                #     "priority" : "NORMAL",                   // Priority of the message in the downlink queue
                                #     "correlation_ids" : [ "as:downlink..." ] // Correlation identifiers of the message
                                # }
                                # }

                                # endregion

                                ## example is wrong? at least api seems to not provide dev_eui, join_eui, dev_addr and received_at.
                                ## we will work around it so far, but this is far from ideal
                                ## TTN is notified and a fix is coming in next release

                                ## get needed info from mqtt payload
                                dev_eui = (
                                    parsed_msg["end_device_ids"]["device_id"]
                                    .split("-")[1]
                                    .upper()
                                )
                                network_received_at = datetime.now()
                                downlink_key = f"downlink_{down_type}"
                                frame_port: int = parsed_msg[downlink_key]["f_port"]
                                frame_counter: int = (
                                    -1
                                )  ## TODO: fix. right now f_cnt is missing from the api response
                                # frame_counter: int = parsed_msg[downlink_key]["f_cnt"]
                                frame_payload: str = parsed_msg[downlink_key][
                                    "frm_payload"
                                ]
                                confirmed: bool = parsed_msg[downlink_key]["confirmed"]
                                priority: str = (
                                    "NORMAL"  # TODO: as soon as priority is provided by API fix this
                                )
                                # priority: str = parsed_msg[downlink_key]["priority"]
                                correlation_ids: list[str] = parsed_msg[
                                    "correlation_ids"
                                ]

                                # add to db
                                ## add message
                                result = db_con.execute(
                                    "INSERT INTO Message (related_node, network_received_at) VALUES (?,?) RETURNING message_id",
                                    (dev_eui, network_received_at),
                                ).fetchone()
                                message_id = result[0]

                                ## add content message
                                result = db_con.execute(
                                    "INSERT INTO Content_Message VALUES (?,?,?,?) RETURNING content_message_id",
                                    (
                                        message_id,
                                        frame_counter,
                                        frame_port,
                                        frame_payload,
                                    ),
                                ).fetchone()
                                content_message_id = result[0]

                                ## add downlink event message
                                db_con.execute(
                                    "INSERT INTO Downlink_Event_Message VALUES (?,?,?,?,?)",
                                    (
                                        content_message_id,
                                        confirmed,
                                        down_type,
                                        priority,
                                        json.dumps(correlation_ids),
                                    ),
                                )
                            case ["service", "data"]:
                                pass
                            case ["location", "solved"]:
                                pass
                    except Exception as e:
                        logging.error(e)
                        logging.error(parsed_msg)
                        raise

                logging.error("This should never be reached")
    except asyncio.CancelledError:
        logging.info("Stopping mqtt_submit_coroutine")
        raise


async def mqtt_clear_down_queue_coroutine():
    global EXPERIMENT_ID
    try:
        async with aiomqtt.Client(
            hostname=CONFIG.mqtt.address,
            port=CONFIG.mqtt.port,
            username=CONFIG.mqtt.username,
            password=SECRETS["MQTT"]["PASSWORD"],
            clean_session=False,
            client_id=str(EXPERIMENT_ID),
        ) as client:
            logging.info("clearing downlink queue")
            for node in CONFIG.nodes:
                topic = CONFIG.mqtt.topic[:-1] + node.ttn_device_id + "/down/replace"
                payload = {"downlinks": []}
                await client.publish(topic, json.dumps(payload))
    except asyncio.CancelledError:
        logging.info("canceling mqtt_clear_down_queue_coroutine")
        raise
    except Exception as e:
        logging.error(e)
        raise


async def mqtt_submit_query_coroutine():
    global EXPERIMENT_ID
    try:
        async with aiomqtt.Client(
            hostname=CONFIG.mqtt.address,
            port=CONFIG.mqtt.port,
            username=CONFIG.mqtt.username,
            password=SECRETS["MQTT"]["PASSWORD"],
            clean_session=False,
            client_id=str(EXPERIMENT_ID),
        ) as client:
            await asyncio.sleep(10)
            logging.info("submitting query to mqtt")
            for node in (node for node in CONFIG.nodes if not node.failed):
                topic = CONFIG.mqtt.topic[:-1] + node.ttn_device_id + "/down/replace"
                payload = {
                    "downlinks": [
                        {
                            "f_port": 1,
                            "frm_payload": CONFIG.mqtt.payload,
                            "confirmed": CONFIG.mqtt.payload_confirmed,
                            "priority": "NORMAL",
                        }
                    ]
                }
                await client.publish(topic, json.dumps(payload))
    except asyncio.CancelledError:
        logging.info("canceling mqtt_submit_coroutine")
        raise
    except Exception as e:
        logging.error(e)
        raise


async def print_progress(db_con: sqlite3.Connection):
    try:
        while True:
            # print("\n" * 3, end="")
            ## fetch stats from db
            nodes_count = db_con.execute("SELECT COUNT(*) FROM Node").fetchone()
            trace_count = db_con.execute("SELECT COUNT(*) FROM Trace").fetchone()
            power_count = db_con.execute(
                "SELECT node_id, COUNT(*) FROM Power_Consumption GROUP BY node_id"
            ).fetchall()
            rad_count = db_con.execute("SELECT COUNT(*) FROM Radio").fetchone()
            sites_list = db_con.execute("SELECT * from Site").fetchall()
            mqtt_messages_count = db_con.execute(
                "SELECT COUNT(*) FROM Message"
            ).fetchone()
            gateways_count = db_con.execute("SELECT COUNT(*) FROM Gateway").fetchone()
            # print stats
            output_strings = [
                f"Nodes: {nodes_count}",
                f"Sites: {str(sites_list)}",
                f"Traces: {trace_count}",
                f"Power Consumption: {str(power_count)}",
                f"Radio: {rad_count}",
                f"Gateway: {gateways_count}",
                f"Messages: {mqtt_messages_count}",
            ]
            logging.info(str(output_strings))
            await asyncio.sleep(1)
        #    print("\033[F\033[K" * len(output_strings), end="")
    except asyncio.CancelledError:
        logging.info("Stopping print_progress")
        raise


async def commit(db_con: sqlite3.Connection):
    try:
        while True:
            await asyncio.sleep(5)
            db_con.execute("CHECKPOINT")  # for some reason this is needed
    except asyncio.CancelledError:
        logging.info("Stopping commit")
        raise


async def data_collection_tasks(db_con: sqlite3.Connection):
    tasks = []
    for nodelist in NODES_BY_SITE.values():
        site_url = nodelist[0].site_url
        tasks.extend(
            [
                # serial_aggregation_coroutine(site_url),
                # radio_coroutine(site_url, [n.oml_name for n in nodelist]),
                mqtt_clear_down_queue_coroutine(),
                mqtt_collect_coroutine(db_con),
                mqtt_submit_query_coroutine(),
            ]
        )
    # tasks.append(commit())

    return await asyncio.gather(*tasks, return_exceptions=False)


async def download_data_folder(
    site: str, local_path: Path, user=CONFIG.user, remote_path="./.iot-lab/last"
):
    site_url = f"{site}.iot-lab.info"

    local_path_with_site = local_path / site
    # make sure local path exists
    if not local_path_with_site.exists():
        local_path.mkdir(parents=True)
    p = await asyncio_create_subprocess_exec(
        "scp", "-r", f"{user}@{site_url}:{remote_path}", str(local_path_with_site)
    )
    return await p.wait()


async def get_nodes_from_iotlab():
    # NODES is a list of dictionaries with following info
    # {
    #     "archi": "nucleo-wl55jc:stm32wl",
    #     "camera": "0",
    #     "mobile": "0",
    #     "mobility_type": null,
    #     "network_address": "nucleo-wl55jc-1.grenoble.iot-lab.info",
    #     "power_consumption": "1",
    #     "power_control": "1",
    #     "production": "YES",
    #     "radio_sniffing": "1",
    #     "site": "grenoble",
    #     "state": "Alive",
    #     "uid": " ",
    #     "x": "13.36",
    #     "y": "31.97",
    #     "z": "2.58"
    # }
    p = await asyncio_create_subprocess_exec(
        "iotlab",
        "experiment",
        "get",
        "-i",
        str(EXPERIMENT_ID),
        "-n",
        stdout=asyncio.subprocess.PIPE,
    )
    out, _ = await p.communicate()
    out_decoded = out.decode("utf-8")
    return json.loads(out_decoded)["items"]


def populate_sites_table(db_con):
    logging.info("Populating sites table")
    db_con.executemany("INSERT INTO Site (name) VALUES (?)", [list(SITES)])


def create_and_initialise_db(db_path: Path):
    db_con = sqlite3.connect(f"{str(db_path)}")

    db_con.executescript("".join(CREATE_SQL))
    return db_con


def populate_nodes_table(db_con, nodes: List[configuration.Node]):
    for node in nodes:
        db_con.execute(
            "INSERT INTO Node (node_deveui,node_appeui,node_appkey,board_id,radio_chipset,node_site,profile,riot_board,failed) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                node.deveui,
                node.appeui,
                SECRETS["LORAWAN"]["APPKEY"],
                node.iot_lab_board_id,
                node.iot_lab_radio_chipset,
                node.site,
                node.profile,
                node.riot_board,
                node.failed
            ),
        )
    db_con.commit()


async def find_latest_running_experiment():
    p = await asyncio_create_subprocess_exec(
        "iotlab", "experiment", "get", "-e", stdout=asyncio.subprocess.PIPE
    )
    out, _ = await p.communicate()
    out_decoded = out.decode("utf-8")

    running_experiments = json.loads(out_decoded)["Running"]
    logging.info(
        "Found %s running experiments: %s",
        len(running_experiments),
        running_experiments,
    )
    return sorted(running_experiments)[-1]


async def wait_for_experiment_to_start():
    p = await asyncio_create_subprocess_exec(
        "iotlab", "experiment", "wait", "-i", str(EXPERIMENT_ID)
    )
    return await p.wait() == 0


def populate_traces_table_from_file(db_con, nodes, file_path: Path):
    traces = []
    nodes_by_id = {node.node_id: node for node in nodes}
    with file_path.open() as f:
        for line in f:
            record = line.split(";")
            if len(record) <= 2:  # probably something like <time>;Aggregator started
                continue
            traces.append(
                (
                    nodes_by_id[record[1]].deveui,
                    datetime.fromtimestamp(float(record[0])),
                    record[2],
                )
            )
    db_con.executemany(
        "INSERT INTO Trace (node_id, timestamp, message) VALUES (?,?,?)", traces
    )
    db_con.commit()


def populate_power_consumption_table_from_file(
    db_con, nodes: List[configuration.Node], file_path: Path
):
    node = next(filter(lambda node: node.oml_name == file_path.name, nodes))
    if node is None:
        logging.error("Could not find node for %s", file_path.name)
        return

    # # oml is used through some magic in the following db_con.execute call
    # # pylint: disable=unused-variable
    # oml = db_con.read_csv(
    #     file_path,
    #     skiprows=9,
    #     names=[
    #         "exp_runtime",
    #         "schema",
    #         "cnmc",
    #         "timestamp_s",
    #         "timestamp_us",
    #         "power",
    #         "voltage",
    #         "current",
    #     ],
    # )
    # db_con.execute(
    #     f"WITH PC AS (SELECT '{node.deveui}' as node_id, make_timestamp(timestamp_s * 1000000 + timestamp_us) as timestamp, current, power, voltage FROM oml) INSERT INTO Power_Consumption BY NAME SELECT * from PC"
    # )
    matcher = regex.compile(
        r"^(?P<exp_runtime>\d+(\.\d*)?)\s+(?P<schema>\d+)\s+(?P<cnmc>\d+)\s+(?P<timestamp_s>\d+)\s+(?P<timestamp_us>\d+)\s+(?P<power>\d+(\.\d*)?)\s+(?P<voltage>\d+(\.\d*)?)\s+(?P<current>\d+(\.\d*)?)$"
    )

    rows = []
    with file_path.open() as f:
        for line in f:
            record = matcher.match(line)
            if record is None:
                continue
            timestamp = int(
                int(record["timestamp_s"]) * 1e6 + int(record["timestamp_us"])
            )
            row = (
                node.deveui,
                datetime.fromtimestamp(timestamp / 1e6),
                float(record["current"]),
                float(record["voltage"]),
                float(record["power"]),
            )
            rows.append(row)
    db_con.executemany(
        "INSERT INTO Power_Consumption (node_id, timestamp, current, voltage, power) VALUES (?,?,?,?,?)",
        rows,
    )
    db_con.commit()


async def is_experiment_running():
    p = await asyncio_create_subprocess_exec(
        "iotlab",
        "experiment",
        "get",
        "-p",
        "-i",
        str(EXPERIMENT_ID),
        stdout=asyncio.subprocess.PIPE,
    )
    out, _ = await p.communicate()
    output_decoded = out.decode("utf-8")
    state = json.loads(output_decoded)["state"]
    return state == "Running"


def populate_nodes_with_addresses_from_iotlab(iotlab_nodes):
    for n in iotlab_nodes:
        try:
            next(
                filter(
                    lambda nod: nod.archi == n["archi"] and nod.node_id is None,
                    CONFIG.nodes,
                )
            ).network_address = n["network_address"]
        except StopIteration:
            logging.error("Node not found while populating internal node table")
            sys.exit()


# run all data collection tasks and await their completion
async def main():
    # need access to experiment_id to modify it
    global EXPERIMENT_ID

    if not has_prerequisites():
        logging.error("Prerequisites for this script are not installed")
        sys.exit(0)

    if not cli_args.dont_make:
        logging.info("making firmware")
        make_and_assign_all_firmware(CONFIG.nodes)
    else:
        logging.info("skipping making firmware. Finding existing firmware...")
        find_and_assign_all_firmware(CONFIG.nodes)

    if not await has_passwordless_ssh_access(USER, "saclay.iot-lab.info"):
        logging.error(
            "Passwordless SSH access to the ssh front-end is required.\ncant log in to %s@saclay.iot-lab.info without password. Check if ssh-agent is running and if not run 'eval $(ssh-agent);ssh-add' to start the agent and add your key",
            USER,
        )
        sys.exit(0)

    # we upload and run the experiment below, so if we dont want to do that: early exit
    if cli_args.dont_run:
        sys.exit(0)

    if EXPERIMENT_ID is None:
        EXPERIMENT_ID = await register_experiment(CONFIG.nodes)
    elif EXPERIMENT_ID == 0:
        logging.info("Attaching to latest running experiment...")
        EXPERIMENT_ID = await find_latest_running_experiment()

    logging.info("Waiting for experiment %s to start", EXPERIMENT_ID)
    # find experiment
    await wait_for_experiment_to_start()

    db_path = EXPERIMENT_FOLDER / f"{EXPERIMENT_ID}.db"
    logging.info("Create SQLite for experiment data at %s", db_path)
    # Check if file already exists
    if db_path.exists():
        answer = input(
            f"Database already exists at {str(db_path)}. Do you want us to delete it? [y/n] "
        )
        if answer == "y":
            db_path.unlink()
        else:
            logging.error("Database already exists and wasnt overwritten. Exiting")
            sys.exit(0)

    db_con = create_and_initialise_db(db_path)

    populate_sites_table(db_con)

    iotlab_nodes = await get_nodes_from_iotlab()

    # go through nodes returned and populate internal node list with proper addresses
    populate_nodes_with_addresses_from_iotlab(iotlab_nodes)

    

    # # clear flash on all boards
    logging.info("clearing flash on all boards")
    await asyncio.gather(*[upload_firmware(n, "clear_flash") for n in CONFIG.nodes])
    await asyncio.sleep(20)
    if not cli_args.dont_upload:
        logging.info("uploading terra firmware to all boards")
        # for n in CONFIG.nodes:
        #     await upload_firmware(n)

        await asyncio.gather(*[upload_firmware(n, "terra") for n in CONFIG.nodes])

    # saving nodes in experiment db
    populate_nodes_table(db_con, CONFIG.nodes)

    logging.info("starting data collection")

    ## start all boards
    subprocess_run(["iotlab", "node", "--start", "-i", str(EXPERIMENT_ID)], check=True)
    await asyncio.sleep(5)
    collection_task = asyncio.create_task(data_collection_tasks(db_con))
    # progress_task = asyncio.create_task(print_progress(db_con))
    await asyncio.sleep(5)
    while not collection_task.done():
        await asyncio.sleep(10)
        # ensure experiment is running
        running = await is_experiment_running()
        if not running:
            logging.info(
                "experiment status no longer running. Cancelling all tasks after 30 seconds..."
            )
            await asyncio.sleep(30)
            logging.info("Cancelling tasks...")
            collection_task.cancel()
            #       progress_task.cancel()
            await asyncio.sleep(5)
            logging.info("done")
            break

    logging.info("download data folders from each site")
    for site, nodes in NODES_BY_SITE.items():
        logging.info("downloading data from %s", site)
        await download_data_folder(site, DATA_FOLDER / str(EXPERIMENT_ID))

    logging.info("populate traces in db from serial output file from each site")
    for site in SITES:
        file_path = DATA_FOLDER / str(EXPERIMENT_ID) / site / "serial_output"
        populate_traces_table_from_file(db_con, CONFIG.nodes, file_path)

    logging.info(
        "populating power consumption measurements from .oml files from each site"
    )
    for site in SITES:
        oml_files = list(
            Path(DATA_FOLDER / str(EXPERIMENT_ID) / site / "consumption").glob("*.oml")
        )
        for oml_file in oml_files:
            logging.info(
                "populating power consumption measurements from %s from site %s",
                oml_file,
                site,
            )
            populate_power_consumption_table_from_file(db_con, CONFIG.nodes, oml_file)

    logging.info("copying configuration file to data directory")
    copy(EXPERIMENT_CONFIG_PATH, DATA_FOLDER / str(EXPERIMENT_ID))


asyncio.run(main())
