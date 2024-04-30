BEGIN TRANSACTION;
-- SITE
CREATE TABLE IF NOT EXISTS Site (
    name VARCHAR PRIMARY KEY
);

-- GATEWAY
CREATE TABLE IF NOT EXISTS Gateway (
    gateway_id VARCHAR PRIMARY KEY
);

-- NODE
CREATE TABLE IF NOT EXISTS Node (
    node_deveui VARCHAR PRIMARY KEY,
    node_appeui VARCHAR NOT NULL,
    node_appkey VARCHAR NOT NULL,
    board_id VARCHAR NOT NULL,
    radio_chipset VARCHAR NOT NULL,
    node_site VARCHAR REFERENCES Site(name),
    profile VARCHAR NOT NULL,
    riot_board VARCHAR NOT NULL,
    failed BOOLEAN NOT NULL
);

-- POWER_CONSUMPTION

CREATE TABLE IF NOT EXISTS Power_Consumption (
    power_consumption_id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id VARCHAR REFERENCES Node(node_deveui),
    timestamp DATETIME NOT NULL,
    current REAL NOT NULL,
    voltage REAL NOT NULL,
    power REAL NOT NULL
);

-- RADIO
CREATE TABLE IF NOT EXISTS Radio (
    radio_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME NOT NULL,
    node_id VARCHAR REFERENCES Node(node_deveui),
    channel INTEGER NOT NULL,
    rssi INTEGER NOT NULL
);

-- TRACE
CREATE TABLE IF NOT EXISTS Trace (
    trace_id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id VARCHAR REFERENCES Node(node_deveui),
    timestamp DATETIME NOT NULL,
    message VARCHAR NOT NULL
);


-- MESSAGE
CREATE TABLE IF NOT EXISTS Message (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    related_node VARCHAR REFERENCES Node(node_deveui),
    network_received_at DATETIME NOT NULL
);

-- CONTENT_MESSAGE
CREATE TABLE IF NOT EXISTS Content_Message (
    content_message_id INTEGER PRIMARY KEY REFERENCES Message(message_id),
    frame_counter INTEGER NOT NULL,
    frame_port INTEGER NOT NULL,
    frame_payload VARCHAR NOT NULL
);

-- JOIN_MESSAGE
CREATE TABLE IF NOT EXISTS Join_Message (
    join_message_id INTEGER PRIMARY KEY REFERENCES Message(message_id),
    app_received_at DATETIME NOT NULL
);

-- UPLINK_MESSAGE
CREATE TABLE IF NOT EXISTS Uplink_Message (
    uplink_message_id INTEGER PRIMARY KEY REFERENCES Content_Message(content_message_id),
    through_gateway VARCHAR NOT NULL REFERENCES Gateway(gateway_id),
    gateway_received_at DATETIME,
    app_received_at DATETIME NOT NULL,
    rssi INTEGER NOT NULL,
    snr REAL,
    bandwidth INTEGER NOT NULL,
    frequency INTEGER NOT NULL,
    consumed_airtime_s REAL NOT NULL,
    spreading_factor VARCHAR NOT NULL CHECK (spreading_factor IN ('SF7', 'SF8', 'SF9', 'SF10', 'SF11', 'SF12')),
    coding_rate VARCHAR NOT NULL CHECK (coding_rate IN ('4/5', '4/6', '5/7', '4/8'))
);

-- DOWNLINK_EVENT_MESSAGE

CREATE TABLE IF NOT EXISTS Downlink_Event_Message (
    downlink_event_message_id INTEGER PRIMARY KEY REFERENCES Content_Message(content_message_id),
    confirmed BOOLEAN,
    event_type VARCHAR NOT NULL CHECK (event_type IN ('ack', 'nack', 'queued', 'sent')),
    priority VARCHAR NOT NULL CHECK (priority IN ('LOWEST', 'LOW', 'BELOW_NORMAL', 'NORMAL', 'ABOVE_NORMAL', 'HIGH', 'HIGHEST')),
    correlation_ids VARCHAR
);

-- DOWNLINK_EVENT_ERROR_MESSAGE
CREATE TABLE IF NOT EXISTS Downlink_Event_Error_Message (
    downlink_event_error_message_id INTEGER PRIMARY KEY REFERENCES Message(message_id),
    error_namespace VARCHAR NOT NULL,
    error_id VARCHAR NOT NULL,
    error_message VARCHAR NOT NULL,
    error_code INTEGER NOT NULL
);
COMMIT;
