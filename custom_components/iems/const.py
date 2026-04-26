"""Constants for the iEMS HACS integration."""

DOMAIN = "iems"
VERSION = "0.1.11"

# Config entry keys — stored in the HA config entry, never logged
CONF_API_KEY = "api_key"
CONF_USER_ID = "user_id"
CONF_IOT_ENDPOINT = "iot_endpoint"
CONF_REGION = "region"

# API key — opaque prefixed token issued by the iEMS portal.
# Validation guards only against obvious malformed pastes; trusted
# validation is the server's response to the credential exchange.
#
# Format per Cloud-team spec (cognito_auth_flow.md §3.1):
#   iems_live_<26 chars of Crockford base32>
#   prefix (10) + body (26) = total length 36
# Body charset: [0-9a-z] (lowercase Crockford). 130 bits of entropy.
# Reserves iems_test_* for future test-mode keys (Sprint 3).
API_KEY_PREFIX = "iems_live_"
API_KEY_LENGTH = 36  # prefix(10) + body(26)
API_KEY_REGEX = r"^iems_live_[0-9a-z]{26}$"

# Timing
BATCH_WINDOW_SECONDS = 30
HEARTBEAT_INTERVAL_SECONDS = 60
MAX_QUEUE_DEPTH = 10  # 10 * 30s = 5 minutes of offline buffering
MQTT_CONNECT_TIMEOUT_SECONDS = 10
MQTT_PUBLISH_TIMEOUT_SECONDS = 10
BACKOFF_INITIAL_SECONDS = 1
BACKOFF_MAX_SECONDS = 60

# Credential refresh — refresh N seconds before STS expiry to avoid
# in-flight publish failures. Conservative 5-minute window per
# cognito_auth_flow.md §7 Q5.
CREDENTIAL_REFRESH_LEAD_SECONDS = 300

# iEMS cloud auth endpoint — the ONE hardcoded URL. Everything else
# (iot_endpoint, region, identity_pool_id) comes back in the
# /hacs-auth response, so we can shift regions without a client
# update. Dev stage for Sprint 2; custom-domain cutover is a
# separate Sprint 3 item.
IEMS_AUTH_URL = "https://mnrwhhjnuf.execute-api.eu-central-1.amazonaws.com/hacs-auth"
IEMS_AUTH_HTTP_TIMEOUT_SECONDS = 10

# Rate-limit backoff — per spec §7 Q4: 400/401 are permanent fails
# (no retry); 429 uses 30s→10min exponential; 5xx uses uncapped
# exponential starting at BACKOFF_INITIAL_SECONDS.
RATE_LIMIT_BACKOFF_INITIAL_SECONDS = 30
RATE_LIMIT_BACKOFF_MAX_SECONDS = 600

# Topic templates — per contracts/mqtt_topics.md in the monorepo.
# user_id comes from the auth provider, never hardcoded here.
TELEMETRY_TOPIC_TEMPLATE = "iems/{user_id}/telemetry"
HEARTBEAT_TOPIC_TEMPLATE = "iems/{user_id}/heartbeat"

# Schema — MUST match server-side ingestion validator version
SCHEMA_VERSION = "0.5.0"
MAX_ENTITIES_PER_BATCH = 500
