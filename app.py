from flask import Flask, request
import requests, json, base64, os
from decimal import Decimal, InvalidOperation

app = Flask(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shopify_demo")
KAFKA_REST_ENDPOINT = f"https://{os.getenv('KAFKA_REST_HOST')}/kafka/v3/clusters/{os.getenv('KAFKA_CLUSTER_ID')}/topics/{KAFKA_TOPIC}/records"
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")

@app.route("/shopify", methods=["POST"])
def shopify_webhook():
    data = request.get_json()
    if not data:
        return "No JSON", 400

    email_address = data.get("email")
    cart_url = data.get("abandoned_checkout_url")
    total_price = data.get("total_line_items_price")

    try:
        total_price_number = float(Decimal(str(total_price))) if total_price is not None else None
    except (InvalidOperation, TypeError, ValueError):
        total_price_number = None

    new_object = {
        "email": email_address,
        "url": cart_url,
        "price": total_price_number,
        "customer_id": 12345,
        "first_name": "Andreas M.",
        "onesignal_subscription_id": "2f993b6a-5766-453b-af9e-90e76f90b064",
        "campaign_name": "GrowthLoop Push Test"
    }

    event = {"event_type": "push_test"}
    if isinstance(new_object, dict):
        event.update(new_object)
    else:
        event["raw"] = data

    payload = {
        "value": {
            "type": "JSON",
            "data": event
        }
    }

    auth = base64.b64encode(f"{KAFKA_API_KEY}:{KAFKA_API_SECRET}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth}"
    }

    r = requests.post(KAFKA_REST_ENDPOINT, headers=headers, json=payload)
    app.logger.info("Kafka REST status=%s body=%s", r.status_code, r.text)
    if r.status_code >= 300:
        print("Kafka post failed:", r.text)
        return "Kafka error", 500

    return "", 200

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)