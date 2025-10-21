from flask import Flask, request
import requests, json, base64, os

app = Flask(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shopify_demo")
#Added "/records" to the end of the rest endpoint below after getting a Render error and getting help from ChatGPT
KAFKA_REST_ENDPOINT = f"https://{os.getenv('KAFKA_REST_HOST')}/kafka/v3/clusters/{os.getenv('KAFKA_CLUSTER_ID')}/topics/{KAFKA_TOPIC}/records"
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")

@app.route("/shopify", methods=["POST"])
def shopify_webhook():
    #line below was originally "event ="
    data = request.get_json()
    if not data:
        return "No JSON", 400

    #beg test with event_type injected
    event = {"event_type": "checkout_created"}
    if isinstance(data, dict):
        event.update(data)
    else:
        # if Shopify ever sends non-dict JSON, keep it under "raw"
        event["raw"] = data

    #end test

    #original value of payload was: payload = {"records": [{"value": event}]}
    payload = {
        "value": {
            "type": "JSON",
            "data": event
            #json.dumps(event)
        }
    }

    auth = base64.b64encode(f"{KAFKA_API_KEY}:{KAFKA_API_SECRET}".encode()).decode()
    headers = {
        #Change content type from "application/vnd.kafka.json.v2+json"
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth}"
    }

    r = requests.post(KAFKA_REST_ENDPOINT, headers=headers, json=payload)
    #ChatGPT Suggested code to prove where the response is landing
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
