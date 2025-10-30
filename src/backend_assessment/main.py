import time
import json
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify, make_response

# --- Configuration and Initialization ---
executor = ThreadPoolExecutor(max_workers=5)

# In-Memory Database Simulation
# This dictionary stores all transaction states.
TRANSACTIONS = {}

# Global lock for thread-safe access to the TRANSACTIONS dictionary
transaction_lock = threading.Lock()

# Flask Application Setup
app = Flask(__name__)
# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

def get_utc_timestamp():
    """Returns the current UTC time formatted for the response."""
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

# --- Background Processing Logic ---

def process_transaction(transaction_data):
    """
    Simulates the actual, heavy transaction processing task.
    This function runs in a background thread provided by the executor.
    """
    txn_id = transaction_data['transaction_id']
    
    # Check current status under lock
    with transaction_lock:
        current_status = TRANSACTIONS.get(txn_id, {}).get('status')
        if current_status == "PROCESSED":
            logging.warning(f"Idempotency skip: Transaction {txn_id} already processed.")
            return

    logging.info(f"Starting processing for transaction {txn_id}. (Simulated 30s delay)")
    
    # Simulate heavy external API calls/Database updates
    # Processing must include a 30-second delay
    time.sleep(30)
    
    # Update Status and Timing Information (Under lock for safety)
    with transaction_lock:
        TRANSACTIONS[txn_id]['status'] = "PROCESSED"
        TRANSACTIONS[txn_id]['processed_at'] = get_utc_timestamp()
    
    logging.info(f"Successfully processed and stored transaction {txn_id}.")


# --- API Endpoints ---

@app.route('/', methods=['GET'])
def health_check():
    """
    Health Check Endpoint: GET /
    """
    response = {
        "status": "HEALTHY",
        "current_time": get_utc_timestamp(),
    }
    return jsonify(response), 200

@app.route('/v1/webhooks/transactions', methods=['POST'])
def receive_webhook():
    """
    Webhook Endpoint: POST /v1/webhooks/transactions
    Acknowledge immediately (202 Accepted) and defer processing to the background.
    """
    try:
        data = request.get_json()
        if not data or 'transaction_id' not in data:
            return jsonify({"error": "Invalid payload"}), 400

        txn_id = data['transaction_id']
        created_time = get_utc_timestamp()

        # Prepare Transaction Data for Storage
        transaction_data = {
            "transaction_id": txn_id,
            "source_account": data.get('source_account'),
            "destination_account": data.get('destination_account'),
            "amount": data.get('amount'),
            "currency": data.get('currency'),
            "status": "PROCESSING", # Initial status
            "created_at": created_time,
            "processed_at": None
        }

        # Store the initial 'PROCESSING' state 
        with transaction_lock:
            # Check if transaction is NEW or needs to be re-queued
            if txn_id not in TRANSACTIONS:
                TRANSACTIONS[txn_id] = transaction_data
                logging.info(f"Webhook received for {txn_id}. Status: PROCESSING.")
            else:
                logging.info(f"Duplicate webhook received for {txn_id}. Re-queueing or skipping.")
                # We still allow the task to be submitted to the executor; the
                # idempotency check inside process_transaction will handle the skip.


        # Submit the heavy processing to the background thread pool
        # This is the core of meeting the sub-500ms requirement.
        executor.submit(process_transaction, transaction_data)

        # Immediate Acknowledgment (Requirement 2: 202 Accepted)
        response_body = {"message": f"Webhook received, processing started in background for {txn_id}"}
        return make_response(jsonify(response_body), 202)

    except Exception as e:
        # Graceful error handling for unexpected issues
        logging.error(f"Error processing webhook: {e}", exc_info=True)
        return jsonify({"error": "Internal server error during webhook reception"}), 500

@app.route('/v1/transactions/<string:transaction_id>', methods=['GET'])
def get_transaction_status(transaction_id):
    """
    Query Endpoint: GET /v1/transactions/{transaction_id}
    Retrieves the current status of a transaction.
    """
    with transaction_lock:
        transaction = TRANSACTIONS.get(transaction_id)

    if transaction:
        return jsonify(transaction), 200
    else:
        return jsonify({"error": "Transaction not found"}), 404

# --- Run the Application ---

if __name__ == '__main__':
    # I am adding a dummy transaction for testing the GET endpoint immediately
    with transaction_lock:
        TRANSACTIONS['txn_test_ready'] = {
            "transaction_id": "txn_test_ready",
            "source_account": "acc_user_100",
            "destination_account": "acc_merchant_100",
            "amount": 99.99,
            "currency": "USD",
            "status": "PROCESSED",
            "created_at": get_utc_timestamp(),
            "processed_at": get_utc_timestamp(),
        }

    logging.info("Starting Webhook Service on http://127.0.0.1:5000")
    # Using threaded=True for development/simulation, though the executor handles background work
    app.run(debug=True, threaded=True)
