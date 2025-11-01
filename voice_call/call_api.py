#!/usr/bin/env python3
"""
API Handler for Voice Call Workflow
Provides endpoints to initiate voice calls to customers
"""

import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from livekit import api
    LIVEKIT_AVAILABLE = True
except ImportError:
    LIVEKIT_AVAILABLE = False
    logger.warning("livekit package not installed. Install with: pip install livekit")

app = Flask(__name__)
CORS(app)

# Configuration
LIVEKIT_URL = os.getenv("LIVEKIT_URL", "wss://phantompay-lhq52otl.livekit.cloud")
LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY", "APIKFtiC8ezm2KK")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET", "3U6L3ZLAqlgymP4fXGjxGjrDRHTabq1SUgYMayYKB7S")
CONVEX_URL = os.getenv("CONVEX_URL", "https://marvelous-emu-964.convex.cloud")

# LiveKit API client - will be initialized lazily
_livekit_api = None

def get_livekit_api():
    """Get or initialize LiveKit API client lazily"""
    global _livekit_api
    
    if _livekit_api is not None:
        return _livekit_api
    
    if not LIVEKIT_AVAILABLE:
        logger.warning("livekit package not installed. Install with: pip install livekit")
        return None
    
    if not LIVEKIT_URL or not LIVEKIT_API_KEY or not LIVEKIT_API_SECRET:
        logger.warning("LiveKit credentials not set. Voice calls will not work.")
        return None
    
    # Skip SDK initialization - always use HTTP API for Flask compatibility
    # The SDK requires async event loop which Flask doesn't provide
    logger.info("Using HTTP API mode for LiveKit (Flask compatibility)")
    _livekit_api = {
        "url": LIVEKIT_URL,
        "api_key": LIVEKIT_API_KEY,
        "api_secret": LIVEKIT_API_SECRET,
        "use_http": True
    }
    return _livekit_api


def get_customer_profile(email=None, customer=None):
    """Get customer profile from Convex"""
    try:
        endpoint = f"{CONVEX_URL.rstrip('/')}/api/query"
        payload = {
            "path": "emailRAG:getCustomerProfile",
            "args": {
                "email": email,
                "customer": customer,
            },
            "format": "json"
        }
        
        response = requests.post(endpoint, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        if isinstance(result, dict) and "value" in result:
            return result["value"]
        return result
    except Exception as e:
        logger.error(f"Error fetching customer profile: {e}")
        return None


def get_customers_needing_calls(days_since_email=3, max_overdue_days=None):
    """Get customers with unpaid invoices from Convex"""
    try:
        endpoint = f"{CONVEX_URL.rstrip('/')}/api/query"
        payload = {
            "path": "voiceCall:getCustomersNeedingCalls",
            "args": {
                "daysSinceEmail": days_since_email,
                "maxOverdueDays": max_overdue_days,
            },
            "format": "json"
        }
        
        response = requests.post(endpoint, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        if isinstance(result, dict) and "value" in result:
            return result["value"]
        return result if isinstance(result, list) else []
    except Exception as e:
        logger.error(f"Error fetching customers needing calls: {e}")
        return []


@app.route("/api/initiate-call", methods=["POST"])
def initiate_call():
    """
    Initiate a voice call to a customer
    Requires: email or customer, phone_number
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        
        email = data.get("email")
        customer = data.get("customer")
        phone_number = data.get("phone_number")
        
        if not email and not customer:
            return jsonify({"success": False, "error": "email or customer is required"}), 400
        
        if not phone_number:
            return jsonify({"success": False, "error": "phone_number is required"}), 400
        
        livekit_api = get_livekit_api()
        if not livekit_api:
            return jsonify({
                "success": False,
                "error": "LiveKit credentials not configured"
            }), 500
        
        # Verify customer has unpaid invoices
        customer_profile = get_customer_profile(email=email, customer=customer)
        
        if not customer_profile:
            return jsonify({
                "success": False,
                "error": "Customer not found or has no invoices"
            }), 404
        
        unpaid_count = customer_profile.get("unpaidInvoices", 0)
        if unpaid_count == 0:
            # Provide helpful message with suggestions
            customer_info = email or customer or "this customer"
            return jsonify({
                "success": False,
                "error": f"Customer {customer_info} has no unpaid invoices",
                "suggestion": "Use /api/customers-to-call to see customers who need calls"
            }), 400
        
        # Create room name from customer identifier
        customer_identifier = email or customer or "customer"
        room_name = f"payment-call-{customer_identifier}".replace("@", "-").replace(" ", "-").replace(".", "-")
        
        # Create room token for the agent using JWT (always use manual generation to avoid async issues)
        agent_token = None
        if LIVEKIT_AVAILABLE:
            try:
                # Always use manual JWT generation to avoid async event loop issues
                import base64
                import hmac
                import hashlib
                import time
                import json as json_lib
                
                header = {"alg": "HS256", "typ": "JWT"}
                payload = {
                    "iss": LIVEKIT_API_KEY,
                    "exp": int(time.time()) + 3600,
                    "sub": "payment-agent",
                    "name": "Payment Reminder Agent",
                    "video": {
                        "room": room_name,
                        "roomJoin": True,
                        "canPublish": True,
                        "canSubscribe": True
                    }
                }
                
                header_b64 = base64.urlsafe_b64encode(json_lib.dumps(header).encode()).decode().rstrip('=')
                payload_b64 = base64.urlsafe_b64encode(json_lib.dumps(payload).encode()).decode().rstrip('=')
                
                message = f"{header_b64}.{payload_b64}"
                signature = hmac.new(
                    LIVEKIT_API_SECRET.encode(),
                    message.encode(),
                    hashlib.sha256
                ).digest()
                sig_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')
                
                agent_token = f"{header_b64}.{payload_b64}.{sig_b64}"
                logger.info("Generated agent token using manual JWT")
            except Exception as e:
                logger.error(f"Failed to create agent token: {e}")
        
        # Create participant identity with customer info
        participant_identity = f"email:{email}" if email else f"customer:{customer}"
        
        # For telephony integration, you would use LiveKit's telephony SDK
        # This is a placeholder - actual telephony requires SIP integration
        # or using LiveKit's telephony service
        
        # Create SIP call (if using telephony)
        # Note: This requires LiveKit telephony setup
        try:
            actual_room_name = room_name
            
            # Check if using HTTP API mode or SDK mode
            if isinstance(livekit_api, dict) and livekit_api.get("use_http"):
                # Use HTTP API directly (better for Flask - no async needed)
                import base64
                import hmac
                import hashlib
                import time
                import json as json_lib
                
                # Generate JWT token for HTTP API
                def generate_token(api_key, api_secret):
                    header = {"alg": "HS256", "typ": "JWT"}
                    payload = {
                        "iss": api_key,
                        "exp": int(time.time()) + 3600,
                        "sub": "api"
                    }
                    
                    header_b64 = base64.urlsafe_b64encode(json_lib.dumps(header).encode()).decode().rstrip('=')
                    payload_b64 = base64.urlsafe_b64encode(json_lib.dumps(payload).encode()).decode().rstrip('=')
                    
                    message = f"{header_b64}.{payload_b64}"
                    signature = hmac.new(
                        api_secret.encode(),
                        message.encode(),
                        hashlib.sha256
                    ).digest()
                    sig_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')
                    
                    return f"{header_b64}.{payload_b64}.{sig_b64}"
                
                token = generate_token(livekit_api["api_key"], livekit_api["api_secret"])
                api_url = livekit_api["url"].replace("wss://", "https://").replace("ws://", "http://")
                
                # Create room via HTTP API
                create_room_response = requests.post(
                    f"{api_url}/twirp/livekit.RoomService/CreateRoom",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "name": room_name,
                        "empty_timeout": 300,
                        "max_participants": 2
                    },
                    timeout=10
                )
                
                if create_room_response.status_code == 200:
                    room_data = create_room_response.json()
                    actual_room_name = room_data.get("name", room_name)
                    logger.info(f"Created room via HTTP API: {actual_room_name}")
                else:
                    logger.warning(f"HTTP API room creation failed: {create_room_response.text}")
                    # Continue with room name anyway - room might be created by agent
            else:
                # Try SDK method (may fail in Flask context)
                try:
                    if hasattr(livekit_api, 'room'):
                        from livekit.protocol import room as proto_room
                        room_request = proto_room.CreateRoomRequest(
                            name=room_name,
                            empty_timeout=300,
                            max_participants=2,
                        )
                        room = livekit_api.room.create_room(room_request)
                        actual_room_name = room.name
                        logger.info(f"Created room via SDK: {actual_room_name}")
                    else:
                        logger.warning("LiveKit API doesn't support room creation, using provided room name")
                except Exception as e:
                    logger.warning(f"SDK room creation failed: {e}, using provided room name")
            
            # Return call information
            return jsonify({
                "success": True,
                "room_name": actual_room_name,
                "room_url": LIVEKIT_URL,
                "agent_token": agent_token,
                "message": "Room created. Connect agent and initiate phone call via telephony service.",
                "customer_profile": {
                    "customer": customer_profile.get("customer"),
                    "email": customer_profile.get("email"),
                    "unpaidInvoices": unpaid_count,
                    "unpaidAmount": customer_profile.get("unpaidAmount"),
                }
            })
            
        except Exception as e:
            logger.error(f"Error creating room: {e}")
            return jsonify({
                "success": False,
                "error": f"Failed to create call room: {str(e)}"
            }), 500
            
    except Exception as e:
        logger.error(f"Error initiating call: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/call-status", methods=["GET"])
def call_status():
    """Get status of an ongoing call"""
    try:
        room_name = request.args.get("room_name")
        
        if not room_name:
            return jsonify({"success": False, "error": "room_name is required"}), 400
        
        livekit_api = get_livekit_api()
        if not livekit_api:
            return jsonify({
                "success": False,
                "error": "LiveKit credentials not configured"
            }), 500
        
        # Get room information
        from livekit.protocol import room as proto_room
        list_request = proto_room.ListRoomsRequest()
        list_request.names = [room_name]
        room_list = livekit_api.room.list_rooms(list_request)
        
        if not room_list.rooms or len(room_list.rooms) == 0:
            return jsonify({
                "success": False,
                "error": "Room not found"
            }), 404
        
        room_info = room_list.rooms[0]
        
        return jsonify({
            "success": True,
            "room": {
                "name": room_info.name,
                "num_participants": room_info.num_participants,
                "creation_time": room_info.creation_time,
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting call status: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/customers-to-call", methods=["GET"])
def customers_to_call():
    """
    Get list of customers who have unpaid invoices
    (After email reminders have been sent)
    
    Query params:
    - days_since_email: Minimum days since email was sent (default: 3)
    - max_overdue_days: Maximum days overdue (optional)
    """
    try:
        days_since_email = request.args.get("days_since_email", 3, type=int)
        max_overdue_days = request.args.get("max_overdue_days", None, type=int)
        
        # Query Convex for customers with unpaid invoices
        customers = get_customers_needing_calls(
            days_since_email=days_since_email,
            max_overdue_days=max_overdue_days
        )
        
        if not customers:
            return jsonify({
                "success": True,
                "customers": [],
                "message": "No customers found with unpaid invoices"
            })
        
        return jsonify({
            "success": True,
            "customers": customers,
            "count": len(customers)
        })
        
    except Exception as e:
        logger.error(f"Error getting customers: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    livekit_api = get_livekit_api()
    return jsonify({
        "status": "healthy",
        "service": "Voice Call API",
        "livekit_configured": livekit_api is not None,
        "livekit_url_set": bool(LIVEKIT_URL),
        "livekit_key_set": bool(LIVEKIT_API_KEY),
        "livekit_secret_set": bool(LIVEKIT_API_SECRET),
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=True)

