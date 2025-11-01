#!/usr/bin/env python3
"""
LiveKit Voice Agent for Payment Reminders
Calls customers to remind them about unpaid invoices
"""

import asyncio
import os
import json
from typing import Annotated
from livekit.agents import (
    AutoSubscribe,
    JobContext,
    WorkerOptions,
    cli,
    llm,
    tts,
    stt,
)
from livekit.agents.voice import AgentSession
from livekit.plugins import (
    openai,
    silero,
    assemblyai,
)
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try different import paths for turn detector
MultilingualModel = None
try:
    # Try new location (livekit-agents 1.x)
    from livekit.plugins.turn_detector import MultilingualModel
    logger.info("Turn detector model loaded: MultilingualModel")
except ImportError:
    try:
        # Try old location
        from livekit.plugins.turn_detector.multilingual import MultilingualModel
        logger.info("Turn detector model loaded: MultilingualModel (old path)")
    except ImportError:
        try:
            # Try alternative location
            from livekit.plugins import turn_detector
            MultilingualModel = turn_detector.MultilingualModel
            logger.info("Turn detector model loaded: MultilingualModel (alt path)")
        except ImportError:
            # If not available, we'll use VAD-only mode
            logger.warning("Turn detector model not available, using VAD-only mode")
            MultilingualModel = None

# Configuration
CONVEX_URL = os.getenv("CONVEX_URL", "https://marvelous-emu-964.convex.cloud")
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "")


async def get_customer_profile(email: str = None, customer: str = None):
    """Get customer profile from Convex database"""
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


def build_payment_reminder_context(customer_profile):
    """Build context for payment reminder conversation"""
    if not customer_profile:
        return "Customer profile not found."
    
    context = f"Customer: {customer_profile.get('customer')}\n"
    context += f"Email: {customer_profile.get('email')}\n"
    context += f"Unpaid invoices: {customer_profile.get('unpaidInvoices', 0)}\n"
    context += f"Total outstanding: ${customer_profile.get('unpaidAmount', 0):.2f}\n\n"
    
    unpaid_list = customer_profile.get('unpaidInvoicesList', [])
    if unpaid_list:
        context += "Unpaid Invoice Details:\n"
        for inv in unpaid_list[:5]:  # Limit to 5 invoices
            days_overdue = inv.get('days_overdue', 0)
            context += f"- Invoice {inv.get('invoice_number')}: ${inv.get('total', 0):.2f}"
            if days_overdue > 0:
                context += f" ({days_overdue} days overdue)"
            context += "\n"
    
    return context


async def entrypoint(ctx: JobContext):
    """Entry point for the voice agent"""
    logger.info(f"Agent connecting to room: {ctx.room.name}")
    
    # Wait for the participant to connect
    await ctx.wait_for_participant()
    if len(ctx.room.remote_participants) == 0:
        logger.warning("No remote participants found, waiting...")
        await asyncio.sleep(2)
        if len(ctx.room.remote_participants) == 0:
            logger.error("No participants found after waiting")
            return
    
    participant = ctx.room.remote_participants[0]
    logger.info(f"Participant connected: {participant.identity}")
    
    # Extract customer info from room metadata or participant identity
    # Format: "customer:email@example.com" or "customer:Customer Name"
    customer_identifier = participant.identity
    email = None
    customer_name = None
    
    if ":" in customer_identifier:
        parts = customer_identifier.split(":", 1)
        if parts[0] == "email":
            email = parts[1]
        elif parts[0] == "customer":
            customer_name = parts[1]
    else:
        # Try to extract email from identity
        if "@" in customer_identifier:
            email = customer_identifier
    
    # Get customer profile
    logger.info(f"Fetching profile for email={email}, customer={customer_name}")
    customer_profile = await get_customer_profile(email=email, customer=customer_name)
    
    if not customer_profile:
        logger.warning("Customer profile not found, using default message")
        # Still proceed but with limited context
    
    # Build conversation context
    payment_context = build_payment_reminder_context(customer_profile) if customer_profile else ""
    
    # Create system prompt for the agent
    system_prompt = f"""You are a professional and courteous accounts receivable representative calling to remind a customer about their outstanding invoices.

Customer Information:
{payment_context}

Your role:
- Be professional, friendly, and understanding
- Clearly state the purpose of the call
- Mention the total outstanding amount and number of unpaid invoices
- Ask if they need any clarification about the invoices
- Offer to help with payment arrangements if needed
- Be concise - keep the call brief (under 2 minutes)
- If they commit to payment, confirm the timeline

Important guidelines:
- Don't be pushy or aggressive
- Listen to the customer's response
- Address any questions or concerns they may have
- If they need time, be accommodating but mention the urgency for overdue invoices
- End the call politely with next steps

Remember: The customer has already received email reminders, so be brief and focus on immediate action."""
    
    # Initialize TTS and STT
    tts_instance = openai.TTS(
        voice="alloy",  # Professional voice
    )
    stt_instance = openai.STT()
    
    # Create chat context with system prompt
    chat_ctx = llm.ChatContext()
    chat_ctx.append(role="system", text=system_prompt)
    
    # Initialize LLM with chat context
    llm_instance = openai.LLM(
        model="gpt-4o-mini",  # Fast and cost-effective
        chat_ctx=chat_ctx,
    )
    
    # Create voice assistant session with turn detection
    # Using LiveKit's recommended turn detection model for natural conversations
    # If MultilingualModel is not available, turn_detection will default to VAD-only
    turn_detection_mode = MultilingualModel() if MultilingualModel is not None else None
    
    assistant = AgentSession(
        vad=silero.VAD.load(),
        stt=stt_instance,
        llm=llm_instance,
        tts=tts_instance,
        turn_detection=turn_detection_mode,  # Context-aware turn detection (or None for VAD-only)
        # Turn detection configuration
        min_endpointing_delay=0.8,  # Wait 0.8s after silence before considering turn complete
        max_endpointing_delay=5.0,  # Max wait time for user to continue
        min_interruption_duration=0.5,  # Minimum speech to interrupt agent
        allow_interruptions=True,  # Allow user to interrupt
    )
    
    # Handle state changes
    from livekit.agents.voice import UserStateChangedEvent, AgentStateChangedEvent
    
    @assistant.on("user_state_changed")
    def on_user_state_changed(ev: UserStateChangedEvent):
        logger.info(f"User state: {ev.new_state}")
    
    @assistant.on("agent_state_changed")
    def on_agent_state_changed(ev: AgentStateChangedEvent):
        logger.info(f"Agent state: {ev.new_state}")
    
    # Start the conversation with a greeting
    greeting = f"""Hello, this is a call regarding your outstanding invoices. 
    {'You have ' + str(customer_profile.get('unpaidInvoices', 0)) + ' unpaid invoice' + ('s' if customer_profile.get('unpaidInvoices', 0) != 1 else '') + ' totaling $' + str(customer_profile.get('unpaidAmount', 0)) + '.' if customer_profile else 'We need to discuss your account.'}
    Do you have a moment to speak?"""
    
    # Create an agent wrapper for the session
    from livekit.agents import Agent
    
    # Create agent instance
    agent = Agent(ctx=ctx, opts=AutoSubscribe())
    
    # Start the assistant session with the agent
    await assistant.start(agent=agent, room=ctx.room)
    
    # Say the greeting
    await assistant.say(greeting, allow_interruptions=True)
    
    # Let the assistant handle the conversation
    # It will automatically manage turn-taking and conversation flow
    # Wait for the participant to disconnect or conversation to end
    try:
        while len(ctx.room.remote_participants) > 0:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await assistant.aclose()
    
    logger.info("Voice call completed")


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))

