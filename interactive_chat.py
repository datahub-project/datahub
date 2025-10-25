#!/usr/bin/env python3
"""
Interactive command-line chat with DataHub AI Agent.

This script provides an interactive CLI for chatting with the DataHub AI agent,
showing real-time streaming responses and detailed logging of tool calls.
"""

import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

import requests
import sseclient  # pip install sseclient-py


@dataclass
class ChatConfig:
    """Configuration for the chat session"""
    
    graphql_url: str = "http://localhost:9002/api/graphql"
    stream_url: str = "http://localhost:9002/api/chat/stream"
    user_urn: str = "urn:li:corpuser:admin"
    token: str = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImFkbWluIiwidHlwZSI6IlBFUlNPTkFMIiwidmVyc2lvbiI6IjIiLCJqdGkiOiJlMDM5ZDk5ZS0wOWI3LTRmODctODQ2Mi05MWQ1YTEwZDUzMTUiLCJzdWIiOiJhZG1pbiIsImV4cCI6MTc2MzAwOTkwMSwiaXNzIjoiZGF0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.MovrgPKe0DcHIwfsGARMypDgB8JKagck3wBLi7ef-I4"


class InteractiveChat:
    """Interactive chat session with DataHub AI Agent"""
    
    def __init__(self, config: ChatConfig):
        self.config = config
        self.conversation_urn: Optional[str] = None
        self.headers = {
            "Authorization": f"Bearer {config.token}",
            "Content-Type": "application/json"
        }
        self.message_count = 0
    
    def create_conversation(self) -> str:
        """Create a new conversation via GraphQL"""
        mutation = """
        mutation CreateConversation($title: String) {
            createAgentConversation(title: $title) {
                urn
            }
        }
        """
        
        variables = {"title": "Interactive CLI Chat Session"}
        
        response = requests.post(
            self.config.graphql_url,
            json={"query": mutation, "variables": variables},
            headers=self.headers
        )
        response.raise_for_status()
        
        result = response.json()
        if "errors" in result:
            raise Exception(f"GraphQL error: {result['errors']}")
        
        conversation_urn = result["data"]["createAgentConversation"]["urn"]
        print(f"\n🆕 Created conversation: {conversation_urn}\n")
        return conversation_urn
    
    def delete_conversation(self, conversation_urn: str):
        """Delete a conversation via GraphQL"""
        mutation = """
        mutation DeleteConversation($urn: String!) {
            deleteAgentConversation(urn: $urn)
        }
        """
        
        variables = {"urn": conversation_urn}
        
        try:
            response = requests.post(
                self.config.graphql_url,
                json={"query": mutation, "variables": variables},
                headers=self.headers
            )
            response.raise_for_status()
            print(f"\n🗑️  Deleted conversation: {conversation_urn}")
        except Exception as e:
            print(f"\n⚠️  Failed to delete conversation: {e}")
    
    def send_message(self, text: str) -> None:
        """Send a message and stream the response"""
        if not self.conversation_urn:
            self.conversation_urn = self.create_conversation()
        
        self.message_count += 1
        
        # Print user message
        print(f"\n{'='*80}")
        print(f"📤 YOU: {text}")
        print(f"{'='*80}")
        
        # Prepare request
        payload = {
            "conversationUrn": self.conversation_urn,
            "text": text,
            "userUrn": self.config.user_urn
        }
        
        # Send request and stream response
        try:
            response = requests.post(
                self.config.stream_url,
                json=payload,
                headers=self.headers,
                stream=True
            )
            response.raise_for_status()
            
            # Parse SSE stream
            client = sseclient.SSEClient(response)
            
            print(f"\n🤖 AI AGENT:")
            print("-" * 80)
            
            event_count = 0
            ai_response_text = ""
            last_was_thinking = False
            
            for event in client.events():
                event_count += 1
                
                if event.event == 'message':
                    try:
                        message_data = json.loads(event.data)
                        msg_type = message_data.get("type", "UNKNOWN")
                        content = message_data.get("content", {})
                        text_content = content.get("text", "")
                        actor = message_data.get("actor", {})
                        actor_type = actor.get("type", "UNKNOWN")
                        
                        # Show different message types
                        if msg_type == "THINKING":
                            if not last_was_thinking:
                                print(f"\n💭 [THINKING]", end="", flush=True)
                            print(".", end="", flush=True)
                            last_was_thinking = True
                        
                        elif msg_type == "TOOL_CALL":
                            if last_was_thinking:
                                print()  # New line after thinking dots
                            print(f"\n🔧 [TOOL_CALL] {text_content}")
                            last_was_thinking = False
                        
                        elif msg_type == "TOOL_RESULT":
                            print(f"   └─ [RESULT] {text_content[:100]}{'...' if len(text_content) > 100 else ''}")
                            last_was_thinking = False
                        
                        elif msg_type == "TEXT":
                            if last_was_thinking:
                                print()  # New line after thinking dots
                            if actor_type == "USER":
                                # Echo of user message (skip)
                                pass
                            else:
                                # AI response
                                if text_content:
                                    print(f"\n{text_content}")
                                    ai_response_text = text_content
                            last_was_thinking = False
                        
                        else:
                            print(f"\n⚠️  [UNKNOWN MESSAGE TYPE: {msg_type}] {text_content[:100]}")
                            last_was_thinking = False
                    
                    except json.JSONDecodeError as e:
                        print(f"\n❌ Failed to parse message: {e}")
                        print(f"   Raw data: {event.data[:200]}")
                
                elif event.event == 'complete':
                    if last_was_thinking:
                        print()  # New line after thinking dots
                    print(f"\n{'─'*80}")
                    print(f"✅ Stream completed ({event_count} events received)")
                    break
                
                elif event.event == 'error':
                    print(f"\n❌ Error: {event.data}")
                    break
                
                else:
                    print(f"\n⚠️  Unknown event type: {event.event}")
        
        except requests.exceptions.HTTPError as e:
            print(f"\n❌ HTTP Error: {e}")
            print(f"   Response: {e.response.text if hasattr(e, 'response') else 'N/A'}")
        except Exception as e:
            print(f"\n❌ Error: {e}")
    
    def run(self):
        """Run the interactive chat loop"""
        print("="*80)
        print(" DataHub AI Agent - Interactive Chat")
        print("="*80)
        print(f"\n📝 Configuration:")
        print(f"   • GraphQL URL: {self.config.graphql_url}")
        print(f"   • Stream URL:  {self.config.stream_url}")
        print(f"   • User URN:    {self.config.user_urn}")
        print(f"\n💡 Commands:")
        print(f"   • Type your message and press Enter to chat")
        print(f"   • Type 'quit' or 'exit' to end the session")
        print(f"   • Type 'new' to start a new conversation")
        print(f"   • Type 'clear' to clear the screen")
        print("="*80)
        
        try:
            while True:
                try:
                    # Get user input
                    user_input = input("\n💬 You: ").strip()
                    
                    if not user_input:
                        continue
                    
                    # Handle commands
                    if user_input.lower() in ['quit', 'exit', 'q']:
                        print("\n👋 Goodbye!")
                        break
                    
                    elif user_input.lower() == 'new':
                        if self.conversation_urn:
                            self.delete_conversation(self.conversation_urn)
                        self.conversation_urn = None
                        self.message_count = 0
                        print("\n🆕 Starting new conversation...")
                        continue
                    
                    elif user_input.lower() == 'clear':
                        os.system('clear' if os.name != 'nt' else 'cls')
                        print("="*80)
                        print(" DataHub AI Agent - Interactive Chat")
                        print("="*80)
                        continue
                    
                    # Send message
                    self.send_message(user_input)
                
                except KeyboardInterrupt:
                    print("\n\n⚠️  Interrupted by user")
                    break
                except EOFError:
                    print("\n\n👋 Goodbye!")
                    break
        
        finally:
            # Clean up
            if self.conversation_urn:
                print("\n🧹 Cleaning up...")
                self.delete_conversation(self.conversation_urn)


def main():
    """Main entry point"""
    # Read token from environment or use default
    token = os.environ.get("DATAHUB_TOKEN")
    if not token:
        token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImFkbWluIiwidHlwZSI6IlBFUlNPTkFMIiwidmVyc2lvbiI6IjIiLCJqdGkiOiJlMDM5ZDk5ZS0wOWI3LTRmODctODQ2Mi05MWQ1YTEwZDUzMTUiLCJzdWIiOiJhZG1pbiIsImV4cCI6MTc2MzAwOTkwMSwiaXNzIjoiZGF0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.MovrgPKe0DcHIwfsGARMypDgB8JKagck3wBLi7ef-I4"
        print(f"⚠️  No DATAHUB_TOKEN environment variable set, using default token")
    
    config = ChatConfig(token=token)
    chat = InteractiveChat(config)
    chat.run()


if __name__ == "__main__":
    main()

