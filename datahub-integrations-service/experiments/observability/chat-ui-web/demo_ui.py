"""
Interactive demo of the DataHub Chat UI.
Keeps browser open so we can see the thinking messages in real-time.
"""
import asyncio
from playwright.async_api import async_playwright


async def demo_ui():
    """Demo the chat UI interactively."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        print("🌐 Opening DataHub Chat UI at http://localhost:8001/")
        await page.goto("http://localhost:8001/")
        await page.wait_for_load_state("networkidle")
        print("✅ Page loaded!")

        await page.wait_for_timeout(2000)

        # Start a new conversation
        print("\n📝 Starting new conversation...")
        start_button = await page.wait_for_selector('button:has-text("Start New Chat")', timeout=5000)
        await start_button.click()
        await page.wait_for_timeout(2000)
        print("✅ Chat opened!")

        # Send a message
        print("\n💬 Sending message: 'What datasets do we have from BigQuery?'")
        input_field = await page.wait_for_selector('textarea', timeout=5000)
        await input_field.fill("What datasets do we have from BigQuery?")

        send_button = await page.query_selector('button[type="submit"]')
        if send_button:
            await send_button.click()
        else:
            await input_field.press("Enter")

        print("✅ Message sent!")
        print("\n👀 Watch the UI - you should see:")
        print("   1. 💭 Thinking messages (agent's reasoning)")
        print("   2. 🔧 Tool calls (which tools are being used)")
        print("   3. 🤖 Final response with data")
        print("\n⏳ Waiting 30 seconds for response...")

        # Watch for updates
        for i in range(30):
            await page.wait_for_timeout(1000)
            content = await page.evaluate("() => document.body.innerText")

            # Check what's visible
            if "💭" in content and i == 0:
                print(f"   [{i+1}s] 💭 Thinking message appeared!")
            if "🔧" in content and "Calling tool" in content:
                # Extract tool name
                if "create_plan" in content:
                    print(f"   [{i+1}s] 🔧 Tool: create_plan")
                elif "datahub__search" in content:
                    print(f"   [{i+1}s] 🔧 Tool: datahub__search")
            if "BigQuery" in content and "datasets" in content.lower():
                print(f"   [{i+1}s] ✅ Final response received!")
                break

        print("\n✨ Demo complete! Browser will stay open for 2 minutes so you can explore.")
        print("   - Scroll through the messages to see thinking process")
        print("   - Try asking another question!")
        print("   - The browser will close automatically in 120 seconds")

        await page.wait_for_timeout(120000)  # Keep open for 2 minutes

        await browser.close()
        print("\n👋 Browser closed!")


if __name__ == "__main__":
    asyncio.run(demo_ui())
