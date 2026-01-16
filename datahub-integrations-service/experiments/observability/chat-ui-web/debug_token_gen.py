"""
Debug token generation by calling the backend API endpoint directly.
"""
import asyncio
import httpx


async def test_token_generation():
    """Test the token generation endpoint."""
    base_url = "http://localhost:8000"

    print("🔍 Testing token generation endpoint...")

    # First, get the current config to find the GMS URL
    print("\n1. Getting current config...")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{base_url}/api/config")
            if response.status_code == 200:
                config = response.json()
                print(f"✅ Current config:")
                print(f"   GMS URL: {config.get('gms_url')}")
                print(f"   Mode: {config.get('mode')}")

                gms_url = config.get('gms_url')
                if not gms_url:
                    print("❌ No GMS URL in config. Try running auto-discover first.")
                    return

                # Now try to generate token
                print(f"\n2. Generating token for GMS URL: {gms_url}")
                token_response = await client.post(
                    f"{base_url}/api/config/token/generate",
                    json={"gms_url": gms_url}
                )

                print(f"   Status: {token_response.status_code}")
                result = token_response.json()
                print(f"   Response: {result}")

                if "token" in result:
                    print(f"✅ Token generated successfully!")
                    print(f"   Token (first 20 chars): {result['token'][:20]}...")
                elif "error" in result:
                    print(f"❌ Token generation failed:")
                    print(f"   Error: {result['error']}")

            else:
                print(f"❌ Failed to get config: {response.status_code}")
                print(f"   Response: {response.text}")

        except Exception as e:
            print(f"❌ Exception occurred: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_token_generation())
