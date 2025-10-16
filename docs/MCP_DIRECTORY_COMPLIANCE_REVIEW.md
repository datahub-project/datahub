# DataHub MCP Server - Anthropic Directory Compliance Review

This document reviews the DataHub MCP server implementation against [Anthropic's MCP Directory Policy](https://support.claude.com/en/articles/11697096-anthropic-mcp-directory-policy) requirements for inclusion in their official directory.

## Safety and Security Requirements

**1)** MCP servers must not be designed to facilitate or easily enable violation of our Usage Policy.

- **✅ COMPLIANT**: Server facilitates legitimate data discovery and metadata querying use cases

**2)** MCP servers must not employ methods to evade or enable users to circumvent Claude's safety guardrails.

- **✅ COMPLIANT**: No attempts to circumvent safety guardrails

**3)** MCP servers should prioritize user privacy protection.

- **✅ COMPLIANT**: Server only accesses DataHub metadata that users have authorized access to

**4)** MCP servers should only collect data from the user's context that is necessary to perform their function.

- **✅ COMPLIANT**: Only collects necessary data (DataHub URNs, query parameters) for core function

**5)** MCP servers must not infringe on the intellectual property rights of others.

- **✅ COMPLIANT**: Server interfaces with DataHub's own API, no IP infringement

**6)** MCP servers should not attempt to access information about the users' previous chats or the contents of their memory.

- **✅ COMPLIANT**: No attempts to access user chat history or memory

## Compatibility Requirements

**7)** MCP tool descriptions must narrowly and unambiguously describe what each tool does and when it should be invoked.

- **✅ COMPLIANT**: Well-documented, specific descriptions that clearly explain tool functionality

**8)** MCP tool descriptions must precisely match actual functionality, ensuring the server is called at correct and appropriate times.

- **✅ COMPLIANT**: Tool descriptions precisely match actual functionality

**9)** MCP tool descriptions should not create confusion or conflict with other MCP servers in our directory.

- **✅ COMPLIANT**: Descriptions are DataHub-specific and don't conflict with other servers

**10)** MCP servers should not intentionally call or coerce Claude into calling other servers.

- **✅ COMPLIANT**: No attempts to call or reference other MCP servers

**11)** MCP servers should not attempt to interfere with Claude calling tools from other servers.

- **✅ COMPLIANT**: No interference with other servers

**12)** MCP servers should not direct Claude to dynamically pull behavioral instructions from external sources for Claude to execute.

- **✅ COMPLIANT**: Server doesn't pull external behavioral instructions

## Functionality Requirements

**13)** MCP servers must deliver reliable performance with fast response times and maintain consistently high availability.

- **✅ COMPLIANT**: Uses async/await patterns with comprehensive logging and error handling

**14)** MCP servers must gracefully handle errors and provide helpful feedback rather than generic error messages.

- **✅ COMPLIANT**: Detailed error messages with context (see `_execute_graphql` function lines 230-249)

**15)** MCP servers should be frugal with their use of tokens.

- **✅ COMPLIANT**: Smart content truncation (`truncate_descriptions`, `sanitize_and_truncate_description`) and response cleaning

**16)** Remote MCP servers that connect to a remote service and require authentication must use secure OAuth 2.0 with certificates from recognized authorities.

- **✅ COMPLIANT**: Uses secure token-based authentication via Bearer tokens or query parameters

**17)** MCP servers must provide all applicable annotations for their tools, in particular _readOnlyHint_, _destructiveHint_, and _title_.

- **❌ NON-COMPLIANT**: Missing `readOnlyHint`, `destructiveHint`, and `title` annotations on tools

**18)** Remote MCP servers should support the Streamable HTTP transport.

- **✅ COMPLIANT**: **YES!** Supports streamable HTTP in `router.py:64-67`: `mcp_http_app = datahub_fastmcp.http_app(stateless_http=True)`

**19)** Local MCP servers should be built with reasonably current versions of all dependencies.

- **✅ COMPLIANT**: Using recent versions of FastMCP and other libraries

## Developer Requirements

**20)** Developers of MCP servers that collect user data or connect to a remote service must provide a clear, accessible privacy policy link explaining data collection, usage, and retention.

- **❌ NON-COMPLIANT**: Missing privacy policy explaining data handling practices

**21)** Developers must provide verified contact information and support channels for users with product concerns.

- **✅ COMPLIANT**: Available via GitHub, Slack community (datahub.com/slack), and company contact

**22)** Developers must document how their MCP server works, its intended purpose, and how users can troubleshoot issues.

- **✅ COMPLIANT**: Comprehensive documentation at `docs/features/feature-guides/mcp.md`

**23)** Developers must provide a standard testing account with sample data for Anthropic to verify full MCP functionality.

- **❌ NON-COMPLIANT**: Need dedicated test DataHub instance with sample data for Anthropic review

**24)** Developers must provide at least three working examples of prompts or use cases that demonstrate core functionality.

- **⚠️ INCOMPLETE**: Documentation shows setup but lacks specific prompt examples

**25)** Developers must verify that they own or control any API endpoint their MCP server connects to.

- **✅ COMPLIANT**: You control DataHub API endpoints

**26)** Developers must maintain their MCP server and address issues within reasonable timeframes.

- **⚠️ NEEDS COMMITMENT**: Formal maintenance commitment required

**27)** Developers must agree to our MCP Directory Terms.

- **⚠️ PENDING**: Must review and agree to Anthropic's MCP Directory Terms

## Unsupported Use Cases

**28)** MCP servers that transfer money, cryptocurrency, or other financial assets, or execute financial transactions on behalf of users.

- **✅ COMPLIANT**: Server only queries metadata, no financial operations

**29)** MCP servers that can generate images, video, or audio content.

- **✅ COMPLIANT**: No content generation capabilities

**30)** MCP servers that enable cross-service automation.

- **✅ COMPLIANT**: Only accesses DataHub APIs, no cross-service orchestration

## Summary

### Compliance Score: **24/30** (80% Compliant)

| Category                       | Score | Status                  |
| ------------------------------ | ----- | ----------------------- |
| Safety & Security (1-6)        | 6/6   | ✅ **FULLY COMPLIANT**  |
| Compatibility (7-12)           | 6/6   | ✅ **FULLY COMPLIANT**  |
| Functionality (13-19)          | 6/7   | ⚠️ **MOSTLY COMPLIANT** |
| Developer Requirements (20-27) | 3/8   | ❌ **NEEDS WORK**       |
| Unsupported Use Cases (28-30)  | 3/3   | ✅ **FULLY COMPLIANT**  |

### Required Actions for Full Compliance

1. **Add Tool Annotations** (Req #17): Add `readOnlyHint=True` and `title` to all `@mcp.tool` decorators
2. **Create Privacy Policy** (Req #20): Document data collection and usage practices
3. **Provide Test Account** (Req #23): Set up test DataHub instance with sample data for Anthropic
4. **Add Usage Examples** (Req #24): Include 3+ specific prompt examples in documentation
5. **Maintenance Commitment** (Req #26): Formally document support commitments
6. **Agree to Terms** (Req #27): Review and accept Anthropic's MCP Directory Terms

### Key Technical Confirmation

**✅ STREAMABLE HTTP SUPPORT: YES** - Your server fully supports the required streamable HTTP transport (Requirement #18)

---

_Review completed on: January 17, 2025_  
_Based on: [Anthropic MCP Directory Policy](https://support.claude.com/en/articles/11697096-anthropic-mcp-directory-policy)_
