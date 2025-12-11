# Security Policy

**Repository:** `ndt-data-catalogue`  
**Description:** `Details the responsible disclosure process for security vulnerabilities.`  
**SPDX-License-Identifier:** `OGL-UK-3.0`  

## Responsible Disclosure 

The National Digital Twin Programme (NDTP) follows a **Coordinated Vulnerability Disclosure (CVD)** process to ensure security risks are addressed responsibly. 

By reporting security vulnerabilities through the responsible channels, you agree to: 
- Not disclose details of the vulnerability publicly until NDTP has had a reasonable opportunity to fix it. 
- Provide NDTP with adequate time to assess and mitigate the risk. 
- Act in good faith and follow ethical security research principles. 

NDTP reserves the right to take necessary action against unauthorised or harmful security testing activities. 

---

## Reporting Security Issues 

NDTP takes security seriously and encourages responsible reporting of vulnerabilities. 

If you believe you have found a security vulnerability in this repository, **please do not report it publicly**. Instead, follow the steps below to disclose the issue responsibly. 

### **How to Report a Security Issue** 

1. **Do not open a public issue on GitHub.** Instead, report security concerns via email to **[ndtp@businessandtrade.gov.uk]**. 
2. **Provide detailed information about the vulnerability**, including: 
   - A clear description of the issue. 
   - Steps to reproduce the vulnerability. 
   - Potential impact or risk level. 
   - Any suggested mitigation strategies. 
3. **Allow time for assessment and response.** NDTP will review the report and respond within **10 working days** to acknowledge receipt. 
4. **Cooperate with NDTP to validate and address the issue.** 

Once a resolution has been identified, NDTP may choose to: 
   - **Release a patch** as part of the next scheduled update. 
   - **Issue a security advisory** if the issue is critical. 
   - **Provide acknowledgments** where appropriate (subject to NDTP’s disclosure policy). 

---

## Scope 

This security policy applies to: 
- All NDTP repositories released as open source. 
- Code, configuration files, and infrastructure deployed as part of NDTP’s **Integration Architecture (IA)**. 
- **Third-party dependencies** included within NDTP repositories. If you identify a vulnerability in a third-party component that NDTP relies on (e.g., outdated libraries or known security flaws in dependencies), we encourage you to report it. 

Out of scope: 
- Issues related to third-party services or software **not used within NDTP repositories**. 
- Vulnerabilities in user environments that are unrelated to this repository. 
- Unsolicited security testing or penetration testing without NDTP’s explicit permission. 

---

## Security Best Practices 

To help maintain security across NDTP repositories, we follow these principles: 
- Dependencies are **scanned and updated regularly** (e.g., using automated tools like Dependabot). 
- Sensitive credentials **must not be included** in public repositories. 
- Security patches are applied in a timely manner, with priority given to critical vulnerabilities. 

For more details, refer to our **Secure Development Guidelines** [Link to internal security policy, if available]. 

---

**Maintained by the National Digital Twin Programme (NDTP).** 

© Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity. 

Licensed under the Open Government Licence v3.0. 

For full licensing terms, see [OGL_LICENCE.md](./OGL_LICENCE.md).
