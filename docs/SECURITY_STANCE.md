# DataHub's Commitment to Security

## Introduction

The open-source DataHub project takes security seriously. As part of our commitment to maintaining a secure environment
for our users and contributors, we have established a comprehensive security policy. This document outlines the key
aspects of our approach to handling security vulnerabilities and keeping our community informed.

## Our Track Record

We have a proactive approach to security. To date we've successfully resolved many security related issues reported by
community members or flagged by automated scanners (which includes upstream dependencies and what known risks the
dependencies contain), demonstrating our commitment to maintaining a secure platform. This is a testament to the
collaborative efforts of our community in identifying and helping us address potential vulnerabilities. It truly takes
a village.

## Reporting Security Issues

If you believe you've discovered a security vulnerability in DataHub, we encourage you to report it immediately. We have
a dedicated process for handling security-related issues to ensure they're addressed promptly and discreetly.

For detailed instructions on how to report a security vulnerability, including our PGP key for encrypted communications,
please visit our official security policy page:

[DataHub Security Policy](https://github.com/datahub-project/datahub/security/policy)

We kindly ask that you do not disclose the vulnerability publicly until the committers have had the chance to address it
and make an announcement.

## Our Response Process

Once a security issue is reported, the project follows a structured process to ensure that each report is handled with
the attention and urgency it deserves. This includes:

1. Verifying the reported vulnerability
2. Assessing its potential impact
3. Developing and testing a fix
4. Releasing security patches
5. Coordinating the public disclosure of the vulnerability

All reported vulnerabilities are carefully assessed and triaged internally to ensure appropriate action is taken.

## How we prioritize (and the dangers of blindly following automated scanners)

While we appreciate the value of automated vulnerability detection systems like Dependabot, we want to emphasize the
importance of critical thinking when addressing flagged issues. These systems are excellent at providing signals of
potential vulnerabilities, but they shouldn't be followed blindly.

Here's why:

1. Context matters: An issue flagged might only affect a non-serving component of the stack (such as our docs-website
   code or our CI smoke tests), which may not pose a significant risk to the overall system.

2. False positives: Sometimes, these systems may flag vulnerabilities in libraries that are linked but not actively
   used. For example, a vulnerability in an email library might be flagged even if the software never sends emails.

3. Exploit feasibility: Some vulnerabilities may be technically present but extremely difficult or impractical to
   exploit in real-world scenarios. Automated scanners often don't consider the actual implementation details or
   security controls that might mitigate the risk. For example, a reported SQL injection vulnerability might exist in
   theory, but if the application uses parameterized queries or has proper input validation in place, the actual risk
   could be significantly lower than the scanner suggests.

We carefully review all automated alerts in the context of our specific implementation to determine the actual risk and
appropriate action.

## Keeping the Community Informed

Transparency is key in maintaining trust within our open-source community. To keep everyone informed about
security-related matters:

- We maintain Security Advisories on the DataHub project GitHub repository
- These advisories include summaries of security issues, details on the fixes implemented, and any necessary mitigation
  steps for users

## Conclusion

Security is an ongoing process, and we're committed to continuously improving our practices. By working together with
our community of users and contributors, we aim to maintain DataHub as a secure and reliable metadata platform.

We encourage all users to stay updated with our security announcements and to promptly apply any security patches
released. Together, we can ensure a safer environment for everyone in the DataHub community.
