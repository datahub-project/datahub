# SMTP Email Notifications

:::note Availability
This feature is available only on self-managed DataHub Enterprise clusters (BYOC / on-prem).
:::

## Overview

DataHub Enterprise (self-managed) can send notifications directly via any standard SMTP server. Use it when:

- You want to use your existing email infrastructure (Gmail, Outlook, AWS SES, etc.)
- You need full control over email delivery, branding, and templates
- Your security policies require email to stay within your network

## Supported Notification Types

- Entity change alerts (tags, owners, domains, schema changes)
- Ingestion pipeline status updates
- Incident creation and status changes
- Metadata change proposals and approvals
- Assertion pass/fail results
- Workflow assignments and status changes
- Compliance form publications
- User invitations
- Custom messages

## Configuration

### Required Environment Variables

| Variable         | Description                                                            |
| ---------------- | ---------------------------------------------------------------------- |
| `EMAIL_PROVIDER` | Must be set to `smtp`                                                  |
| `SMTP_HOST`      | SMTP server hostname (e.g., `smtp.gmail.com`, `smtp-mail.outlook.com`) |
| `SMTP_USERNAME`  | SMTP login username (typically your email address)                     |
| `SMTP_PASSWORD`  | SMTP login password (use an app password, not your regular password)   |


**Minimal setup**

```bash
export EMAIL_PROVIDER=smtp
export SMTP_HOST=smtp.gmail.com
export SMTP_USERNAME=your-email@gmail.com
export SMTP_PASSWORD=your-app-password
```



### Optional Environment Variables

| Variable                                 | Default                      | Description                                                                                                                                                                    |
| ---------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SMTP_PORT`                              | `587`                        | SMTP port (465 for SSL, 587 for STARTTLS)                                                                                                                                      |
| `SMTP_USE_TLS`                           | `true`                       | Enable STARTTLS (ignored for port 465 which uses SSL)                                                                                                                          |
| `FROM_EMAIL_ADDRESS`                     | `notifications@app.acryl.io` | Sender email address                                                                                                                                                           |
| `FROM_EMAIL_TITLE`                       | `DataHub Cloud`              | Sender display name                                                                                                                                                            |
| `EMAIL_SINK_ENABLED`                     | `true`                       | Enable/disable email notifications                                                                                                                                             |
| `MAX_NOTIFICATION_RETRIES`               | `3`                          | Max retry attempts per notification                                                                                                                                            |
| `DATAHUB_BASE_URL`                       | `http://localhost:9002`      | DataHub URL used in email links                                                                                                                                                |
| `NOTIFICATION_LOGO_URL`                  | _(built-in DataHub logo)_    | URL of a logo image shown in email templates (roughly square PNG/JPG, scaled to 60px wide; SVG is unreliable in email clients). Leave unset to keep the built-in DataHub logo. |
| `NOTIFICATION_FOOTER_TEXT`               | _(built-in DataHub footer)_  | Footer signature text shown on every email. Leave unset to keep the built-in DataHub footer.                                                                                   |
| `SMTP_POOL_MIN_CONNECTIONS`              | `1`                          | Minimum pooled SMTP connections                                                                                                                                                |
| `SMTP_POOL_MAX_CONNECTIONS`              | `5`                          | Maximum pooled SMTP connections                                                                                                                                                |
| `SMTP_POOL_MAX_CONNECTION_AGE`           | `300`                        | Max connection age in seconds before retirement                                                                                                                                |
| `SMTP_POOL_MAX_CONNECTION_USES`          | `100`                        | Max uses per connection before retirement                                                                                                                                      |
| `SMTP_POOL_CONNECTION_TIMEOUT`           | `30`                         | Timeout in seconds when waiting for a connection                                                                                                                               |
| `SMTP_CIRCUIT_BREAKER_ENABLED`           | `true`                       | Enable circuit breaker for fail-fast                                                                                                                                           |
| `SMTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5`                          | Failures before opening circuit                                                                                                                                                |
| `SMTP_CIRCUIT_BREAKER_RECOVERY_TIMEOUT`  | `60`                         | Seconds before attempting recovery                                                                                                                                             |
| `LOG_LEVEL`                              | `INFO`                       | Log verbosity (`DEBUG` for SMTP diagnostics)                                                                                                                                   |

### Email Branding

The logo and footer of SMTP email notifications are configurable (on self-managed DataHub Enterprise). Both are optional — leave them unset to keep the built-in DataHub logo and footer.

```bash
export NOTIFICATION_LOGO_URL=https://example.com/logo.png   # roughly square PNG/JPG, scaled to 60px wide
export NOTIFICATION_FOOTER_TEXT="Acme Corp"                  # footer signature shown on every email
```

### Provider Examples


**Gmail**

```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USE_TLS=true
```

Requires an [App Password](https://support.google.com/accounts/answer/185833) (not your regular password). Enable 2-Factor Authentication first.




**Outlook / Office 365**

```bash
export SMTP_HOST=smtp-mail.outlook.com
export SMTP_PORT=587
export SMTP_USE_TLS=true
```




**Custom SMTP Server**

```bash
export SMTP_HOST=your-smtp-server.com
export SMTP_PORT=587  # or 465 for SSL, 25 for unencrypted
export SMTP_USE_TLS=true
```

**Note:** Port 465 uses SSL from connection start. Port 587 uses STARTTLS when `SMTP_USE_TLS=true`.



## Reliability Features

The SMTP system includes built-in reliability features that require no additional configuration:

- **Connection pooling** — reuses SMTP connections across sends; automatically validates, retires, and replaces connections
- **Circuit breaker** — when the SMTP server is unreachable, requests fail immediately instead of timing out; automatically tests for recovery and resumes normal operation
- **Smart retry** — classifies errors as transient or permanent; retries transient failures with exponential backoff, fails immediately on auth errors or invalid addresses

All of these are configurable via the environment variables listed above.

### Prometheus Metrics

SMTP metrics are exported via the `/metrics` endpoint:

| Metric                             | Type      | Labels                    | Description                               |
| ---------------------------------- | --------- | ------------------------- | ----------------------------------------- |
| `smtp_emails_total`                | Counter   | `template_type`, `status` | Total emails sent (success/failure)       |
| `smtp_delivery_duration_seconds`   | Histogram | `template_type`, `status` | Per-email delivery time including retries |
| `smtp_circuit_breaker_trips_total` | Counter   | -                         | Times circuit breaker tripped to open     |
| `smtp_pool_exhaustion_total`       | Counter   | -                         | Times connection pool was fully exhausted |

## Monitoring

### Health Endpoint


**Response structure and alert thresholds**

```
GET /private/notifications/smtp/health
```

```bash
kubectl port-forward -n datahub svc/datahub-acryl-datahub-integrations 9003:9003
curl -s http://localhost:9003/private/notifications/smtp/health | jq .
```

**Key metrics to alert on:**

| Metric                         | Alert Condition               | Meaning                  |
| ------------------------------ | ----------------------------- | ------------------------ |
| `status`                       | `"unhealthy"` or `"degraded"` | System has issues        |
| `circuit_breaker.state`        | `"open"`                      | SMTP server is down      |
| `connection_pool.utilization`  | `> 0.8`                       | Pool near capacity       |
| `connection_pool.failure_rate` | `> 0.1`                       | 10%+ connection failures |



## Troubleshooting

1. **Authentication Failed** — Use an app password, not your regular password. Verify 2FA is enabled for Gmail.
2. **Connection Timeout** — Verify SMTP host and port. Check firewall rules. Ensure TLS/SSL settings match your provider.
3. **Email Not Received** — Check spam/junk folder. Verify recipient email address. Check provider sending limits.
4. **SSL/TLS Errors** — Port 465 requires SSL; port 587 requires `SMTP_USE_TLS=true` for STARTTLS.

Set `LOG_LEVEL=DEBUG` for verbose SMTP diagnostics.

## Security Considerations

1. **Use App Passwords** — Never use your main email password
2. **Store credentials in Kubernetes secrets** — Not in env vars or code
3. **Always use TLS** — Encrypt SMTP traffic in transit
4. **Configure DNS** — Set up SPF, DKIM, and DMARC records for your sending domain
5. **Use a dedicated sending account** — Don't use personal email accounts in production
