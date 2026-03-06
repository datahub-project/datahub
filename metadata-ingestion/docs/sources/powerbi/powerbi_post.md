### Limitations

- Some metadata and lineage fields are only available through admin APIs or specific tenant settings.
- Lineage quality depends on available model metadata and supported query/source patterns.
- Large tenants with many workspaces can require longer extraction windows.

### Troubleshooting

- **Authentication failures**: verify `tenant_id`, `client_id`, and `client_secret`, and confirm the app has the required Power BI API permissions.
- **Missing workspaces/assets**: check service principal access to target workspaces or enable the required admin API mode/settings.
- **Lineage gaps**: confirm lineage-related config is enabled and that semantic models expose supported upstream source details.
