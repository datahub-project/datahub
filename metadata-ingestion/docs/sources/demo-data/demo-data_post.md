### Capabilities

- Loads any named pack from the DataHub registry (`bootstrap`, `showcase-ecommerce`, `covid-bigquery`)
- Loads custom packs from arbitrary HTTP(S) URLs via `pack_url`
- Time-shifts timestamps so ingested metadata appears current (set `no_time_shift: false`)
- SHA256 integrity verification for registry packs
- Local caching to avoid repeated downloads

### Limitations

- Data packs are read-only collections of MCPs; they cannot be modified before loading.
- Time-shifting adjusts all temporal fields by a fixed offset — relative ordering is preserved but absolute times may not match real-world events.
- Custom URL packs (`pack_url`) bypass SHA256 verification unless the pack includes a checksum.

### Troubleshooting

- **"Pack not found"**: Verify the pack name with `datahub datapack list`. Pack names are case-sensitive.
- **Trust errors**: Community and custom packs require explicit opt-in via `trust_community: true` or `trust_custom: true`.
- **Download failures**: Check network connectivity to the pack URL. Use `no_cache: true` to force a fresh download if a cached file is corrupted.
