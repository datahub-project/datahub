/**
 * Keys used in the `extraArgs` map that is passed through to the ingestion executor.
 * Shared between the old builder (NameSourceStep) and V2 multi-step builder (AdvancedSection).
 */
export const ExtraEnvKey = 'extra_env_vars';
export const ExtraReqKey = 'extra_pip_requirements';
export const ExtraPluginKey = 'extra_pip_plugins';
export const ExternalPluginKey = 'datahub_plugins';
export const PluginSourceUrlKey = 'datahub_plugin_source_url';
