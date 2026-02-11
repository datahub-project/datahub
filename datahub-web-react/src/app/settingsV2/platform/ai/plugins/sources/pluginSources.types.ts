import { PluginFormState } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';

import { AiPluginAuthType } from '@types';

/**
 * Per-field display overrides for a plugin source.
 * Allows each source to customize labels, placeholders, and helper text.
 */
export type FieldOverride = {
    label?: string;
    placeholder?: string;
    helperText?: string;
    /** Optional URL rendered as a "Learn more" link after the helper text */
    helperDocsUrl?: string;
    /** Label for the helper docs link. Defaults to "Learn more". */
    helperDocsLabel?: string;
};

/**
 * A single structured header field that maps to a custom header key-value pair.
 * Rendered as a labelled input instead of raw key/value rows.
 */
export type StructuredHeaderField = {
    /** The HTTP header key (e.g. "x-dbt-prod-environment-id") */
    headerKey: string;
    /** Label shown to the user */
    label: string;
    /** Placeholder text for text inputs */
    placeholder?: string;
    /** Helper text below the input */
    helperText?: string;
    /** Whether this field is required */
    required?: boolean;
    /**
     * If set, this field is only shown when the current auth type is in this list.
     * Omit to show for all auth types.
     */
    visibleForAuthTypes?: AiPluginAuthType[];
};

/**
 * Declarative definition for structured headers within a source's auth/connection card.
 * Replaces raw custom header key/value pairs with user-friendly inputs and switches.
 */
export type StructuredHeadersConfig = {
    /** Title for the structured headers section (e.g. "dbt Cloud Configuration") */
    sectionTitle: string;
    /** Always-visible fields (e.g. required environment IDs) */
    fields: StructuredHeaderField[];
};

/**
 * Configuration for a plugin source type.
 *
 * To add a new source, create a new entry in PLUGIN_SOURCES (pluginSources.ts).
 * The UI will automatically render the correct form based on this config.
 */
export type PluginSourceConfig = {
    /** Unique identifier for the source (e.g. 'github', 'dbt', 'snowflake', 'custom') */
    name: string;

    /** Display name shown on the source card */
    displayName: string;

    /** Short description shown on the source card */
    description: string;

    /** Subtitle shown in the modal header when configuring this source */
    configSubtitle?: string;

    /** URL to DataHub documentation for this source (rendered as "Learn more" in the modal subtitle) */
    datahubDocsUrl?: string;

    /**
     * Auth types this source supports.
     * - 1 entry  => auth type is auto-applied, selector hidden.
     * - 2+ entries => selector shown, filtered to just these options.
     */
    allowedAuthTypes: AiPluginAuthType[];

    /**
     * Fields to auto-populate when this source is selected.
     * These are applied to the form state and hidden from the user.
     */
    defaults: Partial<PluginFormState>;

    /**
     * Fields shown in the main configuration form.
     * Only these fields are visible to the user (plus advancedFields behind a toggle).
     */
    visibleFields: (keyof PluginFormState)[];

    /**
     * Fields shown behind an "Advanced" toggle.
     * Optional power-user settings.
     */
    advancedFields: (keyof PluginFormState)[];

    /**
     * Optional URL to documentation that helps users find their credentials.
     * Rendered as a link in the credential/authentication section of the form.
     */
    docsUrl?: string;

    /** Label for the docs link (e.g. "GitHub Developer Settings"). Falls back to "Setup guide". */
    docsLabel?: string;

    /**
     * Per-field label/placeholder/helperText overrides.
     * Allows customizing the display of fields per source.
     */
    fieldOverrides?: Partial<Record<keyof PluginFormState, FieldOverride>>;

    /**
     * Structured headers rendered as user-friendly inputs/switches inside the auth card.
     * When defined, replaces the raw custom headers section for this source.
     */
    structuredHeaders?: StructuredHeadersConfig;
};
