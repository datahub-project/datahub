import yaml from 'js-yaml';
import React, { useContext, useEffect, useState } from 'react';
import { Route, Switch } from 'react-router';

import { MFEConfigContext } from '@app/mfeframework/MFEConfigContext';
import { MFEBaseConfigurablePage } from '@app/mfeframework/MFEConfigurableContainer';
import { DEFAULT_MFE_SLOT, MFESlotId, isMFESlotId } from '@app/mfeframework/slots/slotTypes';
import { NoPageFound } from '@app/shared/NoPageFound';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export interface MFEFlags {
    enabled: boolean;
    showInNav: boolean;
}

/**
 * Where an MFE renders. Omitted => `nav.page` (a full page reached from the left navigation),
 * which is how every entry behaved before placements existed.
 */
export type MFEPlacement = {
    slot: MFESlotId;
    /**
     * Coarse filter for entity-scoped slots: GraphQL EntityType names (case-insensitive, e.g. `dataset`).
     * Omitted => the MFE is offered on every entity type the slot appears on.
     */
    entityTypes?: string[];
    /** Tab label for `entity.detail.tab`; defaults to `label`. */
    tabName?: string;
};

// MFEConfig: Type for a valid micro frontend config entry.
export interface MFEConfig {
    id: string;
    label: string;
    /** Route under /mfe. Required for `nav.page` placements only. */
    path?: string;
    remoteEntry: string;
    // Must look like 'myRemoteModule/mount' which is exposed remote followed by exposed mount function inside it
    module: string;
    flags: MFEFlags;
    /** Phosphor icon name. Required for `nav.page` placements only. */
    navIcon?: string;
    placement?: MFEPlacement;
}

// MFESchema: The overall config schema.
export interface MFESchema {
    topLevelMenuTitle: string;
    subNavigationMode: boolean;
    microFrontends: MFEConfig[];
}

const REQUIRED_FIELDS: (keyof MFEConfig)[] = ['id', 'label', 'remoteEntry', 'module', 'flags'];
const NAV_PAGE_REQUIRED_FIELDS: (keyof MFEConfig)[] = ['path', 'navIcon'];

export function getPlacement(config: MFEConfig): MFEPlacement {
    return config.placement ?? { slot: DEFAULT_MFE_SLOT };
}

export function isNavPageMfe(config: MFEConfig): boolean {
    return getPlacement(config).slot === 'nav.page';
}

function validatePlacement(placement: any, errors: string[]): void {
    if (placement === undefined) return;
    if (typeof placement !== 'object' || placement === null) {
        errors.push('[MFE Loader] placement must be an object');
        return;
    }
    if (!isMFESlotId(placement.slot)) {
        errors.push(`[MFE Loader] placement.slot must be one of the known slots; got "${placement.slot}"`);
    }
    if (
        placement.entityTypes !== undefined &&
        (!Array.isArray(placement.entityTypes) || placement.entityTypes.some((t: unknown) => typeof t !== 'string'))
    ) {
        errors.push('[MFE Loader] placement.entityTypes must be an array of strings');
    }
    if (placement.tabName !== undefined && typeof placement.tabName !== 'string') {
        errors.push('[MFE Loader] placement.tabName must be a string');
    }
}

/**
 * validateMFEConfig:
 * - Validates a single micro frontend config entry.
 * - Collects all validation errors for the entry.
 * - Returns the valid MFEConfig if no errors, otherwise returns null and logs all errors.
 */
function validateMFEConfig(config: any): MFEConfig | null {
    const errors: string[] = [];

    const slot: unknown = config?.placement?.slot;
    const requiresNavFields = slot === undefined || slot === 'nav.page';
    const requiredFields = requiresNavFields ? [...REQUIRED_FIELDS, ...NAV_PAGE_REQUIRED_FIELDS] : REQUIRED_FIELDS;

    requiredFields.forEach((field) => {
        if (config[field] === undefined || config[field] === null) {
            errors.push(`[MFE Loader] Missing required field: ${field}`);
        }
    });
    if (typeof config.id !== 'string') errors.push('[MFE Loader] id must be a string');
    if (typeof config.label !== 'string') errors.push('[MFE Loader] label must be a string');
    if (requiresNavFields && (typeof config.path !== 'string' || !config.path.startsWith('/')))
        errors.push('[MFE Loader] path must be a string starting with "/"');
    if (typeof config.remoteEntry !== 'string') errors.push('[MFE Loader] remoteEntry must be a string');
    if (typeof config.module !== 'string' || !config.module.includes('/'))
        errors.push('[MFE Loader] module must be a string with pattern "moduleName/functionName"');
    if (typeof config.flags !== 'object' || config.flags === null) errors.push('[MFE Loader] flags must be an object');
    if (config.flags) {
        if (typeof config.flags.enabled !== 'boolean') errors.push('[MFE Loader] flags.enabled must be boolean');
        if (typeof config.flags.showInNav !== 'boolean') errors.push('[MFE Loader] flags.showInNav must be boolean');
    }
    if (requiresNavFields && (typeof config.navIcon !== 'string' || !config.navIcon.length)) {
        errors.push('[MFE Loader] navIcon must be a non-empty string');
    }
    validatePlacement(config.placement, errors);

    // If any errors, log them and return null
    if (errors.length > 0) {
        console.error(`[MFE Loader] Invalid config for entry (id: ${config.id || 'unknown'}):`, errors);
        return null;
    }
    // Otherwise, return as valid MFEConfig
    return config as MFEConfig;
}

/**
 * loadMFEConfigFromYAML:
 * - Loads and parses the YAML config string.
 * - Validates each micro frontend entry, logging errors for invalid entries.
 * - Returns the parsed schema with only valid entries.
 * - Throws only if the overall YAML is malformed or missing the microFrontends array.
 */
export function loadMFEConfigFromYAML(yamlString: string): MFESchema {
    try {
        const parsed = yaml.load(yamlString) as MFESchema;
        if (!parsed || !Array.isArray(parsed.microFrontends)) {
            console.error('[MFE Loader] Invalid YAML: missing microFrontends array:', parsed);
            throw new Error('[MFE Loader] Invalid YAML: missing microFrontends array');
        }
        // Validate each entry, filtering out invalid ones
        parsed.microFrontends = parsed.microFrontends
            .map(validateMFEConfig)
            .filter((config): config is MFEConfig => config !== null);
        return parsed;
    } catch (e) {
        console.error('[MFE Loader] Error parsing YAML:', e);
        throw e;
    }
}

export type MFEConfigFetchState = {
    config: MFESchema | null;
    loading: boolean;
};

/**
 * Fetches and parses /mfe/config. Pass `skip` when a provider above already did the fetch.
 */
export function useMFEConfigFetch(skip = false): MFEConfigFetchState {
    const [config, setConfig] = useState<MFESchema | null>(null);
    const [loading, setLoading] = useState<boolean>(!skip);

    useEffect(() => {
        if (skip) return;
        async function fetchConfig() {
            try {
                const response = await fetch(resolveRuntimePath('/mfe/config'));
                if (!response.ok) throw new Error(`Failed to fetch YAML: ${response.statusText}`);
                const yamlText = await response.text();

                if (import.meta.env.DEV) {
                    console.log('[MFE Loader] Fetched YAML: ', yamlText);
                }
                const parsedConfig = loadMFEConfigFromYAML(yamlText);
                setConfig(parsedConfig);
            } catch (e) {
                console.error('[MFE Loader] Config error:', e);
                setConfig(null);
            } finally {
                setLoading(false);
            }
        }
        fetchConfig();
    }, [skip]);

    return { config, loading };
}

export function useMFEConfigFromBackend(): MFESchema | null {
    return useMFEConfigFetch().config;
}

/**
 * The shared MFE config: from `MFEConfigProvider` when one is mounted, otherwise fetched directly.
 */
export function useMFEConfig(): MFEConfigFetchState {
    const shared = useContext(MFEConfigContext);
    const standalone = useMFEConfigFetch(shared.provided);
    return shared.provided ? { config: shared.config, loading: shared.loading } : standalone;
}

export function useDynamicRoutes(): JSX.Element[] {
    const { config: mfeConfig } = useMFEConfig();
    if (!mfeConfig) return [];
    // TODO- Reintroduce useMemo() hook here. Make it work with getting yaml from api as a react hook.
    return mfeConfig.microFrontends
        .filter(isNavPageMfe)
        .map((mfe) => (
            <Route
                key={mfe.path}
                path={`/mfe${mfe.path}`}
                exact
                render={() => <MFEBaseConfigurablePage config={mfe} />}
            />
        ));
}

export const MFERoutes = () => {
    const routes = useDynamicRoutes();
    if (routes.length === 0) {
        return null;
    }
    return (
        <Switch>
            {routes}
            <Route path="/*" component={NoPageFound} />
        </Switch>
    );
};
