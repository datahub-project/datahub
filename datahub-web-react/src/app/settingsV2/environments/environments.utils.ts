import { CustomFabricType } from '@types';

// The shape of a custom environment id, as the server checks it.
const ENVIRONMENT_ID_PATTERN = /^[A-Z][A-Z0-9_]{0,63}$/;

/** The id as it will be stored: trimmed and upper-cased, like built-in environments. */
export function normalizeEnvironmentId(id: string): string {
    return id.trim().toUpperCase();
}

export function isValidEnvironmentId(id: string): boolean {
    return ENVIRONMENT_ID_PATTERN.test(normalizeEnvironmentId(id));
}

export function isDeprecated(environment: CustomFabricType): boolean {
    return !!environment.deprecation?.deprecated;
}

/** Environments matching a search query on their id or display name, case-insensitively. */
export function filterEnvironments(environments: CustomFabricType[], query: string): CustomFabricType[] {
    const needle = query.trim().toLowerCase();
    if (!needle) {
        return environments;
    }
    return environments.filter(
        (environment) =>
            environment.id.toLowerCase().includes(needle) ||
            (environment.properties?.displayName || '').toLowerCase().includes(needle),
    );
}
