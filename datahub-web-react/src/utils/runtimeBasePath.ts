/**
 * Gets the base path from HTML base tag (set by server) or fallback methods.
 * Priority order: 1. HTML base tag, 2. Global variable, 3. Root fallback
 */
function getBasePath(): string {
    // automatically return '/' as the basepath for local dev at 3000 since we rely on injecting @basepath in index.html
    if (process.env.NODE_ENV === 'development') {
        return '/';
    }
    // Check if base tag exists (set by server)
    const baseTag = document.querySelector('base');
    if (baseTag) {
        const href = baseTag.getAttribute('href');
        if (href) {
            // Remove trailing slash for consistency (except for root)
            return href === '/' ? '' : href.replace(/\/$/, '');
        }
    }

    // Check global variable
    const globalBasePath = (window as any).__DATAHUB_BASE_PATH__;
    if (globalBasePath && globalBasePath !== '/') {
        return globalBasePath.replace(/\/$/, '');
    }

    // Otherwise defaults to empty to allow existing assets anchored to /, e.g. /assets/platforms/datahublogo.png
    return '';
}

// Cache the base path on first detection
let cachedBasePath: string | null = null;

/**
 * Gets the runtime base path where the application is being served from.
 * Uses HTML base tag set by server-side template for reliable detection,
 * with config endpoint fallback support.
 */
export function getRuntimeBasePath(): string {
    if (cachedBasePath !== null) {
        return cachedBasePath;
    }
    cachedBasePath = getBasePath();
    return cachedBasePath;
}

/**
 * Resolves a path against the runtime base path
 */
export function resolveRuntimePath(path: string): string {
    const basePath = getRuntimeBasePath();

    // Handle root base path special case
    if (!basePath || basePath === '' || basePath === '/') {
        return path;
    }

    // If path already starts with our non-root base path, return as-is
    if (path.startsWith(basePath)) {
        return path;
    }

    // Remove leading slash if present to make it relative
    const cleanPath = path.startsWith('/') ? path.slice(1) : path;
    const resolvedPath = `${basePath}/${cleanPath}`.replace(/\/+/g, '/');
    return resolvedPath;
}

/**
 * Remove the base path from a given path
 */
export function removeRuntimePath(path: string): string {
    const basePath = getRuntimeBasePath();

    // Handle root base path special case
    if (!basePath || basePath === '' || basePath === '/') {
        return path;
    }

    // If path already starts with our non-root base path, return as-is
    if (!path.startsWith(basePath)) {
        return path;
    }

    return path.replace(basePath, '');
}
