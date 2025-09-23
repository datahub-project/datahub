/**
 * Gets the base path from HTML base tag (set by server) or fallback methods.
 * Priority order: 1. HTML base tag, 2. Global variable, 3. Root fallback
 */
function getBasePath(): string {
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
    if (!basePath || basePath === "" || basePath === '/') {
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
 * Fixes CSS font URLs by applying runtime base path resolution.
 * Scans existing @font-face rules and modifies absolute /assets/ paths.
 */
export function fixCSSFontPaths(): void {
    const basePath = getBasePath();

    // Only need to fix paths if we have a non-root base path
    if (!basePath) {
        return;
    }

    // Track if we made any changes
    let changesCount = 0;

    // Scan all loaded stylesheets
    Array.from(document.styleSheets).forEach((styleSheet) => {
        try {
            // Skip stylesheets from external domains due to CORS
            if (styleSheet.href && !styleSheet.href.startsWith(window.location.origin)) {
                return;
            }

            // Scan CSS rules in this stylesheet
            Array.from(styleSheet.cssRules || styleSheet.rules || []).forEach((rule: any) => {
                // Look for @font-face rules
                if (rule.type === CSSRule.FONT_FACE_RULE) {
                    const currentSrc = rule.style.src;

                    // Check if this rule has absolute /assets/ paths
                    if (currentSrc && currentSrc.includes('url("/assets/')) {
                        // Replace absolute /assets/ paths with runtime base path
                        const fixedSrc = currentSrc.replace(/url\("\/assets\//g, `url("${basePath}/assets/`);
                        console.log(`currentSrc:${  currentSrc  } fixedSrc: ${  fixedSrc}`);
                        // Apply the fix
                        if (fixedSrc !== currentSrc) {
                            rule.style.setProperty('src', fixedSrc);
                            changesCount++;
                            console.log('Fixed CSS font URL:', currentSrc, '->', fixedSrc);
                        }
                    }
                }
            });
        } catch (e) {
            // Silently skip CORS-protected or inaccessible stylesheets
            console.debug('Skipping stylesheet due to access restriction:', e);
        }
    });

    if (changesCount > 0) {
        console.log(`Successfully fixed ${changesCount} CSS font path(s) for base path: ${basePath}`);
    } else {
        console.log('No CSS font paths needed fixing (no absolute /assets/ URLs found)');
    }
}
