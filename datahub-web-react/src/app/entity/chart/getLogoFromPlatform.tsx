import lookerLogo from '../../../images/lookerlogo.png';

/**
 * TODO: This is a temporary solution, until the backend can push logos for all data platform types.
 */
export function getLogoFromPlatform(platform: string) {
    if (platform.toLowerCase() === 'looker') {
        return lookerLogo;
    }
    return undefined;
}
