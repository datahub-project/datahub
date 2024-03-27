import { useSetUserPersona } from './homeV2/persona/useUserPersona';
import { useSetThemeIsV2 } from './useIsThemeV2Enabled';

/**
 * Component for adding SaaS-specific functionality to the app, separated to reduce merge conflicts.
 */
export default function AcrylApp({ children }: { children: JSX.Element }) {
    useSetThemeIsV2();
    useSetUserPersona();

    return children;
}
