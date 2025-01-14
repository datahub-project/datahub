import { useSetUserPersona } from './homeV2/persona/useUserPersona';
import { useSetUserTitle } from './identity/user/useUserTitle';
import { useSetThemeIsV2 } from './useIsThemeV2';
import { useSetNavBarRedesignEnabled } from './useShowNavBarRedesign';
import { useUserTracking } from './useUserTracking';

/**
 * Component for adding SaaS-specific functionality to the app, separated to reduce merge conflicts.
 */
export default function AcrylApp({ children }: { children: JSX.Element }) {
    useSetThemeIsV2();
    useSetUserPersona();
    useSetUserTitle();
    useUserTracking();
    useSetNavBarRedesignEnabled();

    return children;
}
