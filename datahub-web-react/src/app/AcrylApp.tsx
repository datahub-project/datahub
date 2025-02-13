import { useUserTracking } from './useUserTracking';

/**
 * Component for adding SaaS-specific functionality to the app, separated to reduce merge conflicts.
 */
export default function AcrylApp({ children }: { children: JSX.Element }) {
    useUserTracking();

    return children;
}
