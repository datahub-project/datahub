import { useIsShowNavBarRedesignEnabled } from './useAppConfig';

export function useShowNavBarRedesign() {
    const isShowNavBarRedesignEnabled = useIsShowNavBarRedesignEnabled();
    return isShowNavBarRedesignEnabled;
}
