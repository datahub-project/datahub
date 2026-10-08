import { useUserContext } from '@app/context/useUserContext';

/**
 * Search can run once a default view has been resolved, or immediately when a view is already
 * stored. `null` means the user explicitly cleared the view. `undefined` means nothing is stored yet.
 */
export function isDefaultViewReady(hasSetDefaultView: boolean, selectedViewUrn: string | null | undefined): boolean {
    return hasSetDefaultView || selectedViewUrn !== undefined;
}

export default function useIsDefaultViewReady(): boolean {
    const { state, localState } = useUserContext();
    return isDefaultViewReady(state.views.hasSetDefaultView, localState?.selectedViewUrn);
}
