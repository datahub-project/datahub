import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

interface DocumentNavigation {
    /** The document urn in the current route, or null when none is open. */
    getCurrentDocumentUrn: () => string | null;
    /**
     * Handle a click on a document row: select it in selection mode (e.g. the
     * move dialog), otherwise navigate to its entity page.
     */
    handleDocumentClick: (urn: string) => void;
}

/**
 * Routing/selection glue for the document sidebar tree. Kept out of the tree
 * component so the render code stays focused on layout, and so selection vs
 * navigation is decided in one place.
 *
 * @param onSelectDocument - When provided, clicks select rather than navigate.
 */
export function useDocumentNavigation(onSelectDocument?: (urn: string) => void): DocumentNavigation {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const getCurrentDocumentUrn = useCallback(() => {
        const match = location.pathname.match(/\/document\/([^/]+)/);
        return match ? decodeURIComponent(match[1]) : null;
    }, [location.pathname]);

    const handleDocumentClick = useCallback(
        (urn: string) => {
            if (onSelectDocument) {
                onSelectDocument(urn);
            } else {
                history.push(entityRegistry.getEntityUrl(EntityType.Document, urn));
            }
        },
        [onSelectDocument, entityRegistry, history],
    );

    return { getCurrentDocumentUrn, handleDocumentClick };
}
