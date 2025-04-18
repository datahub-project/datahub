import { useMemo } from 'react';
import { ACTION_TYPES_MAP } from '../constants';

export const useIsFormDisabled = (recipe) => {
    return useMemo(() => {
        const type = recipe?.action?.type || '';
        // Check if the action is for AI term suggestion
        if (type === ACTION_TYPES_MAP.ai_term_suggestion) {
            const glossaryNodeUrns = recipe?.action?.config?.glossary_node_urns || [];
            const glossaryTermUrns = recipe?.action?.config?.glossary_term_urns || [];
            // Disable if both glossaryNodeUrns and glossaryTermUrns are empty
            return glossaryNodeUrns.length === 0 && glossaryTermUrns.length === 0;
        }
        return false; // Default is not disabled
    }, [recipe]);
};
