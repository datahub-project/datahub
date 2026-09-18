import { isPropagated } from '@app/entity/shared/propagation/utils';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { PropagationContext } from '@app/sharedV2/tags/usePropagationContextEntities';

export function parsePropagationContext(context?: string | null): PropagationContext | null {
    if (!context) return null;
    try {
        return JSON.parse(context) as PropagationContext;
    } catch (e) {
        console.warn('Failed to parse propagation context as JSON:', context, e);
        return null;
    }
}

/**
 * Whether `HoverCardAttributionDetails` will render anything.
 *
 * Attribution rides along with most associations, so the mere presence of `propagationDetails`
 * says nothing — only propagated ones produce output. Callers that reserve layout space for the
 * section must agree with this predicate, or they reserve space for a component that renders null.
 */
export function hasPropagationDetails(propagationDetails?: AttributionDetails): boolean {
    return (
        isPropagated(propagationDetails?.attribution?.sourceDetail) ||
        !!parsePropagationContext(propagationDetails?.context)?.propagated
    );
}
