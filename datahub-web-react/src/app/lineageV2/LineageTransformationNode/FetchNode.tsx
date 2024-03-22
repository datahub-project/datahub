import { useContext } from 'react';
import { LineageDirection } from '../../../types.generated';
import { LineageEntity, LineageNodesContext } from '../common';
import useSearchAcrossLineage from '../useSearchAcrossLineage';

export default function FetchNode({ urn, direction }: LineageEntity) {
    const context = useContext(LineageNodesContext);

    // Note: Direction default is to pass typing. Should not ever be queries.
    useSearchAcrossLineage(urn, context, direction || LineageDirection.Downstream);

    return null;
}
