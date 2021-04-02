import { Dataset, RelatedDataset } from '../../../types.generated';
import { Direction } from '../types';

export default function getChildren(entity: Dataset, direction: Direction | null): Array<RelatedDataset> {
    if (direction === Direction.Upstream) {
        return entity.upstreamLineage?.upstreams || [];
    }

    if (direction === Direction.Downstream) {
        return entity.downstreamLineage?.downstreams || [];
    }

    return [];
}
