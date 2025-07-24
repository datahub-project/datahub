import useLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useLoader';
import { ChildrenLoaderType } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

interface Props {
    parentValue: string;
    loadChildren: ChildrenLoaderType;
    loadRelatedEntities?: ChildrenLoaderType;
}

export default function ChildLoader({ parentValue, loadChildren, loadRelatedEntities }: Props) {
    useLoader(parentValue, loadChildren, loadRelatedEntities);

    return null;
}
