import { RouteComponentProps } from 'react-router-dom';
import { EntityType } from '../../../../types.generated';
import EntityRegistry from '../../../entity/EntityRegistry';

export const navigateToSubviewUrl = ({
    urn,
    subview,
    item,
    history,
    entityRegistry,
    entityType,
}: {
    urn: string;
    subview?: string;
    item?: string;
    history: RouteComponentProps['history'];
    entityRegistry: EntityRegistry;
    entityType: EntityType;
}) => {
    history.push({
        pathname: `/${entityRegistry.getPathName(entityType)}/${urn}${subview ? `/${subview}` : ''}${
            item && subview ? `/${item}` : ''
        }`,
    });
};
