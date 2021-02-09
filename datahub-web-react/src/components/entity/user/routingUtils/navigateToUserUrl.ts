import { RouteComponentProps } from 'react-router-dom';
import { EntityType } from '../../../../types.generated';
import EntityRegistry from '../../EntityRegistry';

export const navigateToUserUrl = ({
    urn,
    subview,
    item,
    history,
    entityRegistry,
}: {
    urn: string;
    subview?: string;
    item?: string;
    history: RouteComponentProps['history'];
    entityRegistry: EntityRegistry;
}) => {
    history.push({
        pathname: `/${entityRegistry.getPathName(EntityType.User)}/${urn}${subview ? `/${subview}` : ''}${
            item && subview ? `/${item}` : ''
        }`,
    });
};
