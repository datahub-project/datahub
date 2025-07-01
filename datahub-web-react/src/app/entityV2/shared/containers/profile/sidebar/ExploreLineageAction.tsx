import { Tooltip } from '@components';
import React, { useContext } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import CompactContext from '@src/app/shared/CompactContext';

const ActionButton = styled(Link)`
    height: 22px;
    width: 22px;
    border: 1px solid ${(props) => props.theme.styles['primary-color']};
    border-radius: 50%;
    text-align: center;
    color: ${(props) => props.theme.styles['primary-color']};

    svg {
        height: 20px;
        width: 20px;
        padding: 4px 5px 4px 4px;
    }

    :hover {
        cursor: pointer;
        color: ${REDESIGN_COLORS.WHITE};
        background: ${(props) => props.theme.styles['primary-color']};
    }
`;

interface Props {
    icon?: React.FC<any>;
}

export const ExploreLineageAction = ({ icon }: Props) => {
    const entityRegistry = useEntityRegistry();
    const isCompact = useContext(CompactContext);
    const { urn, entityType, entityData } = useEntityData();
    const entityName = (entityData && entityRegistry.getDisplayName(entityType, entityData)) || '-';

    const ButtonIcon = icon;

    return (
        <>
            {ButtonIcon && (
                <Tooltip
                    placement="left"
                    showArrow={false}
                    title={`Visually explore the upstreams and downstreams of ${entityName}`}
                >
                    <ActionButton
                        target={isCompact ? '_blank' : '_self'}
                        to={getEntityPath(entityType, urn, entityRegistry, false, false, 'Lineage')}
                    >
                        <ButtonIcon />
                    </ActionButton>
                </Tooltip>
            )}
        </>
    );
};
