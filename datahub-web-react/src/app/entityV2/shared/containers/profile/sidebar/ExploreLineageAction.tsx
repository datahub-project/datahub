import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { REDESIGN_COLORS } from '../../../constants';
import { getEntityPath } from '../utils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';

const ActionButton = styled(Link)`
    height: 22px;
    width: 22px;
    border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    border-radius: 50%;
    text-align: center;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};

    svg {
        height: 20px;
        width: 20px;
        padding: 4px 5px 4px 4px;
    }

    :hover {
        cursor: pointer;
        color: ${REDESIGN_COLORS.WHITE};
        background: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

interface Props {
    icon?: React.FC<any>;
}

export const ExploreLineageAction = ({ icon }: Props) => {
    const entityRegistry = useEntityRegistry();
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
                    <ActionButton to={getEntityPath(entityType, urn, entityRegistry, false, false, 'Lineage')}>
                        <ButtonIcon />
                    </ActionButton>
                </Tooltip>
            )}
        </>
    );
};
