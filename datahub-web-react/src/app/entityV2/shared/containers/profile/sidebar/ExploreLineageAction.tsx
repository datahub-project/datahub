import React, { useCallback } from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { useHistory } from 'react-router';
import { REDESIGN_COLORS } from '../../../constants';
import { getEntityPath } from '../utils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';

const ActionButton = styled.div`
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
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { urn, entityType, entityData } = useEntityData();
    const entityName = (entityData && entityRegistry.getDisplayName(entityType, entityData)) || '-';

    const routeToLineage = useCallback(() => {
        history.push(getEntityPath(entityType, urn, entityRegistry, true, false, 'Lineage'));
    }, [history, entityType, urn, entityRegistry]);

    const ButtonIcon = icon;

    return (
        <>
            {ButtonIcon && (
                <Tooltip
                    placement="left"
                    showArrow={false}
                    title={`Visually explore the upstreams and downstreams of ${entityName}`}
                >
                    <ActionButton onClick={routeToLineage}>
                        <ButtonIcon />
                    </ActionButton>
                </Tooltip>
            )}
        </>
    );
};
