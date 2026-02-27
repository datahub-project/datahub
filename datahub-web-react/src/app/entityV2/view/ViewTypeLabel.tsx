import { Icon, Text, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { DataHubViewType } from '@types';

type Props = {
    type: DataHubViewType;
    color: string;
    onClick?: () => void;
};

const ViewNameContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

/**
 * Label used to describe View Types
 *
 * @param param0 the color of the text and iconography
 */
export const ViewTypeLabel = ({ type, color, onClick }: Props) => {
    const isPersonal = type === DataHubViewType.Personal;
    return (
        // eslint-disable-next-line jsx-a11y/click-events-have-key-events,jsx-a11y/no-static-element-interactions
        <Tooltip title={isPersonal ? 'Only visible to you' : 'Visible to everyone'}>
            <ViewNameContainer onClick={onClick}>
                {!isPersonal && <Icon source="phosphor" icon="Globe" size="md" />}
                {isPersonal && <Icon source="phosphor" icon="Lock" size="md" />}
                <Text type="span" color="gray" style={{ color }}>
                    {!isPersonal ? 'Public' : 'Private'}
                </Text>
            </ViewNameContainer>
        </Tooltip>
    );
};
