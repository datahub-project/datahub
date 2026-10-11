import { Icon, Text, Tooltip } from '@components';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { Lock } from '@phosphor-icons/react/dist/csr/Lock';
import React from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('entity.views');
    const isPersonal = type === DataHubViewType.Personal;
    return (
        // eslint-disable-next-line jsx-a11y/click-events-have-key-events,jsx-a11y/no-static-element-interactions
        <Tooltip title={isPersonal ? t('typePrivateTooltip') : t('typePublicTooltip')}>
            <ViewNameContainer onClick={onClick}>
                {!isPersonal && <Icon icon={Globe} size="md" />}
                {isPersonal && <Icon icon={Lock} size="md" />}
                <Text type="span" color="gray" style={{ color }}>
                    {!isPersonal ? t('typePublic') : t('typePrivate')}
                </Text>
            </ViewNameContainer>
        </Tooltip>
    );
};
