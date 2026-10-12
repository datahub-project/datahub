import { Icon, Text, Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import spacing from '@components/theme/foundations/spacing';

const Wrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    padding: ${spacing.xsm};
    align-items: center;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    text-overflow: ellipsis;
    word-wrap: nowrap;
`;

const IconWrapper = styled.div`
    display: flex;
    flex-shrink: 0;
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

interface Props {
    icon: React.ComponentType<any>;
    title: string;
    description?: string;
    hasChildren?: boolean;
    isDisabled?: boolean;
    isSmallModule?: boolean;
}

export default function MenuItem({ icon, title, description, hasChildren, isDisabled, isSmallModule }: Props) {
    const { t } = useTranslation('modules');
    const tooltipText = useMemo(() => {
        if (!isDisabled) return undefined;
        if (isSmallModule) {
            return t('menu.cannotAddSmallToLarge');
        }
        return t('menu.cannotAddLargeToSmall');
    }, [t, isDisabled, isSmallModule]);

    const itemColor = isDisabled ? 'textDisabled' : 'text';
    const descriptionColor = isDisabled ? 'textDisabled' : 'textSecondary';
    const iconColor = isDisabled ? 'iconDisabled' : 'icon';

    const content = (
        <Wrapper>
            <IconWrapper>
                <Icon icon={icon} color={iconColor} size="2xl" />
            </IconWrapper>

            <Container>
                <Text weight="semiBold" color={itemColor}>
                    {title}
                </Text>
                {description && (
                    <Text color={descriptionColor} size="sm">
                        {description}
                    </Text>
                )}
            </Container>

            <SpaceFiller />

            {hasChildren && <Icon icon={CaretRight} color={iconColor} size="lg" />}
        </Wrapper>
    );

    if (isDisabled && tooltipText) {
        return <Tooltip title={tooltipText}>{content}</Tooltip>;
    }

    return content;
}
