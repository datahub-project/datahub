import { Icon, Text, Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import spacing from '@components/theme/foundations/spacing';

const Wrapper = styled.div<{ $isDisabled?: boolean }>`
    display: flex;
    gap: ${spacing.xsm};
    padding: ${spacing.xsm};
    align-items: center;
    color: ${({ $isDisabled, theme }) => ($isDisabled ? theme.colors.textDisabled : theme.colors.text)};
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    text-overflow: ellipsis;
    word-wrap: nowrap;
`;

const Description = styled.div<{ $isDisabled?: boolean }>`
    color: ${({ $isDisabled, theme }) => ($isDisabled ? theme.colors.textDisabled : theme.colors.textSecondary)};
`;

const IconWrapper = styled.div<{ $isDisabled?: boolean }>`
    display: flex;
    flex-shrink: 0;
    color: ${({ $isDisabled, theme }) => ($isDisabled ? theme.colors.iconDisabled : theme.colors.icon)};
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

    const content = (
        <Wrapper $isDisabled={isDisabled}>
            <IconWrapper $isDisabled={isDisabled}>
                <Icon icon={icon} size="2xl" />
            </IconWrapper>

            <Container>
                <Text weight="semiBold">{title}</Text>
                {description && (
                    <Description $isDisabled={isDisabled}>
                        <Text size="sm">{description}</Text>
                    </Description>
                )}
            </Container>

            <SpaceFiller />

            {hasChildren && (
                <IconWrapper $isDisabled={isDisabled}>
                    <Icon icon={CaretRight} size="lg" />
                </IconWrapper>
            )}
        </Wrapper>
    );

    if (isDisabled && tooltipText) {
        return <Tooltip title={tooltipText}>{content}</Tooltip>;
    }

    return content;
}
