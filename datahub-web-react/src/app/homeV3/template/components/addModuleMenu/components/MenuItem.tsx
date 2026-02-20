import { Icon, IconNames, Text, Tooltip } from '@components';
import React, { useMemo } from 'react';
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

const IconWrapper = styled.div<{ $isDisabled?: boolean }>`
    display: flex;
    flex-shrink: 0;
    color: ${(props) => (props.$isDisabled ? props.theme.colors.textDisabled : props.theme.colors.textTertiary)};
`;

const CaretWrapper = styled(IconWrapper)``;

const DescriptionText = styled(Text)<{ $isDisabled?: boolean }>`
    color: ${(props) => (props.$isDisabled ? props.theme.colors.textDisabled : props.theme.colors.textSecondary)};
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

interface Props {
    icon: IconNames;
    title: string;
    description?: string;
    hasChildren?: boolean;
    isDisabled?: boolean;
    isSmallModule?: boolean;
}

export default function MenuItem({ icon, title, description, hasChildren, isDisabled, isSmallModule }: Props) {
    const tooltipText = useMemo(() => {
        if (!isDisabled) return undefined;
        if (isSmallModule) {
            return 'Cannot add small widget to large widget row';
        }
        return 'Cannot add large widget to small widget row';
    }, [isDisabled, isSmallModule]);

    const content = (
        <Wrapper>
            <IconWrapper $isDisabled={isDisabled}>
                <Icon icon={icon} source="phosphor" size="2xl" />
            </IconWrapper>

            <Container>
                <Text weight="semiBold">{title}</Text>
                {description && (
                    <DescriptionText $isDisabled={isDisabled} size="sm">
                        {description}
                    </DescriptionText>
                )}
            </Container>

            <SpaceFiller />

            {hasChildren && (
                <CaretWrapper $isDisabled={isDisabled}>
                    <Icon icon="CaretRight" source="phosphor" size="lg" />
                </CaretWrapper>
            )}
        </Wrapper>
    );

    if (isDisabled && tooltipText) {
        return <Tooltip title={tooltipText}>{content}</Tooltip>;
    }

    return content;
}
