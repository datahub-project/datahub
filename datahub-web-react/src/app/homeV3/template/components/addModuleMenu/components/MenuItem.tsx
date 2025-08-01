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

const IconWrapper = styled.div`
    display: flex;
    flex-shrink: 0;
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
            <IconWrapper>
                <Icon icon={icon} source="phosphor" color="gray" size="2xl" />
            </IconWrapper>

            <Container>
                <Text weight="semiBold">{title}</Text>
                {description && (
                    <Text color="gray" colorLevel={isDisabled ? 300 : 1700} size="sm">
                        {description}
                    </Text>
                )}
            </Container>

            <SpaceFiller />

            {hasChildren && <Icon icon="CaretRight" source="phosphor" color="gray" size="lg" />}
        </Wrapper>
    );

    if (isDisabled && tooltipText) {
        return <Tooltip title={tooltipText}>{content}</Tooltip>;
    }

    return content;
}
