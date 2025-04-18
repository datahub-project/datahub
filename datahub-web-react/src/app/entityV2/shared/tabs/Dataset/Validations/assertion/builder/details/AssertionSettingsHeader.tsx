import React from 'react';

import styled from 'styled-components';
import { Divider } from 'antd';

import { DescriptionBuilder } from './DescriptionBuilder';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: start;
    font-size: 14px;
`;

const DescriptionWrapper = styled.div`
    margin-right: 60px;
    margin-bottom: 32px;
`;

const LeftColumn = styled.div`
    flex: 1;
`;

const RightColumn = styled.div`
    padding: 0px;
`;

const StyledDivider = styled(Divider)`
    margin: 12px 0px;
`;

type Props = {
    description?: string;
    action?: React.ReactNode;
    descriptionDisabled?: boolean;
    showDivider: boolean;
    onChangeDescription: (newValue: string) => void;
};

/**
 * Assertion settings header component
 */
export const AssertionSettingsHeader = ({
    description,
    showDivider,
    action,
    descriptionDisabled,
    onChangeDescription,
}: Props) => {
    return (
        <>
            <Container>
                <LeftColumn>
                    <DescriptionWrapper>
                        <DescriptionBuilder
                            value={description}
                            onChange={onChangeDescription}
                            disabled={!!descriptionDisabled}
                        />
                    </DescriptionWrapper>
                </LeftColumn>
                <RightColumn>{action}</RightColumn>
            </Container>
            {showDivider ? <StyledDivider dashed /> : null}
        </>
    );
};
