import React from 'react';
import { Empty, Typography } from 'antd';
import { Popover } from '@components';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { TooltipPlacement } from 'antd/es/tooltip';
import NoDocs from '../../../../../../images/no-docs.svg';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../constants';
import AddButton from './AddButton';

const StyledEmpty = styled(Empty)`
    display: flex;
    gap: 10px;
    align-items: center;

    .ant-empty-image {
        margin: 0;
        height: 50px;
    }
`;

const SectionWrapper = styled.div`
    border-radius: 0 0 10px 10px;
    background-color: white;
    padding: 24px;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    height: 100%;
`;

const ContentContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 12px;
`;

const LeftContainer = styled.div`
    display: flex;
`;

const RightContainer = styled.div`
    display: flex;
    align-self: flex-start;
`;

const SectionTitle = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const Description = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.GREY_500};
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    sectionName?: string;
    showButton: boolean;
    buttonLabel?: string;
    isButtonDisabled?: boolean;
    onButtonClick?: () => void;
    tooltip?: string;
    tooltipPosition?: TooltipPlacement;
}

export default function EmptyQueriesSection({
    sectionName,
    showButton = false,
    buttonLabel,
    isButtonDisabled,
    onButtonClick,
    tooltip,
    tooltipPosition,
}: Props) {
    return (
        <SectionWrapper>
            <div>
                <SectionTitle>{sectionName}</SectionTitle>
                {tooltip && (
                    <Popover content={tooltip} placement={tooltipPosition}>
                        <StyledInfoOutlined />
                    </Popover>
                )}
            </div>
            <ContentContainer>
                <LeftContainer>
                    <StyledEmpty description={<Description>No highlighted queries yet</Description>} image={NoDocs} />
                </LeftContainer>
                <RightContainer>
                    {showButton && (
                        <AddButton
                            dataTestId="add-query-button"
                            buttonLabel={buttonLabel}
                            isButtonDisabled={isButtonDisabled}
                            onButtonClick={onButtonClick}
                        />
                    )}
                </RightContainer>
            </ContentContainer>
        </SectionWrapper>
    );
}
