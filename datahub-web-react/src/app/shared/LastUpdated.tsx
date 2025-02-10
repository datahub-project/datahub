import { green, orange, red } from '@ant-design/colors';
import { ClockCircleOutlined } from '@ant-design/icons';
import { Image } from 'antd';
import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';
import { getLastIngestedColor } from '../entity/shared/containers/profile/sidebar/LastIngested';
import { toRelativeTimeString } from './time/timeUtils';

const LastUpdatedContainer = styled.div`
    align-items: center;
    color: ${ANTD_GRAY[7]};
    display: flex;
    flex-direction: row;
    gap: 5px;
`;

const StyledDot = styled.div<{ color: string }>`
    border-radius: 50%;
    background-color: ${(props) => props.color};
    width: 6px;
    height: 6px;
    margin-right: 4px;
    vertical-align: middle;
`;

const PopoverTitle = styled.div`
    margin-bottom: 5px;
    max-width: 250px;
`;

const PopoverContentSection = styled.div`
    align-items: center;
    display: flex;
    margin-bottom: 5px;
`;

const PreviewImage = styled(Image)`
    max-height: 9px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    padding-left: 1px;
`;

function PopoverContent() {
    return (
        <div>
            <PopoverContentSection>
                <StyledDot color={green[5]} /> Updated in the&nbsp;<b>past week</b>
            </PopoverContentSection>
            <PopoverContentSection>
                <StyledDot color={orange[5]} /> Updated in the&nbsp;<b>past month</b>
            </PopoverContentSection>
            <PopoverContentSection>
                <StyledDot color={red[5]} /> Updated&nbsp;<b>more than a month ago</b>
            </PopoverContentSection>
        </div>
    );
}

type Props = {
    time: number; // Milliseconds
    typeName?: string;
    platformName?: string;
    platformLogoUrl?: string;
    noLabel?: boolean;
};

export default function LastUpdated({ time, typeName, platformName, platformLogoUrl, noLabel }: Props) {
    return (
        <Popover
            title={
                <PopoverTitle>
                    The time that this {typeName?.toLocaleLowerCase() || 'asset'} last changed inside{' '}
                    {platformName ? (
                        <strong>
                            {platformLogoUrl && (
                                <>
                                    <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                                    &nbsp;
                                </>
                            )}
                            {platformName}
                        </strong>
                    ) : (
                        <>the source platform</>
                    )}
                </PopoverTitle>
            }
            content={PopoverContent}
            placement="bottom"
            showArrow={false}
        >
            <LastUpdatedContainer>
                <StyledDot color={getLastIngestedColor(time)} />
                {!noLabel && (
                    <>
                        <ClockCircleOutlined />
                        Updated {toRelativeTimeString(time)}
                    </>
                )}
            </LastUpdatedContainer>
        </Popover>
    );
}
