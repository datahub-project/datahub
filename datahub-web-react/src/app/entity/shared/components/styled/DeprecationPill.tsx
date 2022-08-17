import { InfoCircleOutlined } from '@ant-design/icons';
import { Divider, Popover, Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import moment from 'moment';
import { Deprecation } from '../../../../../types.generated';
import { getLocaleTimezone } from '../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../constants';

const DeprecatedContainer = styled.div`
    width: 104px;
    height: 18px;
    border: 1px solid #ef5b5b;
    border-radius: 15px;
    display: flex;
    justify-content: center;
    align-items: center;
    color: #ef5b5b;
    margin-left: 0px;
    padding-top: 12px;
    padding-bottom: 12px;
`;

const DeprecatedText = styled.div`
    color: #ef5b5b;
    margin-left: 5px;
`;

const DeprecatedTitle = styled(Typography.Text)`
    display: block;
    font-size: 14px;
    margin-bottom: 5px;
    font-weight: bold;
`;

const DeprecatedSubTitle = styled(Typography.Text)`
    display: block;
    margin-bottom: 5px;
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    color: #ef5b5b;
`;

type Props = {
    deprecation: Deprecation;
    preview?: boolean | null;
};

export const DeprecationPill = ({ deprecation, preview }: Props) => {
    /**
     * Deprecation Decommission Timestamp
     */
    const localeTimezone = getLocaleTimezone();
    const decommissionTimeLocal =
        (deprecation.decommissionTime &&
            `Scheduled to be decommissioned on ${moment
                .unix(deprecation.decommissionTime)
                .format('DD/MMM/YYYY')} (${localeTimezone})`) ||
        undefined;
    const decommissionTimeGMT =
        deprecation.decommissionTime &&
        moment.unix(deprecation.decommissionTime).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

    const hasDetails = deprecation.note !== '' || deprecation.decommissionTime !== null;
    const isDividerNeeded = deprecation.note !== '' && deprecation.decommissionTime !== null;

    return (
        <Popover
            overlayStyle={{ maxWidth: 240 }}
            placement="right"
            content={
                hasDetails ? (
                    <>
                        {deprecation?.note !== '' && <DeprecatedTitle>Deprecation note</DeprecatedTitle>}
                        {isDividerNeeded && <ThinDivider />}
                        {deprecation?.note !== '' && <DeprecatedSubTitle>{deprecation.note}</DeprecatedSubTitle>}
                        {deprecation?.decommissionTime !== null && (
                            <Typography.Text type="secondary">
                                <Tooltip placement="right" title={decommissionTimeGMT}>
                                    <LastEvaluatedAtLabel>{decommissionTimeLocal}</LastEvaluatedAtLabel>
                                </Tooltip>
                            </Typography.Text>
                        )}
                    </>
                ) : (
                    'No additional details'
                )
            }
        >
            {(preview && <StyledInfoCircleOutlined />) || (
                <DeprecatedContainer>
                    <StyledInfoCircleOutlined />
                    <DeprecatedText>Deprecated</DeprecatedText>
                </DeprecatedContainer>
            )}
        </Popover>
    );
};
