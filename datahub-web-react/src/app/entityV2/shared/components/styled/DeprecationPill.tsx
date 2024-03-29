import React from 'react';
import { InfoCircleOutlined } from '@ant-design/icons';
import { Divider, message, Modal, Popover, Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import moment from 'moment';
import { Deprecation } from '../../../../../types.generated';
import { getLocaleTimezone } from '../../../../shared/time/timeUtils';
import { REDESIGN_COLORS } from '../../constants';
import { useBatchUpdateDeprecationMutation } from '../../../../../graphql/mutations.generated';

const DeprecatedContainer = styled.div`
    height: 22px;
    border: 1px solid #e37878;
    border-radius: 20px;
    display: flex;
    justify-content: center;
    align-items: center;
    color: #cd0d24;
    margin-left: 0px;
    margin-right: 8px;
    margin-top: 2px;
    padding: 4px 8px;
`;

const DeprecatedText = styled.div`
    font-size: 12px;
    text-align: center;
    color: #e37878;
    border: 0.5px;
`;

const DeprecatedTitle = styled(Typography.Text)`
    display: block;
    font-size: 14px;
    margin-bottom: 5px;
    font-weight: bold;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const DeprecatedSubTitle = styled(Typography.Text)`
    display: block;
    margin-bottom: 5px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${REDESIGN_COLORS.SUB_TEXT};
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const UndeprecatedIcon = styled(InfoCircleOutlined)`
    font-size: 14px;
    padding-right: 6px;
`;

const IconGroup = styled.div`
    font-size: 12px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    &:hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        cursor: pointer;
    }
`;

type Props = {
    urn: string;
    deprecation: Deprecation;
    refetch?: () => void;
    showUndeprecate: boolean | null;
};

export const DeprecationPill = ({ deprecation, urn, refetch, showUndeprecate }: Props) => {
    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();
    /**
     * Deprecation Decommission Timestamp
     */
    const localeTimezone = getLocaleTimezone();

    let decommissionTimeSeconds;
    if (deprecation.decommissionTime) {
        if (deprecation.decommissionTime < 943920000000) {
            // Time is set in way past if it was milli-second so considering this as set in seconds
            decommissionTimeSeconds = deprecation.decommissionTime;
        } else {
            decommissionTimeSeconds = deprecation.decommissionTime / 1000;
        }
    }
    const decommissionTimeLocal =
        (decommissionTimeSeconds &&
            `Scheduled to be decommissioned on ${moment
                .unix(decommissionTimeSeconds)
                .format('DD/MMM/YYYY')} (${localeTimezone})`) ||
        undefined;
    const decommissionTimeGMT =
        decommissionTimeSeconds && moment.unix(decommissionTimeSeconds).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

    const hasDetails = deprecation.note !== '' || deprecation.decommissionTime !== null;
    const isDividerNeeded = deprecation.note !== '' && deprecation.decommissionTime !== null;

    const batchUndeprecate = () => {
        batchUpdateDeprecationMutation({
            variables: {
                input: {
                    resources: [{ resourceUrn: urn }],
                    deprecated: false,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Marked assets as un-deprecated!', duration: 2 });
                    refetch?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to mark assets as un-deprecated: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    return (
        <Popover
            overlayStyle={{ maxWidth: 240 }}
            placement="bottom"
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
                        {isDividerNeeded && <ThinDivider />}
                        {showUndeprecate && (
                            <IconGroup
                                onClick={() =>
                                    Modal.confirm({
                                        title: `Confirm Mark as un-deprecated`,
                                        content: `Are you sure you want to mark this asset as un-deprecated?`,
                                        onOk() {
                                            batchUndeprecate();
                                        },
                                        onCancel() {},
                                        okText: 'Yes',
                                        maskClosable: true,
                                        closable: true,
                                    })
                                }
                            >
                                <UndeprecatedIcon />
                                Mark as un-deprecated
                            </IconGroup>
                        )}
                    </>
                ) : (
                    'No additional details'
                )
            }
        >
            <DeprecatedContainer>
                <DeprecatedText>Deprecated</DeprecatedText>
            </DeprecatedContainer>
        </Popover>
    );
};
