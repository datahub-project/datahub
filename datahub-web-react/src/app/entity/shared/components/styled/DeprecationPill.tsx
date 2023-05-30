import React from 'react';
import { InfoCircleOutlined } from '@ant-design/icons';
import { Divider, message, Modal, Popover, Tooltip, Typography } from 'antd';
import { blue } from '@ant-design/colors';
import styled from 'styled-components';
import moment from 'moment';
import { Deprecation } from '../../../../../types.generated';
import { getLocaleTimezone } from '../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../constants';
import { useBatchUpdateDeprecationMutation } from '../../../../../graphql/mutations.generated';

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

const UndeprecatedIcon = styled(InfoCircleOutlined)`
    font-size: 14px;
    padding-right: 6px;
`;

const IconGroup = styled.div`
    font-size: 12px;
    color: 'black';
    &:hover {
        color: ${blue[4]};
        cursor: pointer;
    }
`;

type Props = {
    urn: string;
    deprecation: Deprecation;
    preview?: boolean | null;
    refetch?: () => void;
    showUndeprecate: boolean | null;
};

export const DeprecationPill = ({ deprecation, preview, urn, refetch, showUndeprecate }: Props) => {
    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();
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
            {(preview && <StyledInfoCircleOutlined />) || (
                <DeprecatedContainer>
                    <StyledInfoCircleOutlined />
                    <DeprecatedText>Deprecated</DeprecatedText>
                </DeprecatedContainer>
            )}
        </Popover>
    );
};
