import { Popover, Tooltip } from '@components';
import { Divider, Modal, Typography, message } from 'antd';
import moment from 'moment';
import React from 'react';
import styled from 'styled-components';
import { useBatchUpdateDeprecationMutation } from '../../../../../graphql/mutations.generated';
import { Deprecation, SubResourceType } from '../../../../../types.generated';
import { EntityLink } from '../../../../homeV2/reference/sections/EntityLink';
import { getV1FieldPathFromSchemaFieldUrn } from '../../../../lineageV2/lineageUtils';
import { getLocaleTimezone } from '../../../../shared/time/timeUtils';
import { REDESIGN_COLORS } from '../../constants';
import MarkAsDeprecatedButton from './MarkAsDeprecatedButton';

const DeprecatedContainer = styled.div`
    background-color: ${REDESIGN_COLORS.WHITE};
    height: 22px;
    border: 1px solid ${REDESIGN_COLORS.DEPRECATION_RED_LIGHT};
    border-radius: 20px;
    display: flex;
    justify-content: center;
    align-items: center;
    color: ${REDESIGN_COLORS.DEPRECATION_RED};
    padding: 4px 8px;
`;

const DeprecatedText = styled.div`
    font-size: 12px;
    text-align: center;
    color: ${REDESIGN_COLORS.DEPRECATION_RED_LIGHT} !important;
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
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${REDESIGN_COLORS.SUB_TEXT};
`;

const ReplacementContainer = styled.span`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    // make sure the span doesn't exceed the parent div
    max-width: 100%;
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const IconGroup = styled.div`
    padding-top: 10px;
    font-size: 12px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};

    &:hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        cursor: pointer;
    }
`;

type Props = {
    urn: string;
    subResource?: string | null;
    subResourceType?: SubResourceType;
    deprecation: Deprecation;
    refetch?: () => void;
    showUndeprecate: boolean | null;
    zIndexOverride?: number;
};

export const DeprecationPill = ({
    deprecation,
    urn,
    subResource,
    subResourceType,
    refetch,
    showUndeprecate,
    zIndexOverride,
}: Props) => {
    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();
    const localeTimezone = getLocaleTimezone(); // Deprecation Decommission Timestamp

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
                    resources: [{ resourceUrn: urn, subResource, subResourceType }],
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

    const isReplacementSchemaField = deprecation?.replacement?.urn?.startsWith('urn:li:schemaField');

    return (
        <Popover
            overlayStyle={{ maxWidth: 240 }}
            zIndex={zIndexOverride || 999} // set to 999 to ensure it is below the 1000 mark of the entity popover if on the entity level
            placement="bottom"
            content={
                hasDetails ? (
                    <>
                        {deprecation?.note !== '' && <DeprecatedTitle>Deprecation note</DeprecatedTitle>}
                        {isDividerNeeded && <ThinDivider />}
                        {deprecation.replacement && (
                            <DeprecatedSubTitle>
                                {isReplacementSchemaField ? (
                                    <>
                                        <b>Replacement: </b>
                                        <ReplacementContainer>
                                            {getV1FieldPathFromSchemaFieldUrn(deprecation.replacement.urn)}
                                        </ReplacementContainer>
                                    </>
                                ) : (
                                    <>
                                        <b>Replacement:</b> <EntityLink entity={deprecation.replacement} />
                                    </>
                                )}
                            </DeprecatedSubTitle>
                        )}
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
                                <MarkAsDeprecatedButton internalText="Mark as un-deprecated" />
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
