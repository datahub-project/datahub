import React from 'react';
import styled from 'styled-components';
import { colors, Tooltip } from '@components';
import { Divider, Modal, Typography, message } from 'antd';
import moment from 'moment';
import { TooltipPlacement } from 'antd/es/tooltip';
import { Tooltip2 } from '@src/alchemy-components/components/Tooltip2';
import CompactMarkdownViewer from '@src/app/entity/shared/tabs/Documentation/components/CompactMarkdownViewer';
import DeprecatedIcon from '../../../../../images/deprecated-status.svg?react';
import { useBatchUpdateDeprecationMutation } from '../../../../../graphql/mutations.generated';
import { Deprecation, SubResourceType } from '../../../../../types.generated';
import { EntityLink } from '../../../../homeV2/reference/sections/EntityLink';
import { getV1FieldPathFromSchemaFieldUrn } from '../../../../lineageV2/lineageUtils';
import { toLocalDateString } from '../../../../shared/time/timeUtils';
import { REDESIGN_COLORS } from '../../constants';
import MarkAsDeprecatedButton from './MarkAsDeprecatedButton';

const DeprecatedContainer = styled.div`
    display: flex;
    justify-content: center;
    gap: 4px;
    align-items: center;
    color: ${REDESIGN_COLORS.DEPRECATION_RED};
`;

const DeprecatedTitle = styled(Typography.Text)`
    display: block;
    font-size: 16px;
    margin-bottom: 5px;
    font-weight: bold;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const DeprecatedSubTitle = styled(Typography.Text)`
    display: block;
    margin-bottom: 5px;
    font-size: 12px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    max-width: 100%;
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${colors.gray[1800]};
    font-size: 14px;
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

const StyledDeprecatedIcon = styled(DeprecatedIcon)`
    color: inherit;
    path {
        fill: currentColor;
    }
    && {
        fill: currentColor;
    }
`;

type Props = {
    urn: string;
    subResource?: string | null;
    subResourceType?: SubResourceType;
    deprecation: Deprecation;
    refetch?: () => void;
    showUndeprecate: boolean | null;
    showText?: boolean;
    zIndexOverride?: number;
    popoverPlacement?: TooltipPlacement;
};

export const DeprecationIcon = ({
    deprecation,
    urn,
    subResource,
    subResourceType,
    refetch,
    showUndeprecate,
    zIndexOverride,
    showText = true,
    popoverPlacement = 'bottom',
}: Props) => {
    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();

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
            `Scheduled to be decommissioned on ${toLocalDateString(decommissionTimeSeconds * 1000)}`) ||
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
    const entityTypeDisplayName = subResourceType === SubResourceType.DatasetField ? 'column' : 'asset';

    return (
        <Tooltip2
            zIndex={zIndexOverride || 999} // set to 999 to ensure it is below the 1000 mark of the entity popover if on the entity level
            placement={popoverPlacement}
            width={340}
            title={
                hasDetails ? (
                    <>
                        <DeprecatedTitle>This {entityTypeDisplayName} is deprecated</DeprecatedTitle>
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
                        {deprecation?.note && (
                            <DeprecatedSubTitle>
                                <CompactMarkdownViewer content={deprecation.note} />
                            </DeprecatedSubTitle>
                        )}
                        {deprecation?.decommissionTime !== null && (
                            <Tooltip placement="right" title={decommissionTimeGMT}>
                                <LastEvaluatedAtLabel>{decommissionTimeLocal}</LastEvaluatedAtLabel>
                            </Tooltip>
                        )}
                        {isDividerNeeded && showUndeprecate ? <ThinDivider /> : null}
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
                <StyledDeprecatedIcon />
                {showText ? 'Deprecated' : null}
            </DeprecatedContainer>
        </Tooltip2>
    );
};
