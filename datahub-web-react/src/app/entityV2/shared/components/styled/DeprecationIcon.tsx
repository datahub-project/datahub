import { Tooltip } from '@components';
import { Divider, Typography, message } from 'antd';
import { TooltipPlacement } from 'antd/es/tooltip';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import MarkAsDeprecatedButton from '@app/entityV2/shared/components/styled/MarkAsDeprecatedButton';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { getV1FieldPathFromSchemaFieldUrn } from '@app/lineageV2/lineageUtils';
import { toLocalDateString } from '@app/shared/time/timeUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { StructuredPopover } from '@src/alchemy-components/components/StructuredPopover';
import dayjs from '@utils/dayjs';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { Deprecation, SubResourceType } from '@types';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

const DeprecatedContainer = styled.div`
    display: flex;
    justify-content: center;
    gap: 4px;
    align-items: center;
    color: ${(props) => props.theme.colors.textError};
`;

const DeprecatedTitle = styled(Typography.Text)`
    display: block;
    font-size: 16px;
    margin-bottom: 5px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
`;

const DeprecatedSubTitle = styled(Typography.Text)`
    display: block;
    margin-bottom: 5px;
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    max-width: 100%;
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.textTertiary};
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
    color: ${(props) => props.theme.colors.text};

    &:hover {
        color: ${(props) => props.theme.styles['primary-color']};
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
    const { t } = useTranslation('entity.shared.components');
    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();
    const [showUndeprecateModal, setShowUndeprecateModal] = useState(false);

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
            t('deprecation.scheduledDecommission', {
                date: toLocalDateString(decommissionTimeSeconds * 1000),
            })) ||
        undefined;
    const decommissionTimeGMT =
        decommissionTimeSeconds && dayjs.unix(decommissionTimeSeconds).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

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
                    message.success({ content: t('deprecation.markedUnDeprecatedSuccess'), duration: 2 });
                    refetch?.();
                    analytics.event({
                        type: EventType.SetDeprecation,
                        entityUrns: [urn],
                        deprecated: false,
                        resources: subResource ? [{ resourceUrn: urn, subResource, subResourceType }] : undefined,
                    });
                }
                setShowUndeprecateModal(false);
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: t('deprecation.markUnDeprecatedError', { message: e.message || '' }),
                    duration: 3,
                });
            });
    };

    const isReplacementSchemaField = deprecation?.replacement?.urn?.startsWith('urn:li:schemaField');
    const isSubResource = subResourceType === SubResourceType.DatasetField;

    return (
        <StructuredPopover
            zIndex={zIndexOverride || 999} // set to 999 to ensure it is below the 1000 mark of the entity popover if on the entity level
            placement={popoverPlacement}
            width={340}
            title={
                hasDetails ? (
                    <>
                        <DeprecatedTitle>
                            {isSubResource ? t('deprecation.columnDeprecated') : t('deprecation.assetDeprecated')}
                        </DeprecatedTitle>
                        {deprecation.replacement && (
                            <DeprecatedSubTitle>
                                {isReplacementSchemaField ? (
                                    <>
                                        <b>{t('deprecation.replacementLabel')} </b>
                                        <ReplacementContainer>
                                            {getV1FieldPathFromSchemaFieldUrn(deprecation.replacement.urn)}
                                        </ReplacementContainer>
                                    </>
                                ) : (
                                    <>
                                        <b>{t('deprecation.replacementLabel')}</b>{' '}
                                        <EntityLink entity={deprecation.replacement} />
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
                            <IconGroup onClick={() => setShowUndeprecateModal(true)}>
                                <MarkAsDeprecatedButton internalText={t('deprecation.markAsUnDeprecated')} />
                            </IconGroup>
                        )}
                    </>
                ) : (
                    t('deprecation.noAdditionalDetails')
                )
            }
        >
            <DeprecatedContainer>
                <DeprecatedIcon />
                {showText ? t('deprecation.deprecated') : null}
                <ConfirmationModal
                    isOpen={showUndeprecateModal}
                    handleClose={() => setShowUndeprecateModal(false)}
                    handleConfirm={batchUndeprecate}
                    modalTitle={t('deprecation.confirmUnDeprecatedTitle')}
                    modalText={t('deprecation.confirmUnDeprecatedText')}
                />
            </DeprecatedContainer>
        </StructuredPopover>
    );
};
