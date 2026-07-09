import { Text, Tooltip, toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import MarkAsDeprecatedButton from '@app/entityV2/shared/components/styled/MarkAsDeprecatedButton';
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

const DeprecatedTitle = styled(Text).attrs({
    size: 'lg',
    weight: 'bold',
    color: 'text',
    type: 'div',
})`
    display: block;
    margin-bottom: 5px;
`;

const DeprecatedSubTitle = styled.div`
    display: block;
    margin-bottom: 5px;
    max-width: 100%;
`;

const LastEvaluatedAtLabel = styled(Text).attrs({
    size: 'sm',
    color: 'textSecondary',
    type: 'div',
})`
    display: flex;
    align-items: center;
`;

const ReplacementContainer = styled.span`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    // make sure the span doesn't exceed the parent div
    max-width: 100%;
`;

const ThinDivider = styled.hr`
    margin: 8px 0;
    border: none;
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

const IconGroup = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};

    &:hover {
        color: ${(props) => props.theme.colors.textBrand};
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
    popoverPlacement?: React.ComponentProps<typeof StructuredPopover>['placement'];
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
                    toast.success(t('deprecation.markedUnDeprecatedSuccess'), { duration: 2 });
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
                toast.destroy();
                toast.error(t('deprecation.markUnDeprecatedError', { message: e.message || '' }), { duration: 3 });
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
                                <Text size="sm" weight="bold" color="text" type="div">
                                    {t('deprecation.replacementLabel')}
                                </Text>
                                {isReplacementSchemaField ? (
                                    <ReplacementContainer>
                                        {getV1FieldPathFromSchemaFieldUrn(deprecation.replacement.urn)}
                                    </ReplacementContainer>
                                ) : (
                                    <EntityLink entity={deprecation.replacement} />
                                )}
                            </DeprecatedSubTitle>
                        )}
                        {deprecation?.note && (
                            <DeprecatedSubTitle>
                                <Text size="sm" weight="bold" color="text" type="div">
                                    {t('deprecation.reasonLabel')}
                                </Text>
                                <Text size="md" color="text" type="p">
                                    {deprecation.note}
                                </Text>
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
                    <Text size="md" color="text" type="p">
                        {t('deprecation.noAdditionalDetails')}
                    </Text>
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
