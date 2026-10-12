import { Text, Tooltip } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUndeprecateResource } from '@app/entityV2/shared/EntityDropdown/useUndeprecateResource';
import DeprecationReplacement from '@app/entityV2/shared/components/styled/DeprecationReplacement';
import MarkAsDeprecatedButton from '@app/entityV2/shared/components/styled/MarkAsDeprecatedButton';
import { decommissionTimeToSeconds, toLocalDateString } from '@app/shared/time/timeUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { StructuredPopover } from '@src/alchemy-components/components/StructuredPopover';
import dayjs from '@utils/dayjs';

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
    const [showUndeprecateModal, setShowUndeprecateModal] = useState(false);
    const [hasPopoverOpened, setHasPopoverOpened] = useState(false);

    const decommissionTimeSeconds = deprecation.decommissionTime
        ? decommissionTimeToSeconds(deprecation.decommissionTime)
        : undefined;
    const decommissionTimeLocal =
        (decommissionTimeSeconds &&
            t('deprecation.scheduledDecommission', {
                date: toLocalDateString(decommissionTimeSeconds * 1000),
            })) ||
        undefined;
    const decommissionTimeGMT =
        decommissionTimeSeconds && dayjs.unix(decommissionTimeSeconds).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

    const hasDetails = deprecation.note !== '' || deprecation.decommissionTime !== null || !!deprecation.replacement;
    const isDividerNeeded = deprecation.note !== '' && deprecation.decommissionTime !== null;

    const undeprecate = useUndeprecateResource({ urn, subResource, subResourceType, refetch });

    const batchUndeprecate = () => {
        // Left open on failure so the user can retry: the hook reports the error itself.
        undeprecate().then((succeeded) => succeeded && setShowUndeprecateModal(false));
    };

    const isSubResource = subResourceType === SubResourceType.DatasetField;

    return (
        <StructuredPopover
            zIndex={zIndexOverride || 999} // set to 999 to ensure it is below the 1000 mark of the entity popover if on the entity level
            onOpenChange={(open) => open && setHasPopoverOpened(true)}
            placement={popoverPlacement}
            width={340}
            title={
                hasDetails ? (
                    <>
                        <DeprecatedTitle>
                            {isSubResource ? t('deprecation.columnDeprecated') : t('deprecation.assetDeprecated')}
                        </DeprecatedTitle>
                        {!!deprecation.replacement && (
                            <DeprecatedSubTitle>
                                <Text size="sm" weight="bold" color="text" type="div">
                                    {t('deprecation.replacementLabel')}
                                </Text>
                                <DeprecationReplacement
                                    replacement={deprecation.replacement}
                                    hasPopoverOpened={hasPopoverOpened}
                                />
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
