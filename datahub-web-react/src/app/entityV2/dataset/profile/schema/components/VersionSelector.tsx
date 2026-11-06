import { SimpleSelect, Tooltip } from '@components';
import React, { useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { toRelativeTimeString } from '@app/sharedV2/time/timeUtils';
import navigateToUrl from '@app/utils/navigateToUrl';

import { DataPlatform, SemanticVersionStruct } from '@types';

export const SEMANTIC_VERSION_PARAM = 'semantic_version';
const SIBLING_VERSION_PARAM = 'secondary_version'; // Note: Currently unused

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const SelectorWrapper = styled.div`
    margin-right: 10px;
`;

interface VersionOption {
    label: string;
    timestamp: number;
    disabled: boolean;
}

interface Props {
    versionList: Array<SemanticVersionStruct>;
    selectedVersion: string;
    platform?: DataPlatform;
    isSibling: boolean;
    isPrimary: boolean;
    minTimestamp?: number;
    maxTimestamp?: number;
    primaryVersion?: string;
}

/**
 * Note: Configured to allow for displaying two version selectors, one for each sibling.
 * Currently not used in that way.
 */
export default function VersionSelector({
    versionList,
    selectedVersion,
    platform,
    isSibling,
    isPrimary,
    minTimestamp,
    maxTimestamp,
    primaryVersion,
}: Props) {
    const { t } = useTranslation('entity.types');
    const location = useLocation();
    const history = useHistory();

    const versionOptions = useMemo(() => {
        return getVersionOptions(
            versionList,
            isPrimary ? undefined : minTimestamp,
            isPrimary ? undefined : maxTimestamp,
        );
    }, [versionList, minTimestamp, maxTimestamp, isPrimary]);

    const selectOptions = useMemo(
        () =>
            versionOptions.map((v) => ({
                value: v.label,
                label: `${v.label} - ${toRelativeTimeString(v.timestamp) || t('dataset.unknown')}`,
            })),
        [versionOptions, t],
    );
    const disabledValues = useMemo(
        () => versionOptions.filter((v) => v.disabled).map((v) => v.label),
        [versionOptions],
    );

    useEffect(() => {
        // If the selected version is disabled, navigate to the first available version
        if (isPrimary || !selectedVersion) return;
        const selectedIndex = versionOptions.findIndex((v) => v.label === selectedVersion);
        const nextOption = versionOptions.find((v) => !v.disabled);
        if (selectedIndex !== -1 && versionOptions[selectedIndex].disabled && nextOption?.label) {
            navigateToUrl({
                location,
                history,
                urlParam: isSibling ? SIBLING_VERSION_PARAM : SEMANTIC_VERSION_PARAM,
                value: nextOption.label,
            });
        }
    }, [versionOptions, selectedVersion, isSibling, isPrimary, history, location]);

    return (
        <Wrapper>
            {platform && <PlatformIcon platform={platform} size={17} />}
            <SelectorWrapper>
                <SimpleSelect
                    options={selectOptions}
                    values={[selectedVersion]}
                    disabledValues={disabledValues}
                    onUpdate={(values) => {
                        if (!values[0]) return;
                        navigateToUrl({
                            location,
                            history,
                            urlParam: isSibling ? SIBLING_VERSION_PARAM : SEMANTIC_VERSION_PARAM,
                            value: values[0],
                        });
                    }}
                    renderCustomOptionText={(option) => (
                        <Tooltip
                            title={
                                disabledValues.includes(option.value)
                                    ? t('dataset.incompatibleWithPrimaryVersion', { primaryVersion })
                                    : ''
                            }
                            placement="left"
                        >
                            {option.label}
                        </Tooltip>
                    )}
                    optionDataTestId={(option) => `sem-ver-select-button-${option.value}`}
                    dataTestId="schema-version-selector-dropdown"
                    showClear={false}
                    width="fit-content"
                />
            </SelectorWrapper>
        </Wrapper>
    );
}

function getVersionOptions(
    versionList: Array<SemanticVersionStruct>,
    minTimestamp?: number,
    maxTimestamp?: number,
): VersionOption[] {
    return versionList
        .map(
            (v) =>
                v?.semanticVersion &&
                v?.semanticVersionTimestamp && {
                    label: v.semanticVersion,
                    timestamp: v.semanticVersionTimestamp,
                    disabled:
                        v.semanticVersionTimestamp < (minTimestamp || 0) ||
                        v.semanticVersionTimestamp > (maxTimestamp || Number.POSITIVE_INFINITY),
                },
        )
        .filter((v): v is VersionOption => !!v);
}
