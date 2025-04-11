import { CaretDownOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import navigateToUrl from '@app/utils/navigateToUrl';
import { DataPlatform, SemanticVersionStruct } from '@types';
import { Select } from 'antd';
import { Tooltip } from '@components';
import React, { useEffect, useMemo } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

export const SEMANTIC_VERSION_PARAM = 'semantic_version';
export const SIBLING_VERSION_PARAM = 'secondary_version'; // Note: Currently unused

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const SchemaBlameSelector = styled(Select)`
    &&& .ant-select-selector {
        background: ${REDESIGN_COLORS.LIGHT_GREY};
        font-size: 14px;
        font-weight: 500;
        line-height: 24px;
        color: ${REDESIGN_COLORS.DARK_GREY};
        min-width: 30px;
        margin-right: 10px;
        border-radius: 20px;
    }
`;

const SchemaBlameSelectorOption = styled(Select.Option)`
    &&& {
        overflow: visible;
        margin-top: 6px;
        width: 100%;
    }
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
    const location = useLocation();
    const history = useHistory();

    const versionOptions = useMemo(() => {
        return getVersionOptions(
            versionList,
            isPrimary ? undefined : minTimestamp,
            isPrimary ? undefined : maxTimestamp,
        );
    }, [versionList, minTimestamp, maxTimestamp, isPrimary]);

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
            <SchemaBlameSelector
                value={selectedVersion}
                onChange={(e) => {
                    navigateToUrl({
                        location,
                        history,
                        urlParam: isSibling ? SIBLING_VERSION_PARAM : SEMANTIC_VERSION_PARAM,
                        value: e as string,
                    });
                }}
                data-testid="schema-version-selector-dropdown"
                suffixIcon={<CaretDownOutlined />}
            >
                {versionOptions.map((v) => (
                    <SchemaBlameSelectorOption
                        value={v.label}
                        data-testid={`sem-ver-select-button-${v.label}`}
                        disabled={v.disabled}
                    >
                        <Tooltip
                            title={v.disabled ? `Incompatible with primary version ${primaryVersion}` : ''}
                            placement="left"
                        >
                            {`${v.label} - ${toRelativeTimeString(v.timestamp) || 'unknown'}`}
                        </Tooltip>
                    </SchemaBlameSelectorOption>
                ))}
            </SchemaBlameSelector>
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
