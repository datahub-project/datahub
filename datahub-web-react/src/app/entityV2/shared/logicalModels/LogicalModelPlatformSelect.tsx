import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { SimpleSelect } from '@src/alchemy-components';

import { useListLogicalPlatformsQuery } from '@graphql/logical.generated';

const PLATFORM_URN_PREFIX = 'urn:li:dataPlatform:';
export const LOGICAL_PLATFORM_URN = `${PLATFORM_URN_PREFIX}logical`;

/** Matches `DataPlatformInfo.name`'s `@validate.strlen.max = 15` (DataPlatformInfo.pdl). A
 * newly-typed platform name becomes both the URN id segment and `DataPlatformInfo.name` verbatim
 * (see CreateLogicalModelResolver), so this caps the id segment length client-side to fail fast. */
export const PLATFORM_NAME_MAX_LENGTH = 15;

/** The platform id segment of a platform URN (e.g. `urn:li:dataPlatform:snowflake` -> `snowflake`). */
export function platformIdFromUrn(urn: string): string {
    return urn.startsWith(PLATFORM_URN_PREFIX) ? urn.slice(PLATFORM_URN_PREFIX.length) : urn;
}

type Props = {
    label?: string;
    value: string;
    onChange: (platformUrn: string) => void;
};

/**
 * Creatable platform combobox. Only platforms marked `properties.logical` (hand-authored,
 * created for logical models) are offered as existing options — regular ingested platforms
 * (Snowflake, BigQuery, etc.) don't belong here. Typing a new name is still first-class: the
 * backend stamps any custom platform created through this flow as logical. The selected value is
 * always a platform URN.
 */
export default function LogicalModelPlatformSelect({ label, value, onChange }: Props) {
    const { t } = useTranslation('logicalModels');
    const { data } = useListLogicalPlatformsQuery();
    const [search, setSearch] = useState('');

    const logicalPlatforms = useMemo(
        () =>
            (data?.search?.searchResults ?? [])
                .map((result) => result.entity)
                .filter(
                    (entity): entity is typeof entity & { __typename: 'DataPlatform' } =>
                        !!entity && entity.__typename === 'DataPlatform',
                ),
        [data],
    );

    const options = useMemo(() => {
        const opts = logicalPlatforms.map((p) => ({
            value: p.urn,
            label: p.properties?.displayName || p.name,
        }));
        if (!opts.some((o) => o.value === LOGICAL_PLATFORM_URN)) {
            opts.unshift({ value: LOGICAL_PLATFORM_URN, label: platformIdFromUrn(LOGICAL_PLATFORM_URN) });
        }
        // Keep the current selection renderable even when it's a custom platform not in the list.
        if (value && !opts.some((o) => o.value === value)) {
            opts.push({ value, label: platformIdFromUrn(value) });
        }
        // Creatable entry: offer the typed text as a custom platform when it isn't already an option.
        const trimmed = search.trim();
        const customUrn = `${PLATFORM_URN_PREFIX}${trimmed}`;
        const alreadyOffered = opts.some(
            (o) => o.value === customUrn || o.label.toLowerCase() === trimmed.toLowerCase(),
        );
        if (trimmed && !alreadyOffered) {
            opts.push({ value: customUrn, label: t('modal.platformCustomOption', { name: trimmed }) });
        }
        return opts;
    }, [logicalPlatforms, value, search, t]);

    return (
        <SimpleSelect
            label={label}
            options={options}
            values={value ? [value] : []}
            showSearch
            showClear={false}
            width="full"
            placeholder={t('modal.platformPlaceholder')}
            onSearchChange={setSearch}
            onUpdate={(vals) => {
                setSearch('');
                onChange(vals[0] || LOGICAL_PLATFORM_URN);
            }}
            dataTestId="logical-model-platform-select"
        />
    );
}
