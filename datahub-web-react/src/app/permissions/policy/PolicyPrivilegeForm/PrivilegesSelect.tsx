import { SimpleSelect } from '@components';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

type Props = {
    privilegesSelectValue: string[];
    privilegeOptions: any[];
    onSelectPrivilege: (privilege: string) => void;
    onDeselectPrivilege: (privilege: string) => void;
};

const ALL_PRIVILEGES_VALUE = 'All';

const SelectContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

export default function PrivilegesSelect({
    privilegesSelectValue,
    privilegeOptions,
    onSelectPrivilege,
    onDeselectPrivilege,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const handleUpdate = useCallback(
        (next: string[]) => {
            const current = new Set(privilegesSelectValue);
            const updated = new Set(next);

            // If clearing everything, use the 'All' deselect to avoid multiple calls
            if (next.length === 0 && privilegesSelectValue.length > 0) {
                onDeselectPrivilege(ALL_PRIVILEGES_VALUE);
                return;
            }

            // If 'All' is currently selected and user clicks 'All' again, deselect all
            if (current.has(ALL_PRIVILEGES_VALUE) && updated.has(ALL_PRIVILEGES_VALUE)) {
                // Toggle: All is selected and user clicked All again -> deselect everything
                onDeselectPrivilege(ALL_PRIVILEGES_VALUE);
                return;
            }

            // If 'All' is being added when all privileges are already selected, treat it as deselect all
            if (
                updated.has(ALL_PRIVILEGES_VALUE) &&
                !current.has(ALL_PRIVILEGES_VALUE) &&
                privilegesSelectValue.length === privilegeOptions.length
            ) {
                // All individual privileges are selected, clicking 'All' means deselect all
                onDeselectPrivilege(ALL_PRIVILEGES_VALUE);
                return;
            }

            // If 'All' is being added (new selection), deselect all specific privileges first
            if (updated.has(ALL_PRIVILEGES_VALUE) && !current.has(ALL_PRIVILEGES_VALUE)) {
                // Remove all specific privileges before selecting 'All'
                current.forEach((item) => {
                    if (item !== ALL_PRIVILEGES_VALUE) {
                        onDeselectPrivilege(item);
                    }
                });
                onSelectPrivilege(ALL_PRIVILEGES_VALUE);
                return;
            }

            // If 'All' is being removed and specific privileges are being added, deselect 'All' first
            if (!updated.has(ALL_PRIVILEGES_VALUE) && current.has(ALL_PRIVILEGES_VALUE)) {
                onDeselectPrivilege(ALL_PRIVILEGES_VALUE);
                // Continue to add the specific privileges below
            }

            // Find added items
            updated.forEach((item) => {
                if (!current.has(item)) {
                    onSelectPrivilege(item);
                }
            });

            // Find removed items
            current.forEach((item) => {
                if (!updated.has(item)) {
                    onDeselectPrivilege(item);
                }
            });
        },
        [privilegesSelectValue, onSelectPrivilege, onDeselectPrivilege, privilegeOptions.length],
    );

    return (
        <SelectContainer>
            <SimpleSelect
                isMultiSelect
                width="full"
                showSearch
                dataTestId="privileges"
                values={privilegesSelectValue}
                onUpdate={handleUpdate}
                sortSelectedFirst={false}
                options={[
                    {
                        value: ALL_PRIVILEGES_VALUE,
                        label: t('privilegeForm.allPrivileges'),
                        key: 'all-privileges',
                    },
                    ...privilegeOptions.map((priv, index) => ({
                        value: priv.type,
                        label: priv.displayName,
                        key: `${priv.type}-${index}`,
                    })),
                ]}
            />
        </SelectContainer>
    );
}
