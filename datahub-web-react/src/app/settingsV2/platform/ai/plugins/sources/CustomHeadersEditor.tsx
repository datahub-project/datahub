import { Trash } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { CustomHeader } from '@app/settingsV2/platform/ai/plugins/utils/pluginFormState';
import { Button, Input, colors } from '@src/alchemy-components';

// ---------------------------------------------------------------------------
// Styled components
// ---------------------------------------------------------------------------

const HeaderRow = styled.div`
    display: flex;
    gap: 8px;
    align-items: flex-end;
    margin-bottom: 8px;
`;

const HeaderKeyInput = styled.div`
    flex: 1;
`;

const HeaderValueInput = styled.div`
    flex: 1;
`;

const RemoveButton = styled.div`
    padding-bottom: 8px;
    cursor: pointer;
    color: ${colors.red[500]};

    &:hover {
        color: ${colors.red[700]};
    }
`;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Props = {
    headers: CustomHeader[];
    onAdd: () => void;
    onUpdate: (headerId: string, field: 'key' | 'value', value: string) => void;
    onRemove: (headerId: string) => void;
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

/**
 * Renders editable custom header key-value pairs with add/remove controls.
 */
export function CustomHeadersEditor({ headers, onAdd, onUpdate, onRemove }: Props) {
    return (
        <>
            {headers.map((header, index) => (
                <HeaderRow key={header.id}>
                    <HeaderKeyInput>
                        <Input
                            label={index === 0 ? 'Header Name' : ''}
                            placeholder="x-custom-header"
                            value={header.key}
                            setValue={(val) => onUpdate(header.id, 'key', val)}
                        />
                    </HeaderKeyInput>
                    <HeaderValueInput>
                        <Input
                            label={index === 0 ? 'Value' : ''}
                            placeholder="header-value"
                            value={header.value}
                            setValue={(val) => onUpdate(header.id, 'value', val)}
                        />
                    </HeaderValueInput>
                    <RemoveButton onClick={() => onRemove(header.id)}>
                        <Trash size={18} />
                    </RemoveButton>
                </HeaderRow>
            ))}
            <Button
                variant="text"
                size="sm"
                onClick={onAdd}
                icon={{ icon: 'Plus', source: 'phosphor' }}
                style={{ paddingLeft: 0, paddingRight: 0 }}
            >
                Add Header
            </Button>
        </>
    );
}
