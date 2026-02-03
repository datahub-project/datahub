import { Trash } from '@phosphor-icons/react';
import { message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { Button, Input, Modal, colors } from '@src/alchemy-components';

import { useUpdateUserAiPluginSettingsMutation } from '@graphql/aiPlugins.generated';
import { StringMapEntry } from '@types';

const ModalContent = styled.div`
    padding: 0 4px;
`;

const HeadersContainer = styled.div`
    margin-bottom: 16px;
`;

const HeaderLabelsRow = styled.div`
    display: flex;
    gap: 8px;
    margin-bottom: 8px;
`;

const HeaderLabel = styled.div`
    flex: 1;
    font-size: 12px;
    font-weight: 600;
    color: ${colors.gray[600]};
`;

const HeaderLabelSpacer = styled.div`
    width: 34px;
`;

const HeaderRow = styled.div`
    display: flex;
    gap: 8px;
    margin-bottom: 8px;
    align-items: center;
`;

const HeaderInputWrapper = styled.div`
    flex: 1;
`;

const DeleteButton = styled.button`
    background: none;
    border: none;
    padding: 8px;
    cursor: pointer;
    color: ${colors.red[500]};
    display: flex;
    align-items: center;

    &:hover {
        color: ${colors.red[700]};
    }
`;

const EmptyState = styled.div`
    font-size: 13px;
    color: ${colors.gray[400]};
    padding: 24px;
    text-align: center;
    background: ${colors.gray[1500]};
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    margin-bottom: 16px;
`;

const FooterButtons = styled.div`
    display: flex;
    gap: 8px;
    justify-content: flex-end;
    width: 100%;
`;

interface HeaderEntry {
    key: string;
    value: string;
}

interface CustomHeadersModalProps {
    open: boolean;
    pluginId: string;
    pluginName: string;
    existingHeaders: StringMapEntry[];
    onClose: () => void;
    onSaved: () => void;
}

/**
 * Modal for managing user-specific custom headers for an AI plugin.
 * These headers are sent with every request to the plugin's MCP server.
 */
const CustomHeadersModal: React.FC<CustomHeadersModalProps> = ({
    open,
    pluginId,
    pluginName,
    existingHeaders,
    onClose,
    onSaved,
}) => {
    const [headers, setHeaders] = useState<HeaderEntry[]>([]);
    const [isSaving, setIsSaving] = useState(false);
    const [updateUserSettings] = useUpdateUserAiPluginSettingsMutation();

    // Initialize headers from props when modal opens
    useEffect(() => {
        if (open) {
            const initialHeaders = existingHeaders.map((h) => ({
                key: h.key || '',
                value: h.value || '',
            }));
            setHeaders(initialHeaders.length > 0 ? initialHeaders : []);
        }
    }, [open, existingHeaders]);

    const handleAddHeader = useCallback(() => {
        setHeaders((prev) => [...prev, { key: '', value: '' }]);
    }, []);

    const handleRemoveHeader = useCallback((index: number) => {
        setHeaders((prev) => prev.filter((_, i) => i !== index));
    }, []);

    const handleUpdateHeader = useCallback((index: number, field: 'key' | 'value', newValue: string) => {
        setHeaders((prev) => prev.map((h, i) => (i === index ? { ...h, [field]: newValue } : h)));
    }, []);

    const handleSave = useCallback(async () => {
        // Filter out empty entries
        const validHeaders = headers.filter((h) => h.key.trim() && h.value.trim());

        setIsSaving(true);
        try {
            await updateUserSettings({
                variables: {
                    input: {
                        pluginId,
                        customHeaders: validHeaders.map((h) => ({ key: h.key.trim(), value: h.value.trim() })),
                    },
                },
            });
            message.success('Custom headers saved');
            onSaved();
            onClose();
        } catch (error) {
            message.error('Failed to save custom headers');
        } finally {
            setIsSaving(false);
        }
    }, [headers, pluginId, updateUserSettings, onSaved, onClose]);

    if (!open) {
        return null;
    }

    return (
        <Modal
            title={`Custom Headers - ${pluginName}`}
            subtitle="Add custom headers to send with every request to this plugin. These headers override any admin-configured headers with the same name."
            onCancel={onClose}
            width={500}
            footer={
                <FooterButtons>
                    <Button variant="outline" onClick={onClose}>
                        Cancel
                    </Button>
                    <Button variant="filled" onClick={handleSave} isLoading={isSaving}>
                        Save
                    </Button>
                </FooterButtons>
            }
        >
            <ModalContent>
                <HeadersContainer>
                    {headers.length === 0 ? (
                        <EmptyState>No custom headers configured</EmptyState>
                    ) : (
                        <>
                            <HeaderLabelsRow>
                                <HeaderLabel>Header Name</HeaderLabel>
                                <HeaderLabel>Value</HeaderLabel>
                                <HeaderLabelSpacer />
                            </HeaderLabelsRow>
                            {headers.map((header, index) => (
                                // eslint-disable-next-line react/no-array-index-key
                                <HeaderRow key={index}>
                                    <HeaderInputWrapper>
                                        <Input
                                            placeholder="x-custom-header"
                                            value={header.key}
                                            setValue={(val) => handleUpdateHeader(index, 'key', val)}
                                        />
                                    </HeaderInputWrapper>
                                    <HeaderInputWrapper>
                                        <Input
                                            placeholder="header-value"
                                            value={header.value}
                                            setValue={(val) => handleUpdateHeader(index, 'value', val)}
                                        />
                                    </HeaderInputWrapper>
                                    <DeleteButton onClick={() => handleRemoveHeader(index)} title="Remove header">
                                        <Trash size={18} />
                                    </DeleteButton>
                                </HeaderRow>
                            ))}
                        </>
                    )}
                </HeadersContainer>

                <Button variant="text" size="sm" onClick={handleAddHeader} icon={{ icon: 'Plus', source: 'phosphor' }}>
                    Add Header
                </Button>
            </ModalContent>
        </Modal>
    );
};

export default CustomHeadersModal;
