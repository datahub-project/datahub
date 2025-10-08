import React, { useEffect, useCallback } from 'react';
import styled from 'styled-components';
import { Modal, Text, Heading, Tag } from '@components';
import { Button as DataHubButton } from '@components';


interface KeyboardShortcut {
  key: string;
  description: string;
  category: 'navigation' | 'editing' | 'actions' | 'modals';
}

interface KeyboardShortcutsProps {
  visible: boolean;
  onClose: () => void;
  onSearch: () => void;
  onImport: () => void;
  onRefresh: () => void;
  onSelectAll: () => void;
  onEditMode: () => void;
}

const ModalContainer = styled.div`
  .ant-modal-content {
    border-radius: 8px;
    overflow: hidden;
  }
  
  .ant-modal-header {
    border-bottom: 1px solid #e5e7eb;
    padding: 16px 24px;
  }
  
  .ant-modal-body {
    padding: 24px;
    max-height: 70vh;
    overflow-y: auto;
  }
  
  .ant-modal-footer {
    border-top: 1px solid #e5e7eb;
    padding: 16px 24px;
    background: #f9fafb;
  }
`;

const ModalHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
`;

const ModalTitle = styled(Title)`
  margin: 0 !important;
  font-size: 18px !important;
  font-weight: 600 !important;
  color: #111827 !important;
`;

const CloseButton = styled(DataHubButton)`
  border: none;
  background: transparent;
  color: #6b7280;
  
  &:hover {
    background: #f3f4f6;
    color: #374151;
  }
`;

const CategoryContainer = styled.div`
  margin-bottom: 24px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;

const CategoryTitle = styled(Title)`
  margin: 0 0 12px 0 !important;
  font-size: 16px !important;
  font-weight: 600 !important;
  color: #111827 !important;
`;

const ShortcutItem = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
  border-bottom: 1px solid #f3f4f6;
  
  &:last-child {
    border-bottom: none;
  }
`;

const ShortcutDescription = styled(Text)`
  color: #374151;
  font-size: 14px;
`;

const ShortcutKey = styled(Tag)`
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 12px;
  background: #f3f4f6;
  color: #374151;
  border: 1px solid #d1d5db;
`;

const FooterActions = styled.div`
  display: flex;
  justify-content: flex-end;
  width: 100%;
`;

const shortcuts: KeyboardShortcut[] = [
  // Navigation
  { key: 'Ctrl + /', description: 'Show keyboard shortcuts', category: 'navigation' },
  { key: 'Ctrl + K', description: 'Focus search bar', category: 'navigation' },
  { key: 'Escape', description: 'Close modals and dialogs', category: 'navigation' },
  { key: 'Tab', description: 'Navigate between elements', category: 'navigation' },
  
  // Editing
  { key: 'Enter', description: 'Save current edit', category: 'editing' },
  { key: 'Escape', description: 'Cancel current edit', category: 'editing' },
  { key: 'Ctrl + E', description: 'Toggle edit mode', category: 'editing' },
  
  // Actions
  { key: 'Ctrl + A', description: 'Select all items', category: 'actions' },
  { key: 'Ctrl + I', description: 'Start import process', category: 'actions' },
  { key: 'F5', description: 'Refresh data', category: 'actions' },
  { key: 'Ctrl + R', description: 'Refresh data', category: 'actions' },
  
  // Modals
  { key: 'Enter', description: 'Confirm modal actions', category: 'modals' },
  { key: 'Escape', description: 'Cancel modal actions', category: 'modals' },
];

export const KeyboardShortcuts: React.FC<KeyboardShortcutsProps> = ({
  visible,
  onClose,
  onSearch,
  onImport,
  onRefresh,
  onSelectAll,
  onEditMode,
}) => {
  const handleKeyDown = useCallback((event: KeyboardEvent) => {
    // Don't handle shortcuts when modals are open or inputs are focused
    if (visible || document.activeElement?.tagName === 'INPUT' || document.activeElement?.tagName === 'TEXTAREA') {
      return;
    }

    const { ctrlKey, key, metaKey } = event;
    const isCtrl = ctrlKey || metaKey;

    switch (key.toLowerCase()) {
      case '/':
        if (isCtrl) {
          event.preventDefault();
          // Show shortcuts modal
        }
        break;
      case 'k':
        if (isCtrl) {
          event.preventDefault();
          onSearch();
        }
        break;
      case 'a':
        if (isCtrl) {
          event.preventDefault();
          onSelectAll();
        }
        break;
      case 'i':
        if (isCtrl) {
          event.preventDefault();
          onImport();
        }
        break;
      case 'r':
        if (isCtrl) {
          event.preventDefault();
          onRefresh();
        }
        break;
      case 'e':
        if (isCtrl) {
          event.preventDefault();
          onEditMode();
        }
        break;
      case 'f5':
        event.preventDefault();
        onRefresh();
        break;
      case 'escape':
        onClose();
        break;
    }
  }, [visible, onSearch, onImport, onRefresh, onSelectAll, onEditMode, onClose]);

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  const groupedShortcuts = shortcuts.reduce((acc, shortcut) => {
    if (!acc[shortcut.category]) {
      acc[shortcut.category] = [];
    }
    acc[shortcut.category].push(shortcut);
    return acc;
  }, {} as Record<string, KeyboardShortcut[]>);

  const categoryLabels = {
    navigation: 'Navigation',
    editing: 'Editing',
    actions: 'Actions',
    modals: 'Modals',
  };

  return (
    <Modal
      open={visible}
      onCancel={onClose}
      width={500}
      footer={null}
      closable={false}
      destroyOnClose
    >
      <ModalContainer>
        <ModalHeader>
          <ModalTitle level={4}>
            Keyboard Shortcuts
          </ModalTitle>
          <CloseButton
            variant="text"
            icon={{ icon: 'X', source: 'phosphor' }}
            onClick={onClose}
          />
        </ModalHeader>

        {Object.entries(groupedShortcuts).map(([category, categoryShortcuts]) => (
          <CategoryContainer key={category}>
            <CategoryTitle level={5}>
              {categoryLabels[category as keyof typeof categoryLabels]}
            </CategoryTitle>
            {categoryShortcuts.map((shortcut, index) => (
              <ShortcutItem key={index}>
                <ShortcutDescription>
                  {shortcut.description}
                </ShortcutDescription>
                <ShortcutKey>
                  {shortcut.key}
                </ShortcutKey>
              </ShortcutItem>
            ))}
          </CategoryContainer>
        ))}

        <FooterActions>
          <DataHubButton
            variant="filled"
            color="blue"
            onClick={onClose}
          >
            Close
          </DataHubButton>
        </FooterActions>
      </ModalContainer>
    </Modal>
  );
};
