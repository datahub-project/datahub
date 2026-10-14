import { Menu } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { ContractAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ContractAction';
import { CopyLinkAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyLinkAction';
import { CopyUrnAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyUrnAction';
import { DeleteAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/DeleteAction';
import { ExternalUrlAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ExternalUrlAction';
import { useIsOnSiblingsView } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { Button } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { Assertion, AssertionRunStatus, DataContract } from '@types';

const ActionList = styled.div<{ $shouldRightAlign?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: end;
    padding: ${(props) => (props.$shouldRightAlign ? '0px' : '0px 10px')};
    margin-left: ${(props) => (props.$shouldRightAlign ? 'auto' : undefined)};
    gap: 8px;
`;

type Props = {
    assertion: Assertion;
    contract?: DataContract;
    canEditContract: boolean;
    refetch?: () => void;
    shouldRightAlign?: boolean;
};

export const AssertionListItemActions = ({
    assertion,
    contract,
    canEditContract,
    refetch,
    shouldRightAlign,
}: Props) => {
    const isSiblingsView = useIsOnSiblingsView();
    const mostRun = assertion.runEvents?.runEvents;
    const externalUrl =
        assertion?.info?.externalUrl ||
        (mostRun?.length && mostRun[0].status === AssertionRunStatus.Complete && mostRun[0].result?.externalUrl);
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    // ActionItem stops click propagation, so the menu never sees the click that antd would
    // otherwise use to close itself. Close it explicitly: delete opens a modal that would
    // otherwise sit under the still-open menu.
    const closeMenu = useCallback(() => setIsMenuOpen(false), []);

    // Each entry renders its existing action component, which carries its own tooltip,
    // permission handling and modals, rather than the default title/icon menu row.
    const menuItems: ItemType[] = [
        // Currently, we do not handle adding to a contract in siblings mode, since we only load the root node's contract.
        ...(!isSiblingsView
            ? [
                  {
                      type: 'item' as const,
                      key: 'contract',
                      title: '',
                      render: () => (
                          <ContractAction
                              assertion={assertion}
                              contract={contract}
                              canEdit={canEditContract}
                              refetch={refetch}
                              isExpandedView
                              onActionTriggered={closeMenu}
                          />
                      ),
                  },
              ]
            : []),
        ...(externalUrl
            ? [
                  {
                      type: 'item' as const,
                      key: 'external-url',
                      title: '',
                      render: () => (
                          <ExternalUrlAction assertion={assertion} isExpandedView onActionTriggered={closeMenu} />
                      ),
                  },
              ]
            : []),
        {
            type: 'item' as const,
            key: 'copy-link',
            title: '',
            render: () => <CopyLinkAction assertion={assertion} isExpandedView onActionTriggered={closeMenu} />,
        },
        {
            type: 'item' as const,
            key: 'copy-urn',
            title: '',
            render: () => <CopyUrnAction assertion={assertion} isExpandedView onActionTriggered={closeMenu} />,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: '',
            render: () => (
                <DeleteAction
                    assertion={assertion}
                    canEdit={!!assertion.dataset?.privileges?.canEditAssertions}
                    refetch={refetch}
                    isExpandedView
                    onActionTriggered={closeMenu}
                />
            ),
        },
    ];

    return (
        <ActionList onClick={(e) => e.stopPropagation()} $shouldRightAlign={shouldRightAlign}>
            <Menu items={menuItems} trigger={['click']} open={isMenuOpen} onOpenChange={setIsMenuOpen}>
                <Button
                    variant="text"
                    icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'icon' }}
                    isCircle
                    data-testid="assertion-more-options"
                />
            </Menu>
        </ActionList>
    );
};
