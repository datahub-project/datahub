import { Avatar, Button, EmptyState, Menu, Pagination, Text, Tooltip, toast } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { UserMinus } from '@phosphor-icons/react/dist/csr/UserMinus';
import { UserPlus } from '@phosphor-icons/react/dist/csr/UserPlus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import { ItemType } from '@components/components/Menu/types';

import { AddGroupMembersModal } from '@app/entityV2/group/AddGroupMembersModal';
import { getExternalGroupMembershipTooltip } from '@app/entityV2/group/utils';
import { scrollToTop } from '@app/shared/searchUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetAllGroupMembersQuery, useRemoveGroupMembersMutation } from '@graphql/group.generated';
import { CorpUser, EntityType } from '@types';

/**
 * Styled Components
 */
const AddMember = styled(Button)`
    padding: 13px 13px 30px 30px;
    cursor: pointer;
`;

const AddMemberText = styled(Text)`
    font-family: Mulish;
    font-style: normal;
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
`;

const MemberNameSection = styled.div`
    font-size: 20px;
    line-height: 28px;
    color: ${(props) => props.theme.colors.text};
    display: flex;
    align-items: center;
    justify-content: start;
    padding-left: 12px;
`;

const GroupMemberWrapper = styled.div`
    height: calc(100vh - 217px);
    overflow-y: auto;

    & .groupMemberRow {
        margin: 0 19px;
    }
`;

const MemberRow = styled.div`
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: center;
`;

const MemberColumn = styled.div`
    padding: 19px 0 19px 0;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const MemberEditIcon = styled.div`
    font-size: 22px;
    float: right;
`;

const Name = styled.span`
    font-weight: bold;
    font-size: 14px;
    line-height: 22px;
    color: ${(props) => props.theme.colors.text};
    margin-left: 8px;
`;

const NoGroupMembers = styled(EmptyState)`
    padding: 40px;
`;

const AddMemberRow = styled.div``;

const PaginationRow = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 15px;
`;

type Props = {
    urn: string;
    pageSize: number;
    isExternalGroup: boolean;
    externalGroupType?: string;
    onChangeMembers?: () => void;
};

export default function GroupMembers({ urn, pageSize, isExternalGroup, externalGroupType, onChangeMembers }: Props) {
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();

    const [page, setPage] = useState(1);
    /* eslint-disable @typescript-eslint/no-unused-vars */
    const [isEditingMembers, setIsEditingMembers] = useState(false);
    const start = (page - 1) * pageSize;
    const { data: membersData, refetch } = useGetAllGroupMembersQuery({
        variables: { urn, start, count: pageSize },
        fetchPolicy: 'cache-first',
    });
    const [removeGroupMembersMutation] = useRemoveGroupMembersMutation();
    const [memberToRemove, setMemberToRemove] = useState<string | null>(null);

    const onChangeMembersPage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const removeGroupMember = (userUrn: string) => {
        removeGroupMembersMutation({
            variables: {
                groupUrn: urn,
                userUrns: [userUrn],
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('group.removedMemberSuccess'), { duration: 2 });
                    // Hack to deal with eventual consistency
                    setTimeout(() => {
                        // Reload the page.
                        refetch();
                    }, 3000);
                    onChangeMembers?.();
                }
            })
            .catch((e) => {
                toast.destroy();
                toast.error(t('group.removeMemberError', { error: e.message || '' }), { duration: 3 });
            });
    };

    const onClickEditMembers = () => {
        setIsEditingMembers(true);
    };

    const onAddMembers = () => {
        setTimeout(() => {
            refetch();
        }, 3000);
        onChangeMembers?.();
    };

    const relationships = membersData && membersData.corpGroup?.relationships;
    const total = relationships?.total || 0;
    const groupMembers = relationships?.relationships?.map((rel) => rel.entity as CorpUser) || [];

    const getItems = (urnID: string): ItemType[] => {
        return [
            {
                type: 'item',
                key: 'make',
                title: t('group.makeOwner'),
                icon: UserPlus,
                disabled: true,
            },
            {
                type: 'item',
                key: 'remove',
                title: t('group.removeFromGroup'),
                icon: UserMinus,
                disabled: isExternalGroup,
                onClick: () => setMemberToRemove(urnID),
            },
        ];
    };

    return (
        <>
            <AddMemberRow>
                <Tooltip
                    showArrow={false}
                    title={isExternalGroup ? getExternalGroupMembershipTooltip(externalGroupType) : null}
                >
                    {/*
                     * Keep a hover target around the disabled button so the external-group
                     * explanation remains available.
                     */}
                    <div style={{ display: 'inline-block', cursor: isExternalGroup ? 'not-allowed' : 'auto' }}>
                        <AddMember
                            variant="text"
                            icon={{ icon: UserPlus }}
                            disabled={isExternalGroup}
                            onClick={onClickEditMembers}
                            data-testid="add-group-member-button"
                        >
                            <AddMemberText>{t('group.addMember')}</AddMemberText>
                        </AddMember>
                    </div>
                </Tooltip>
            </AddMemberRow>
            <GroupMemberWrapper>
                {groupMembers.length === 0 && <NoGroupMembers title={t('group.noMembersInGroupEmpty')} size="sm" />}
                {groupMembers
                    ? groupMembers.map((item) => {
                          const entityUrn = entityRegistry.getEntityUrl(EntityType.CorpUser, item.urn);
                          return (
                              <MemberRow className="groupMemberRow" key={entityUrn}>
                                  <MemberColumn>
                                      <Link to={entityUrn}>
                                          <MemberNameSection>
                                              <Avatar
                                                  name={entityRegistry.getDisplayName(EntityType.CorpUser, item)}
                                                  imageUrl={item.editableProperties?.pictureLink || undefined}
                                                  type={AvatarType.user}
                                              />
                                              <Name>{entityRegistry.getDisplayName(EntityType.CorpUser, item)}</Name>
                                          </MemberNameSection>
                                      </Link>
                                  </MemberColumn>
                                  <MemberColumn>
                                      <MemberEditIcon>
                                          <Menu items={getItems(item.urn)} trigger={['click']}>
                                              <Button
                                                  variant="text"
                                                  icon={{
                                                      icon: DotsThreeVertical,
                                                      weight: 'bold',
                                                      size: 'xl',
                                                      color: 'gray',
                                                  }}
                                                  isCircle
                                              />
                                          </Menu>
                                      </MemberEditIcon>
                                  </MemberColumn>
                              </MemberRow>
                          );
                      })
                    : null}
            </GroupMemberWrapper>
            <PaginationRow>
                <Pagination
                    currentPage={page}
                    itemsPerPage={pageSize}
                    total={total}
                    showLessItems
                    onPageChange={onChangeMembersPage}
                    showSizeChanger={false}
                />
            </PaginationRow>
            {isEditingMembers && (
                <AddGroupMembersModal
                    urn={urn}
                    visible={isEditingMembers}
                    onSubmit={onAddMembers}
                    onCloseModal={() => setIsEditingMembers(false)}
                />
            )}
            <ConfirmationModal
                isOpen={!!memberToRemove}
                handleClose={() => setMemberToRemove(null)}
                handleConfirm={() => removeGroupMember(memberToRemove as string)}
                modalTitle={t('group.confirmMemberRemovalTitle')}
                modalText={t('group.confirmMemberRemovalBody')}
            />
        </>
    );
}
