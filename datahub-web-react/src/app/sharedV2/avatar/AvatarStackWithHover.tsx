import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarStackProps, AvatarType } from '@components/components/AvatarStack/types';
import { Badge } from '@components/components/Badge';
import { StructuredPopover } from '@components/components/StructuredPopover';
import { Text } from '@components/components/Text';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { mapRoleIcon } from '@app/identity/user/UserUtils';
import StopPropagationWrapper from '@app/sharedV2/StopPropagationWrapper';
import HoverSectionContent from '@app/sharedV2/avatar/HoverSectionContent';

const HeaderContainer = styled.div`
    display: flex;
    gap: 4px;
`;

interface Props extends AvatarStackProps {
    entityRegistry: EntityRegistry;
}

const AvatarStackWithHover = ({
    avatars,
    size = 'default',
    showRemainingNumber = true,
    maxToShow = 4,
    totalCount,
    entityRegistry,
    title,
}: Props) => {
    const { t: tc } = useTranslation('common.labels');
    const resolvedTitle = title ?? tc('owners');
    const avatarsWithIcons = avatars?.map((avatar) =>
        avatar.type === AvatarType.role ? { ...avatar, icon: avatar.icon ?? mapRoleIcon(avatar.name) } : avatar,
    );
    const users = avatarsWithIcons?.filter((avatar) => avatar.type === AvatarType.user) || [];
    const groups = avatarsWithIcons?.filter((avatar) => avatar.type === AvatarType.group) || [];
    const roles = avatarsWithIcons?.filter((avatar) => avatar.type === AvatarType.role) || [];

    const renderTitle = (headerText, count) => (
        <HeaderContainer>
            <Text size="sm" color="gray" weight="bold">
                {headerText}
            </Text>
            <Badge count={count} size="xs" />
        </HeaderContainer>
    );

    return (
        <StopPropagationWrapper>
            <StructuredPopover
                width={280}
                title={resolvedTitle}
                sections={[
                    ...(users.length > 0
                        ? [
                              {
                                  title: renderTitle(tc('users'), users.length),
                                  content: (
                                      <HoverSectionContent
                                          avatars={users}
                                          entityRegistry={entityRegistry}
                                          size={size}
                                      />
                                  ),
                              },
                          ]
                        : []),
                    ...(groups.length > 0
                        ? [
                              {
                                  title: renderTitle(tc('groups'), groups.length),
                                  content: (
                                      <HoverSectionContent
                                          avatars={groups}
                                          entityRegistry={entityRegistry}
                                          size={size}
                                          type={AvatarType.group}
                                      />
                                  ),
                              },
                          ]
                        : []),
                    ...(roles.length > 0
                        ? [
                              {
                                  title: renderTitle(tc('roles'), roles.length),
                                  content: (
                                      <HoverSectionContent
                                          avatars={roles}
                                          entityRegistry={entityRegistry}
                                          size={size}
                                          type={AvatarType.role}
                                      />
                                  ),
                              },
                          ]
                        : []),
                ]}
            >
                <div>
                    <AvatarStack
                        avatars={avatarsWithIcons}
                        showRemainingNumber={showRemainingNumber}
                        maxToShow={maxToShow}
                        totalCount={totalCount}
                    />
                </div>
            </StructuredPopover>
        </StopPropagationWrapper>
    );
};

export default AvatarStackWithHover;
