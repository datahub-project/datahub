import { Icon, colors } from '@components';
import { Dropdown } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DEFAULT_GLOBAL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
` as typeof Icon;

const DropdownWrapper = styled.div``;

const StyledDropdownContainer = styled.div`
    .ant-dropdown-menu {
        border-radius: 12px;
    }
`;

interface Props {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export default function ModuleMenu({ module, position }: Props) {
    const { type } = module.properties;
    const canEdit = !DEFAULT_GLOBAL_MODULE_TYPES.includes(type);

    const {
        removeModule,
        moduleModalState: { openToEdit },
    } = usePageTemplateContext();

    const handleEditModule = useCallback(() => {
        openToEdit(type, module, position);
    }, [module, openToEdit, type, position]);

    const handleRemove = useCallback(() => {
        removeModule({
            module,
            position,
        });
    }, [removeModule, module, position]);

    const handleMenuClick = useCallback((e: React.MouseEvent) => {
        e.stopPropagation();
    }, []);

    const menuItemStyle = { fontSize: '14px', padding: '5 16px' };

    return (
        <DropdownWrapper onClick={handleMenuClick}>
            <Dropdown
                trigger={['click']}
                dropdownRender={(originNode) => <StyledDropdownContainer>{originNode}</StyledDropdownContainer>}
                menu={{
                    items: [
                        ...(canEdit
                            ? [
                                  {
                                      title: 'Edit',
                                      key: 'edit',
                                      label: 'Edit',
                                      style: {
                                          ...menuItemStyle,
                                          color: colors.gray[600],
                                      },
                                      onClick: handleEditModule,
                                  },
                              ]
                            : []),
                        {
                            title: 'Remove',
                            label: 'Remove',
                            key: 'remove',
                            style: {
                                ...menuItemStyle,
                                color: colors.red[500],
                            },
                            onClick: handleRemove,
                        },
                    ],
                }}
            >
                <StyledIcon icon="DotsThreeVertical" source="phosphor" size="lg" />
            </Dropdown>
        </DropdownWrapper>
    );
}
