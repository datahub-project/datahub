import { Icon, colors } from '@components';
import { Dropdown } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
` as typeof Icon;

interface Props {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export default function ModuleMenu({ module, position }: Props) {
    const { removeModule } = usePageTemplateContext();

    const handleDelete = useCallback(() => {
        removeModule({
            moduleUrn: module.urn,
            position,
        });
    }, [removeModule, module.urn, position]);

    return (
        <Dropdown
            trigger={['click']}
            menu={{
                items: [
                    {
                        title: 'Edit',
                        key: 'edit',
                        label: 'Edit',
                        style: {
                            color: colors.gray[600],
                            fontSize: '14px',
                        },
                    },
                    {
                        title: 'Delete',
                        label: 'Delete',
                        key: 'delete',
                        style: {
                            color: colors.red[500],
                            fontSize: '14px',
                        },
                        onClick: handleDelete,
                    },
                ],
            }}
        >
            <StyledIcon icon="DotsThreeVertical" source="phosphor" size="lg" />
        </Dropdown>
    );
}
