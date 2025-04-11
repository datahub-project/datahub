import { FolderOpenOutlined } from '@ant-design/icons';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container } from '@types';
import { getSubTypeIcon, TYPE_ICON_CLASS_NAME } from '../../../../components/subtypes';

const IconWrapper = styled.span`
    line-height: 0;
    .${TYPE_ICON_CLASS_NAME} {
        font-size: 14px;
    }
`;

const DefaultIcon = styled(FolderOpenOutlined)``;

interface Props {
    container: Maybe<Container | GenericEntityProperties>;
}

export default function ContainerIcon({ container }: Props): JSX.Element {
    return (
        <IconWrapper>
            <ContainerIconBase container={container} />
        </IconWrapper>
    );
}

export function ContainerIconBase({ container }: Props): JSX.Element {
    const subtype = container?.subTypes?.typeNames?.[0].toLowerCase();
    return (subtype && getSubTypeIcon(subtype)) || <DefaultIcon />;
}
