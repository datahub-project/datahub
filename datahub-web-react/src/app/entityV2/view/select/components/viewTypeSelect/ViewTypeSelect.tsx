import React from 'react';

import ViewTypeSelectV1 from '@app/entityV2/view/select/components/viewTypeSelect/ViewTypeSelectV1';
import ViewTypeSelectV2 from '@app/entityV2/view/select/components/viewTypeSelect/ViewTypeSelectV2';
import { ViewTypeSelectProps } from '@app/entityV2/view/select/components/viewTypeSelect/types';

interface Props extends ViewTypeSelectProps {
    showV2?: boolean;
}

export default function ViewTypeSelect({ showV2, ...props }: Props) {
    if (showV2) {
        return <ViewTypeSelectV2 {...props} />;
    }

    return <ViewTypeSelectV1 {...props} />;
}
