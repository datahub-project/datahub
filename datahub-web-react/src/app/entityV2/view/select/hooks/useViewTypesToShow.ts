import { useCallback, useState } from 'react';
import { ViewTypesFilterOption } from '../types';

export default function useViewTypesToShow() {
    const [showPublicViews, setShowPublicViews] = useState<boolean>(true);
    const [showPrivateViews, setShowPrivateViews] = useState<boolean>(true);

    const onViewsTypeChanged = useCallback((type: ViewTypesFilterOption) => {
        setShowPublicViews(['public', 'all'].includes(type));
        setShowPrivateViews(['private', 'all'].includes(type));
    }, []);

    return {
        showPublicViews,
        showPrivateViews,
        onViewsTypeChanged,
    };
}
