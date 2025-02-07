import { useState } from 'react';

export default function useEntityState() {
    const [shouldRefetchContents, setShouldRefetchContents] = useState(false);

    return { shouldRefetchContents, setShouldRefetchContents };
}
