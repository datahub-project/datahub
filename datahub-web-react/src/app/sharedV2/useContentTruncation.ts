import { useEffect, useRef, useState } from 'react';

export default function useParentContainersTruncation(dataDependency: any) {
    const contentRef = useRef<HTMLDivElement>(null);
    const [isContentTruncated, setAreContainersTruncated] = useState(false);

    useEffect(() => {
        if (contentRef && contentRef.current && contentRef.current.scrollWidth > contentRef.current.clientWidth) {
            setAreContainersTruncated(true);
        }
    }, [dataDependency]);

    return { contentRef, isContentTruncated };
}
