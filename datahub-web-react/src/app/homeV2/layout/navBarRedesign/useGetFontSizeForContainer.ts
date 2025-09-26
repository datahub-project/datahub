import { useEffect, useRef, useState } from 'react';

type FontConfig = {
    fontFamily?: string;
    fontWeight?: string;
    baseFontSize?: number;
    minFontSize?: number;
    maxFontSize?: number;
};

export function useGetFontSizeForContainer(
    text: string | undefined,
    dependencies: any[] = [],
    config: FontConfig = {},
) {
    const { fontFamily = 'Mulish', fontWeight = '700', baseFontSize = 16, minFontSize = 10, maxFontSize = 16 } = config;

    const containerRef = useRef<HTMLDivElement>(null);
    const [fontSize, setFontSize] = useState(baseFontSize);

    useEffect(() => {
        if (!text || !containerRef.current) return;

        // Temporarily reset font size to base to get true container width
        const currentFontSize = containerRef.current.style.fontSize;
        containerRef.current.style.fontSize = `${baseFontSize}px`;

        // Create a temporary element to measure text width
        const temp = document.createElement('span');
        temp.style.fontSize = `${baseFontSize}px`;
        temp.style.fontWeight = fontWeight;
        temp.style.fontFamily = fontFamily;
        temp.style.visibility = 'hidden';
        temp.style.position = 'absolute';
        temp.textContent = text;
        document.body.appendChild(temp);

        const textWidth = temp.offsetWidth;
        const containerWidth = containerRef.current.offsetWidth;

        document.body.removeChild(temp);

        // Restore the font size
        containerRef.current.style.fontSize = currentFontSize;

        // Always calculate the optimal font size from scratch
        const ratio = containerWidth / textWidth;
        const calculatedSize = Math.max(minFontSize, Math.min(maxFontSize, baseFontSize * ratio));
        setFontSize(Math.floor(calculatedSize));
    }, [text, fontFamily, fontWeight, baseFontSize, minFontSize, maxFontSize, dependencies]);

    return {
        containerRef,
        fontSize,
    };
}
