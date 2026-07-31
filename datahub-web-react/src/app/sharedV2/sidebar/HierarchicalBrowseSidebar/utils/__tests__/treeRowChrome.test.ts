import {
    getTreeRowChromeFlags,
    getTreeRowPaddingLeft,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/treeRowChrome';

describe('getTreeRowPaddingLeft', () => {
    it('uses 8px base plus 16px per level', () => {
        expect(getTreeRowPaddingLeft(0)).toBe(8);
        expect(getTreeRowPaddingLeft(1)).toBe(24);
        expect(getTreeRowPaddingLeft(2)).toBe(40);
    });

    it('clamps negative levels to the base indent', () => {
        expect(getTreeRowPaddingLeft(-1)).toBe(8);
    });
});

describe('getTreeRowChromeFlags', () => {
    const base = {
        isCollapsed: false,
        hasChildren: true,
        isExpanded: false,
        count: 3,
        hasToggle: true,
    };

    it('shows caret and count for a collapsed parent', () => {
        expect(getTreeRowChromeFlags(base)).toEqual({
            canExpand: true,
            showCount: true,
            showRightChrome: true,
            reserveCaretSlot: true,
        });
    });

    it('hides count when the parent is expanded', () => {
        expect(getTreeRowChromeFlags({ ...base, isExpanded: true }).showCount).toBe(false);
        expect(getTreeRowChromeFlags({ ...base, isExpanded: true }).canExpand).toBe(true);
    });

    it('hides caret and count when the sidebar is collapsed', () => {
        expect(getTreeRowChromeFlags({ ...base, isCollapsed: true })).toEqual({
            canExpand: false,
            showCount: false,
            showRightChrome: false,
            reserveCaretSlot: false,
        });
    });

    it('reserves caret slot for leaves so icons stay aligned', () => {
        const flags = getTreeRowChromeFlags({
            ...base,
            hasChildren: false,
            count: undefined,
            hasToggle: false,
        });
        expect(flags.canExpand).toBe(false);
        expect(flags.showCount).toBe(false);
        expect(flags.reserveCaretSlot).toBe(true);
        expect(flags.showRightChrome).toBe(true);
    });

    it('hides count when count is zero or missing', () => {
        expect(getTreeRowChromeFlags({ ...base, count: 0 }).showCount).toBe(false);
        expect(getTreeRowChromeFlags({ ...base, count: undefined }).showCount).toBe(false);
    });

    it('requires hasToggle to show the caret', () => {
        expect(getTreeRowChromeFlags({ ...base, hasToggle: false }).canExpand).toBe(false);
    });
});
