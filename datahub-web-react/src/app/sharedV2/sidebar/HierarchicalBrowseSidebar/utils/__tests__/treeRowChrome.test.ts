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

    it('mounts count for a collapsed parent (row countReveal controls visibility)', () => {
        expect(getTreeRowChromeFlags(base)).toEqual({
            canExpand: true,
            showCount: true,
        });
    });

    it('hides count when the parent is expanded', () => {
        expect(getTreeRowChromeFlags({ ...base, isExpanded: true }).showCount).toBe(false);
        expect(getTreeRowChromeFlags({ ...base, isExpanded: true }).canExpand).toBe(true);
    });

    it('hides expand and count when the sidebar is collapsed', () => {
        expect(getTreeRowChromeFlags({ ...base, isCollapsed: true })).toEqual({
            canExpand: false,
            showCount: false,
        });
    });

    it('does not expand leaves', () => {
        const flags = getTreeRowChromeFlags({
            ...base,
            hasChildren: false,
            count: undefined,
            hasToggle: false,
        });
        expect(flags.canExpand).toBe(false);
        expect(flags.showCount).toBe(false);
    });

    it('hides count when count is zero or missing', () => {
        expect(getTreeRowChromeFlags({ ...base, count: 0 }).showCount).toBe(false);
        expect(getTreeRowChromeFlags({ ...base, count: undefined }).showCount).toBe(false);
    });

    it('requires hasToggle to allow expand', () => {
        expect(getTreeRowChromeFlags({ ...base, hasToggle: false }).canExpand).toBe(false);
    });
});
