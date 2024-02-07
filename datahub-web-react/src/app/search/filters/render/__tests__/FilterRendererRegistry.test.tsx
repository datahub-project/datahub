import React from 'react';
import FilterRendererRegistry from '../FilterRendererRegistry';

describe('FilterRendererRegistry', () => {
    let registry;
    let renderer;
    let props;

    beforeEach(() => {
        registry = new FilterRendererRegistry();

        // Mock a FilterRenderer instance
        renderer = {
            field: 'mockField',
            render: vi.fn().mockReturnValue(<div>Rendered</div>),
            valueLabel: vi.fn().mockReturnValue(<div>ValueLabel</div>),
            icon: vi.fn().mockReturnValue(<div>Icon</div>),
        };

        // Mock FilterRenderProps
        props = {
            /* assuming some props here */
        };
    });

    describe('register', () => {
        it('should register a new FilterRenderer', () => {
            registry.register(renderer);
            expect(registry.renderers).toContain(renderer);
            expect(registry.fieldNameToRenderer.get(renderer.field)).toBe(renderer);
        });
    });

    describe('hasRenderer', () => {
        it('should return true if the renderer for a field is registered', () => {
            registry.register(renderer);
            expect(registry.hasRenderer(renderer.field)).toBe(true);
        });

        it('should return false if the renderer for a field is not registered', () => {
            expect(registry.hasRenderer('nonexistentField')).toBe(false);
        });
    });

    describe('render', () => {
        it('should return the result of the renderer render method', () => {
            registry.register(renderer);
            expect(registry.render(renderer.field, props)).toEqual(<div>Rendered</div>);
            expect(renderer.render).toHaveBeenCalledWith(props);
        });

        it('should throw an error if the renderer for a field is not registered', () => {
            expect(() => registry.render('nonexistentField', props)).toThrow();
        });
    });

    describe('getValueLabel', () => {
        it('should return the result of the renderer valueLabel method', () => {
            registry.register(renderer);
            expect(registry.getValueLabel(renderer.field, 'mockValue')).toEqual(<div>ValueLabel</div>);
            expect(renderer.valueLabel).toHaveBeenCalledWith('mockValue');
        });

        it('should throw an error if the renderer for a field is not registered', () => {
            expect(() => registry.getValueLabel('nonexistentField', 'mockValue')).toThrow();
        });
    });

    describe('getIcon', () => {
        it('should return the result of the renderer icon method', () => {
            registry.register(renderer);
            expect(registry.getIcon(renderer.field)).toEqual(<div>Icon</div>);
            expect(renderer.icon).toHaveBeenCalled();
        });

        it('should throw an error if the renderer for a field is not registered', () => {
            expect(() => registry.getIcon('nonexistentField')).toThrow();
        });
    });
});
