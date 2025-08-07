import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { Carousel } from '@components/components/Carousel/Carousel';

// Test slides component
const TestSlide = ({ children }: { children: React.ReactNode }) => <div data-testid="test-slide">{children}</div>;

describe('Carousel', () => {
    describe('Basic functionality', () => {
        it('renders with default props', () => {
            const { container } = render(
                <Carousel>
                    <TestSlide>Slide 1</TestSlide>
                </Carousel>,
            );

            const carousel = container.querySelector('.slick-slider');
            expect(carousel).toBeInTheDocument();
        });

        it('renders right component when provided', () => {
            const RightComponent = () => (
                <button type="button" data-testid="right-component">
                    Next
                </button>
            );

            render(
                <Carousel rightComponent={<RightComponent />}>
                    <TestSlide>Slide 1</TestSlide>
                </Carousel>,
            );

            expect(screen.getByTestId('right-component')).toBeInTheDocument();
        });

        it('renders left component when provided', () => {
            const LeftComponent = () => (
                <button type="button" data-testid="left-component">
                    Previous
                </button>
            );

            render(
                <Carousel leftComponent={<LeftComponent />}>
                    <TestSlide>Slide 1</TestSlide>
                </Carousel>,
            );

            expect(screen.getByTestId('left-component')).toBeInTheDocument();
        });
    });

    describe('Animation props without rendering styled errors', () => {
        it('accepts animateDot and dotDuration props', () => {
            const { container } = render(
                <Carousel animateDot={false} dotDuration={3000}>
                    <TestSlide>Slide 1</TestSlide>
                </Carousel>,
            );

            const carousel = container.querySelector('.slick-slider');
            expect(carousel).toBeInTheDocument();
        });

        it('works without animation props', () => {
            const { container } = render(
                <Carousel>
                    <TestSlide>Slide 1</TestSlide>
                </Carousel>,
            );

            const carousel = container.querySelector('.slick-slider');
            expect(carousel).toBeInTheDocument();
        });

        it('passes through standard Ant Design Carousel props', () => {
            const { container } = render(
                <Carousel autoplay autoplaySpeed={2000} arrows dots={false}>
                    <TestSlide>Slide 1</TestSlide>
                </Carousel>,
            );

            const carousel = container.querySelector('.slick-slider');
            expect(carousel).toBeInTheDocument();
        });

        it('combines all props together', () => {
            const { container } = render(
                <Carousel autoplay={false} dots animateDot={false} dotDuration={4000} arrows={false}>
                    <TestSlide>Slide 1</TestSlide>
                    <TestSlide>Slide 2</TestSlide>
                </Carousel>,
            );

            const carousel = container.querySelector('.slick-slider');
            expect(carousel).toBeInTheDocument();
        });
    });

    describe('Interface coverage', () => {
        it('handles all animateDot prop variations', () => {
            // Test true value
            const { container: container1 } = render(
                <Carousel animateDot dotDuration={0}>
                    <TestSlide>Test</TestSlide>
                </Carousel>,
            );
            expect(container1.querySelector('.slick-slider')).toBeInTheDocument();

            // Test false value
            const { container: container2 } = render(
                <Carousel animateDot={false}>
                    <TestSlide>Test</TestSlide>
                </Carousel>,
            );
            expect(container2.querySelector('.slick-slider')).toBeInTheDocument();

            // Test undefined (default)
            const { container: container3 } = render(
                <Carousel dotDuration={5000}>
                    <TestSlide>Test</TestSlide>
                </Carousel>,
            );
            expect(container3.querySelector('.slick-slider')).toBeInTheDocument();
        });

        it('handles all dotDuration prop variations', () => {
            // Test positive number
            const { container: container1 } = render(
                <Carousel dotDuration={1000}>
                    <TestSlide>Test</TestSlide>
                </Carousel>,
            );
            expect(container1.querySelector('.slick-slider')).toBeInTheDocument();

            // Test zero
            const { container: container2 } = render(
                <Carousel dotDuration={0}>
                    <TestSlide>Test</TestSlide>
                </Carousel>,
            );
            expect(container2.querySelector('.slick-slider')).toBeInTheDocument();

            // Test undefined (default)
            const { container: container3 } = render(
                <Carousel animateDot>
                    <TestSlide>Test</TestSlide>
                </Carousel>,
            );
            expect(container3.querySelector('.slick-slider')).toBeInTheDocument();
        });
    });
});
