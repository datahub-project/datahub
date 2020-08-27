import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

/**
 * Rendering options for carousel insight cards / components
 * @export
 * @interface IInsightCarouselCardProps
 * @extends {IDynamicComponent}
 */
export interface IInsightCarouselCardProps extends IDynamicComponent {
  // The options for each insight card / component
  options?: {
    // Unique number indicating the rendering position / priority for the component / card
    priority?: number;
  };
}
