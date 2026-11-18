import { type Locator, type Page, expect } from '@playwright/test';
import { TIMEOUTS } from '../../utils/constants';

/**
 * The entity hover card (HoverEntityTooltip) that opens when the pointer rests on an entity label,
 * for example a value option in a filter picker. It is an alchemy overlay with no test id of its
 * own; the overlay carrying the entity's name is the card.
 *
 * Why it needs handling: clicking an entity label leaves the pointer on it, and about 100 ms later
 * the card opens next to the label, often over the control a test clicks next. A pointer resting
 * on the card keeps it open, so a click whose point lands under the card is intercepted for the
 * rest of the test. Park the pointer on something that opens nothing and wait for the card to
 * close before the next click.
 */
export class EntityHoverCardComponent {
  constructor(private readonly page: Page) {}

  forEntity(entityName: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- FloatingOverlay renders no test id; the overlay containing the entity name is the hover card
    return this.page.locator('[id^="alchemy-overlay-"]').filter({ hasText: entityName });
  }

  /** Move the pointer to `parkOn` and wait until the card for `entityName` is gone. */
  async dismiss(parkOn: Locator, entityName: string): Promise<void> {
    await parkOn.hover();
    await expect(this.forEntity(entityName)).toBeHidden({ timeout: TIMEOUTS.SHORT });
  }
}
