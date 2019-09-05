import { module, test } from 'qunit';
import { RouteInfoStub } from 'wherehows-web/tests/stubs/routes/route-info';
import { resolveDynamicRouteName } from '@datahub/utils/routes/routing';
import { mapOfRouteNamesToResolver } from '@datahub/data-models/utils/entity-route-name-resolver';

module('Unit | Utility | helpers/routes', function() {
  test('resolveDynamicRouteName utility resolves RouteInfo attributes correctly', function(assert) {
    const routeInfo = new RouteInfoStub('test', {});
    let routeName = resolveDynamicRouteName(mapOfRouteNamesToResolver, routeInfo);
    assert.equal(
      routeName,
      routeInfo.name,
      'expected resolved route name to default to default to name property of RouteInfo object when routeName not in resolver map'
    );

    const testEntity = 'testBrowseEntityAttributeA';
    const routeInfos = ['browse.entity', 'browse.entity.index'].map(
      routeName => new RouteInfoStub(routeName, { entity: testEntity })
    );
    routeInfos.forEach(routeInfo => {
      const resolvedRouteName = resolveDynamicRouteName(mapOfRouteNamesToResolver, routeInfo);
      assert.equal(
        resolvedRouteName,
        `browse.${testEntity}`,
        'expected browse entity route to resolve with expected string'
      );
    });
  });
});
