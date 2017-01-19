function testPagedFlows(assert)
{
    var done = assert.async();
    var page = 1;
    var size = 10;
    var url = '/api/v1/flows?size=' + size + '&page=' + page;

    $.get( url, function( result ) {
        assert.equal(result.status, 'ok', "Called getPagedFlows check status");
        assert.equal(result.data.page, '1', "Called getPagedFlows check if return correct page.");
        assert.ok(result.data.count > 0, "Total " + result.data.count + " flows returned.");
        done();
    });
}

QUnit.test( "flows test", function( assert ) {
    testPagedFlows(assert);
});
