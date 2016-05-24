function highlightResults(result, index, keyword)
{
    var content = result[index].schema;
    if (keyword)
        keyword = keyword.replace("+", "").replace("-", "");
    var query = new RegExp("(" + keyword + ")", "gim");
    var i = content.indexOf(keyword);
    var len = content.length;
    if (len > 500)
    {
        if ((len - i) < 500)
        {
            content = content.substring((len - 500), len);
        }
        else
        {
            content = content.substring(i, 500+i);
        }
    }
    var newContent = content.replace(query, "<b>$1</b>");
    result[index].schema = newContent;
    var urn = result[index].urn;
    if (urn)
    {
        var newUrn = urn.replace(query, "<b>$1</b>");
        result[index].urn = newUrn;
    }
};

App.SearchRoute = Ember.Route.extend({
    queryParams: {
        category: {
            refreshModel: true
        },
        keywords: {
            refreshModel: true
        },
        source: {
            refreshModel: true
        },
        page: {
            refreshModel: true
        }
    },
    model: function(params) {
        var searchController = this.controllerFor('search')
        searchController.set('loading', true);
        var q;
        if (params)
        {
            q = params;
        }
        else
        {
            q = convertQueryStringToObject();
        }

        var keyword = atob(q.keywords);
        var url = 'api/v1/search' + '?page=' + params.page + "&keyword=" + keyword;
        if(q.category) {
            url += ("&category=" + q.category.toLowerCase());
            currentTab = q.category.toProperCase();
            updateActiveTab();
        }
        if(q.source) {
            url += '&source=' + q.source;
        }
        $.get(url, function(data) {
            if (data && data.status == "ok") {
                var result = data.result;
                var keywords = result.keywords;
                window.g_currentCategory = result.category;
                updateSearchCategories(result.category);
                for(var index = 0; index < result.data.length; index++) {
                    var schema = result.data[index].schema;
                    if (schema) {
                        result.data[index].originalSchema = schema;
                        highlightResults(result.data, index, keyword);
                    }
                }
                searchController.set('model', result);
                searchController.set('keyword', keyword);
                searchController.set('isMetric', false);
                if (result.data.length > 0) {
                    searchController.set('showNoResult', false);
                } else {
                    searchController.set('showNoResult', true);
                }
                searchController.set('loading', false)
            }
        });
    }
});