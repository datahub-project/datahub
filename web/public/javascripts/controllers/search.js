App.SearchController = Ember.Controller.extend({
    queryParams: [
        'keywords',
        'category',
        'source',
        'page'
    ],
    keywords: null,
    category: null,
    source: null,
    page: null,
    loading: true,
    showNoResult: false,
    isMetric: false,
    previousPage: function(){
        var model = this.get("model");
        if (model && model.page) {
            var currentPage = model.page;
            if (currentPage <= 1) {
                return currentPage;
            }
            else {
                return currentPage - 1;
            }
        } else {
            return 1;
        }

    }.property('model.page'),
    nextPage: function(){
        var model = this.get("model");
        if (model && model.page) {
            var currentPage = model.page;
            var totalPages = model.totalPages;
            if (currentPage >= totalPages) {
                return totalPages;
            }
            else {
                return currentPage + 1;
            }
        } else {
            return 1;
        }
    }.property('model.page'),
    first: function(){
        var model = this.get("model");
        if (model && model.page) {
            var currentPage = model.page;
            if (currentPage <= 1) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.page'),
    last: function(){
        var model = this.get("model");
        if (model && model.page) {
            var currentPage = model.page;
            var totalPages = model.totalPages;
            if (currentPage >= totalPages) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.page'),
    actions: {
        switchSearchToMetric: function(keyword){
            this.transitionToRoute
            ( 'search'
                , { queryParams:
                { category: 'Metric'
                    , keywords: this.get('keywords')
                    , page: 1
                    , source: null
                }
                }
            )
        },
        switchSearchToFlow: function(keyword){
            this.transitionToRoute
            ( 'search'
                , { queryParams:
                { category: 'Flow'
                    , keywords: this.get('keywords')
                    , page: 1
                    , source: null
                }
                }
            )
        },
        switchSearchToComments: function(keyword){
            this.transitionToRoute
            ( 'search'
                , { queryParams:
                { category: 'Comment'
                    , keyword: this.get('keywords')
                    , page: 1
                    , source: null
                }
                }
            )
        }
    }
});