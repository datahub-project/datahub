App.SearchController = Ember.Controller.extend({
    queryParams: [
        'keywords',
        'category',
        'source',
        'page'
    ],
    keywords: null,
    category: null,
    datasetTitle: function(){
        var model = this.get("model");
        if (model && model.source) {
            if (model.source.toLocaleLowerCase() != 'all')
            {
                return model.source;
            }
        }
        return "Datasets";
    }.property('model.source'),
    isDatasets: function(){
        var model = this.get("model");
        if (model && model.category) {
            if (model.category.toLocaleLowerCase() === 'datasets')
            {
                return true;
            }
        }
        return false;
    }.property('model.category'),
    isComments: function(){
        var model = this.get("model");
        if (model && model.category) {
            if (model.category.toLocaleLowerCase() === 'comments')
            {
                return true;
            }
        }
        return false;
    }.property('model.category'),
    isFlows: function(){
        var model = this.get("model");
        if (model && model.category) {
            if (model.category.toLocaleLowerCase() === 'flows')
            {
                return true;
            }
        }
        return false;
    }.property('model.category'),
    isJobs: function(){
        var model = this.get("model");
        if (model && model.category) {
            if (model.category.toLocaleLowerCase() === 'jobs')
            {
                return true;
            }
        }
        return false;
    }.property('model.category'),
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
                { category: 'Flows'
                    , keywords: this.get('keywords')
                    , page: 1
                    , source: null
                }
                }
            )
        },
        switchSearchToJob: function(keyword){
            this.transitionToRoute
            ( 'search'
                , { queryParams:
                { category: 'Jobs'
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
                { category: 'Comments'
                    , keyword: this.get('keywords')
                    , page: 1
                    , source: null
                }
                }
            )
        }
    }
});