App.FlowsController = Ember.Controller.extend({
    currentName: null,
    urn: null,
    queryParams:null,
    previousPage: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            if (currentPage <= 1) {
                return currentPage;
            }
            else {
                return currentPage - 1;
            }
        } else {
            return 1;
        }

    }.property('model.data.page'),
    nextPage: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            var totalPages = model.data.totalPages;
            if (currentPage >= totalPages) {
                return totalPages;
            }
            else {
                return currentPage + 1;
            }
        } else {
            return 1;
        }
    }.property('model.data.page'),
    first: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            if (currentPage <= 1) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.data.page'),
    last: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            var totalPages = model.data.totalPages;
            if (currentPage >= totalPages) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.data.page')

});

App.FlowssubpageController = Ember.Controller.extend({
    queryParams: ['urn'],
    urn: null
});

App.PagedflowController = Ember.Controller.extend({
    flowname: 'flowName',
    queryParams: ['urn'],
    urn: null,
    lineageUrl: null,
    previousPage: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            if (currentPage <= 1) {
                return currentPage;
            }
            else {
                return currentPage - 1;
            }
        } else {
            return 1;
        }

    }.property('model.data.page'),
    nextPage: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            var totalPages = model.data.totalPages;
            if (currentPage >= totalPages) {
                return totalPages;
            }
            else {
                return currentPage + 1;
            }
        } else {
            return 1;
        }
    }.property('model.data.page'),
    first: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            if (currentPage <= 1) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.data.page'),
    last: function(){
        var model = this.get("model");
        if (model && model.data && model.data.page) {
            var currentPage = model.data.page;
            var totalPages = model.data.totalPages;
            if (currentPage >= totalPages) {
                return true;
            }
            else {
                return false
            }
        } else {
            return false;
        }
    }.property('model.data.page'),
    getUrnWatchId: function(urn){
        var controller = this;
        var watcherEndpoint = "/api/v1/urn/watch?urn=" + urn
        $.get(watcherEndpoint, function(data){
            if(data.id && data.id !== 0) {
                controller.set('urnWatched', true)
                controller.set('urnWatchedId', data.id)
            } else {
                controller.set('urnWatched', false)
                controller.set('urnWatchedId', 0)
            }
        })
    },
    actions: {
        watchUrn: function(urn) {
            var _this = this
            var url = "/api/v1/urn/watch"
            var token = $("#csrfToken").val().replace('/', '')
            if(!this.get('urnWatched')) {
                $.ajax({
                    url: url,
                    method: 'POST',
                    header: {
                        'Csrf-Token': token
                    },
                    data: {
                        csrfToken: token,
                        urn: urn,
                        type: 'urn',
                        'notification_type': 'WEEKLY'
                    }
                }).done(function(data, txt, xhr){
                    Notify.toast('You are now watching: ' + urn , 'Success', 'success')
                    _this.set('urnWatched', true)
                    _this.getUrnWatchId(urn);
                }).fail(function(xhr, txt, error){
                    Notify.toast('URN could not be watched', 'Error Watching Urn', 'error')
                })
            } else {
                url += ("/" + this.get('urnWatchedId'))
                url += "?csrfToken=" + token
                $.ajax({
                    url: url,
                    method: 'DELETE',
                    header: {
                        'Csrf-Token': token
                    },
                    dataType: 'json'
                }).done(function(data, txt, xhr){
                    _this.set('urnWatched', false)
                    _this.set('urnWatchedId', 0)
                }).fail(function(xhr, txt, error){
                    Notify.toast('Could not unwatch urn', 'Error', 'error')
                })
            }
        }
    }
});
