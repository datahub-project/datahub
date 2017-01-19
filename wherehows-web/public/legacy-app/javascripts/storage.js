(function() {
  var Storage = window.Storage = {}

  Storage.data = {}
  Storage.prefix = 'WHZ_'

  Storage.get = function(key) {
    var data, result
    try {
      data = localStorage.getItem(this.prefix + key)
    } catch(e) {
      data = {}
    }

    try {
      result = JSON.parse(data)
    } catch(e) {
      result = data
    }

    return result
  }

  Storage.set = function(key, data, cb) {
    if(typeof data === "object") {
      data = JSON.stringify(data)
      console.log('++ Storage', key)
    }

    try {
      localStorage.setItem(this.prefix + key, data)
    } catch(e) {
      console.log('!! Storage', e, data)
    }

    if(typeof cb === "function")
      cb.call(this)
  }

  Storage.remove = function(key) {
    try {
      var status = localStorage.removeItem(this.prefix + key)
      console.log('-- Storage', key)
      return status
    } catch(e) {
      console.log('!! Storage', e)
      return false
    }
  }
})(window)
