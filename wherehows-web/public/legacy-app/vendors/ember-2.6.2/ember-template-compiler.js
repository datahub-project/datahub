;(function() {
/*!
 * @overview  Ember - JavaScript Application Framework
 * @copyright Copyright 2011-2016 Tilde Inc. and contributors
 *            Portions Copyright 2006-2011 Strobe Inc.
 *            Portions Copyright 2008-2011 Apple Inc. All rights reserved.
 * @license   Licensed under MIT license
 *            See https://raw.github.com/emberjs/ember.js/master/LICENSE
 * @version   2.6.2
 */

var enifed, requireModule, require, Ember;
var mainContext = this;

(function() {
  var isNode = typeof window === 'undefined' &&
    typeof process !== 'undefined' && {}.toString.call(process) === '[object process]';

  if (!isNode) {
    Ember = this.Ember = this.Ember || {};
  }

  if (typeof Ember === 'undefined') { Ember = {}; }

  if (typeof Ember.__loader === 'undefined') {
    var registry = {};
    var seen = {};

    enifed = function(name, deps, callback) {
      var value = { };

      if (!callback) {
        value.deps = [];
        value.callback = deps;
      } else {
        value.deps = deps;
        value.callback = callback;
      }

      registry[name] = value;
    };

    require = requireModule = function(name) {
      return internalRequire(name, null);
    };

    // setup `require` module
    require['default'] = require;

    require.has = function registryHas(moduleName) {
      return !!registry[moduleName] || !!registry[moduleName + '/index'];
    };

    function missingModule(name, referrerName) {
      if (referrerName) {
        throw new Error('Could not find module ' + name + ' required by: ' + referrerName);
      } else {
        throw new Error('Could not find module ' + name);
      }
    }

    function internalRequire(_name, referrerName) {
      var name = _name;
      var mod = registry[name];

      if (!mod) {
        name = name + '/index';
        mod = registry[name];
      }

      var exports = seen[name];

      if (exports !== undefined) {
        return exports;
      }

      exports = seen[name] = {};

      if (!mod) {
        missingModule(_name, referrerName);
      }

      var deps = mod.deps;
      var callback = mod.callback;
      var length = deps.length;
      var reified = new Array(length);

      for (var i = 0; i < length; i++) {
        if (deps[i] === 'exports') {
          reified[i] = exports;
        } else if (deps[i] === 'require') {
          reified[i] = require;
        } else {
          reified[i] = internalRequire(deps[i], name);
        }
      }

      callback.apply(this, reified);

      return exports;
    }

    requireModule._eak_seen = registry;

    Ember.__loader = {
      define: enifed,
      require: require,
      registry: registry
    };
  } else {
    enifed = Ember.__loader.define;
    require = requireModule = Ember.__loader.require;
  }
})();

enifed('backburner', ['exports', 'backburner/utils', 'backburner/platform', 'backburner/binary-search', 'backburner/deferred-action-queues'], function (exports, _backburnerUtils, _backburnerPlatform, _backburnerBinarySearch, _backburnerDeferredActionQueues) {
  'use strict';

  exports.default = Backburner;

  function Backburner(queueNames, options) {
    this.queueNames = queueNames;
    this.options = options || {};
    if (!this.options.defaultQueue) {
      this.options.defaultQueue = queueNames[0];
    }
    this.instanceStack = [];
    this._debouncees = [];
    this._throttlers = [];
    this._eventCallbacks = {
      end: [],
      begin: []
    };

    var _this = this;
    this._boundClearItems = function () {
      clearItems();
    };

    this._timerTimeoutId = undefined;
    this._timers = [];

    this._platform = this.options._platform || _backburnerPlatform.default;

    this._boundRunExpiredTimers = function () {
      _this._runExpiredTimers();
    };
  }

  Backburner.prototype = {
    begin: function () {
      var options = this.options;
      var onBegin = options && options.onBegin;
      var previousInstance = this.currentInstance;

      if (previousInstance) {
        this.instanceStack.push(previousInstance);
      }

      this.currentInstance = new _backburnerDeferredActionQueues.default(this.queueNames, options);
      this._trigger('begin', this.currentInstance, previousInstance);
      if (onBegin) {
        onBegin(this.currentInstance, previousInstance);
      }
    },

    end: function () {
      var options = this.options;
      var onEnd = options && options.onEnd;
      var currentInstance = this.currentInstance;
      var nextInstance = null;

      // Prevent double-finally bug in Safari 6.0.2 and iOS 6
      // This bug appears to be resolved in Safari 6.0.5 and iOS 7
      var finallyAlreadyCalled = false;
      try {
        currentInstance.flush();
      } finally {
        if (!finallyAlreadyCalled) {
          finallyAlreadyCalled = true;

          this.currentInstance = null;

          if (this.instanceStack.length) {
            nextInstance = this.instanceStack.pop();
            this.currentInstance = nextInstance;
          }
          this._trigger('end', currentInstance, nextInstance);
          if (onEnd) {
            onEnd(currentInstance, nextInstance);
          }
        }
      }
    },

    /**
     Trigger an event. Supports up to two arguments. Designed around
     triggering transition events from one run loop instance to the
     next, which requires an argument for the first instance and then
     an argument for the next instance.
      @private
     @method _trigger
     @param {String} eventName
     @param {any} arg1
     @param {any} arg2
     */
    _trigger: function (eventName, arg1, arg2) {
      var callbacks = this._eventCallbacks[eventName];
      if (callbacks) {
        for (var i = 0; i < callbacks.length; i++) {
          callbacks[i](arg1, arg2);
        }
      }
    },

    on: function (eventName, callback) {
      if (typeof callback !== 'function') {
        throw new TypeError('Callback must be a function');
      }
      var callbacks = this._eventCallbacks[eventName];
      if (callbacks) {
        callbacks.push(callback);
      } else {
        throw new TypeError('Cannot on() event "' + eventName + '" because it does not exist');
      }
    },

    off: function (eventName, callback) {
      if (eventName) {
        var callbacks = this._eventCallbacks[eventName];
        var callbackFound = false;
        if (!callbacks) return;
        if (callback) {
          for (var i = 0; i < callbacks.length; i++) {
            if (callbacks[i] === callback) {
              callbackFound = true;
              callbacks.splice(i, 1);
              i--;
            }
          }
        }
        if (!callbackFound) {
          throw new TypeError('Cannot off() callback that does not exist');
        }
      } else {
        throw new TypeError('Cannot off() event "' + eventName + '" because it does not exist');
      }
    },

    run: function () /* target, method, args */{
      var length = arguments.length;
      var method, target, args;

      if (length === 1) {
        method = arguments[0];
        target = null;
      } else {
        target = arguments[0];
        method = arguments[1];
      }

      if (_backburnerUtils.isString(method)) {
        method = target[method];
      }

      if (length > 2) {
        args = new Array(length - 2);
        for (var i = 0, l = length - 2; i < l; i++) {
          args[i] = arguments[i + 2];
        }
      } else {
        args = [];
      }

      var onError = getOnError(this.options);

      this.begin();

      // guard against Safari 6's double-finally bug
      var didFinally = false;

      if (onError) {
        try {
          return method.apply(target, args);
        } catch (error) {
          onError(error);
        } finally {
          if (!didFinally) {
            didFinally = true;
            this.end();
          }
        }
      } else {
        try {
          return method.apply(target, args);
        } finally {
          if (!didFinally) {
            didFinally = true;
            this.end();
          }
        }
      }
    },

    /*
      Join the passed method with an existing queue and execute immediately,
      if there isn't one use `Backburner#run`.
       The join method is like the run method except that it will schedule into
      an existing queue if one already exists. In either case, the join method will
      immediately execute the passed in function and return its result.
       @method join
      @param {Object} target
      @param {Function} method The method to be executed
      @param {any} args The method arguments
      @return method result
    */
    join: function () /* target, method, args */{
      if (!this.currentInstance) {
        return this.run.apply(this, arguments);
      }

      var length = arguments.length;
      var method, target;

      if (length === 1) {
        method = arguments[0];
        target = null;
      } else {
        target = arguments[0];
        method = arguments[1];
      }

      if (_backburnerUtils.isString(method)) {
        method = target[method];
      }

      if (length === 1) {
        return method();
      } else if (length === 2) {
        return method.call(target);
      } else {
        var args = new Array(length - 2);
        for (var i = 0, l = length - 2; i < l; i++) {
          args[i] = arguments[i + 2];
        }
        return method.apply(target, args);
      }
    },

    /*
      Defer the passed function to run inside the specified queue.
       @method defer
      @param {String} queueName
      @param {Object} target
      @param {Function|String} method The method or method name to be executed
      @param {any} args The method arguments
      @return method result
    */
    defer: function (queueName /* , target, method, args */) {
      var length = arguments.length;
      var method, target, args;

      if (length === 2) {
        method = arguments[1];
        target = null;
      } else {
        target = arguments[1];
        method = arguments[2];
      }

      if (_backburnerUtils.isString(method)) {
        method = target[method];
      }

      var stack = this.DEBUG ? new Error() : undefined;

      if (length > 3) {
        args = new Array(length - 3);
        for (var i = 3; i < length; i++) {
          args[i - 3] = arguments[i];
        }
      } else {
        args = undefined;
      }

      if (!this.currentInstance) {
        createAutorun(this);
      }
      return this.currentInstance.schedule(queueName, target, method, args, false, stack);
    },

    deferOnce: function (queueName /* , target, method, args */) {
      var length = arguments.length;
      var method, target, args;

      if (length === 2) {
        method = arguments[1];
        target = null;
      } else {
        target = arguments[1];
        method = arguments[2];
      }

      if (_backburnerUtils.isString(method)) {
        method = target[method];
      }

      var stack = this.DEBUG ? new Error() : undefined;

      if (length > 3) {
        args = new Array(length - 3);
        for (var i = 3; i < length; i++) {
          args[i - 3] = arguments[i];
        }
      } else {
        args = undefined;
      }

      if (!this.currentInstance) {
        createAutorun(this);
      }
      return this.currentInstance.schedule(queueName, target, method, args, true, stack);
    },

    setTimeout: function () {
      var l = arguments.length;
      var args = new Array(l);

      for (var x = 0; x < l; x++) {
        args[x] = arguments[x];
      }

      var length = args.length,
          method,
          wait,
          target,
          methodOrTarget,
          methodOrWait,
          methodOrArgs;

      if (length === 0) {
        return;
      } else if (length === 1) {
        method = args.shift();
        wait = 0;
      } else if (length === 2) {
        methodOrTarget = args[0];
        methodOrWait = args[1];

        if (_backburnerUtils.isFunction(methodOrWait) || _backburnerUtils.isFunction(methodOrTarget[methodOrWait])) {
          target = args.shift();
          method = args.shift();
          wait = 0;
        } else if (_backburnerUtils.isCoercableNumber(methodOrWait)) {
          method = args.shift();
          wait = args.shift();
        } else {
          method = args.shift();
          wait = 0;
        }
      } else {
        var last = args[args.length - 1];

        if (_backburnerUtils.isCoercableNumber(last)) {
          wait = args.pop();
        } else {
          wait = 0;
        }

        methodOrTarget = args[0];
        methodOrArgs = args[1];

        if (_backburnerUtils.isFunction(methodOrArgs) || _backburnerUtils.isString(methodOrArgs) && methodOrTarget !== null && methodOrArgs in methodOrTarget) {
          target = args.shift();
          method = args.shift();
        } else {
          method = args.shift();
        }
      }

      var executeAt = Date.now() + parseInt(wait !== wait ? 0 : wait, 10);

      if (_backburnerUtils.isString(method)) {
        method = target[method];
      }

      var onError = getOnError(this.options);

      function fn() {
        if (onError) {
          try {
            method.apply(target, args);
          } catch (e) {
            onError(e);
          }
        } else {
          method.apply(target, args);
        }
      }

      return this._setTimeout(fn, executeAt);
    },

    _setTimeout: function (fn, executeAt) {
      if (this._timers.length === 0) {
        this._timers.push(executeAt, fn);
        this._installTimerTimeout();
        return fn;
      }

      // find position to insert
      var i = _backburnerBinarySearch.default(executeAt, this._timers);

      this._timers.splice(i, 0, executeAt, fn);

      // we should be the new earliest timer if i == 0
      if (i === 0) {
        this._reinstallTimerTimeout();
      }

      return fn;
    },

    throttle: function (target, method /* , args, wait, [immediate] */) {
      var backburner = this;
      var args = new Array(arguments.length);
      for (var i = 0; i < arguments.length; i++) {
        args[i] = arguments[i];
      }
      var immediate = args.pop();
      var wait, throttler, index, timer;

      if (_backburnerUtils.isNumber(immediate) || _backburnerUtils.isString(immediate)) {
        wait = immediate;
        immediate = true;
      } else {
        wait = args.pop();
      }

      wait = parseInt(wait, 10);

      index = findThrottler(target, method, this._throttlers);
      if (index > -1) {
        return this._throttlers[index];
      } // throttled

      timer = this._platform.setTimeout(function () {
        if (!immediate) {
          backburner.run.apply(backburner, args);
        }
        var index = findThrottler(target, method, backburner._throttlers);
        if (index > -1) {
          backburner._throttlers.splice(index, 1);
        }
      }, wait);

      if (immediate) {
        this.run.apply(this, args);
      }

      throttler = [target, method, timer];

      this._throttlers.push(throttler);

      return throttler;
    },

    debounce: function (target, method /* , args, wait, [immediate] */) {
      var backburner = this;
      var args = new Array(arguments.length);
      for (var i = 0; i < arguments.length; i++) {
        args[i] = arguments[i];
      }

      var immediate = args.pop();
      var wait, index, debouncee, timer;

      if (_backburnerUtils.isNumber(immediate) || _backburnerUtils.isString(immediate)) {
        wait = immediate;
        immediate = false;
      } else {
        wait = args.pop();
      }

      wait = parseInt(wait, 10);
      // Remove debouncee
      index = findDebouncee(target, method, this._debouncees);

      if (index > -1) {
        debouncee = this._debouncees[index];
        this._debouncees.splice(index, 1);
        this._platform.clearTimeout(debouncee[2]);
      }

      timer = this._platform.setTimeout(function () {
        if (!immediate) {
          backburner.run.apply(backburner, args);
        }
        var index = findDebouncee(target, method, backburner._debouncees);
        if (index > -1) {
          backburner._debouncees.splice(index, 1);
        }
      }, wait);

      if (immediate && index === -1) {
        backburner.run.apply(backburner, args);
      }

      debouncee = [target, method, timer];

      backburner._debouncees.push(debouncee);

      return debouncee;
    },

    cancelTimers: function () {
      _backburnerUtils.each(this._throttlers, this._boundClearItems);
      this._throttlers = [];

      _backburnerUtils.each(this._debouncees, this._boundClearItems);
      this._debouncees = [];

      this._clearTimerTimeout();
      this._timers = [];

      if (this._autorun) {
        this._platform.clearTimeout(this._autorun);
        this._autorun = null;
      }
    },

    hasTimers: function () {
      return !!this._timers.length || !!this._debouncees.length || !!this._throttlers.length || this._autorun;
    },

    cancel: function (timer) {
      var timerType = typeof timer;

      if (timer && timerType === 'object' && timer.queue && timer.method) {
        // we're cancelling a deferOnce
        return timer.queue.cancel(timer);
      } else if (timerType === 'function') {
        // we're cancelling a setTimeout
        for (var i = 0, l = this._timers.length; i < l; i += 2) {
          if (this._timers[i + 1] === timer) {
            this._timers.splice(i, 2); // remove the two elements
            if (i === 0) {
              this._reinstallTimerTimeout();
            }
            return true;
          }
        }
      } else if (Object.prototype.toString.call(timer) === '[object Array]') {
        // we're cancelling a throttle or debounce
        return this._cancelItem(findThrottler, this._throttlers, timer) || this._cancelItem(findDebouncee, this._debouncees, timer);
      } else {
        return; // timer was null or not a timer
      }
    },

    _cancelItem: function (findMethod, array, timer) {
      var item, index;

      if (timer.length < 3) {
        return false;
      }

      index = findMethod(timer[0], timer[1], array);

      if (index > -1) {

        item = array[index];

        if (item[2] === timer[2]) {
          array.splice(index, 1);
          this._platform.clearTimeout(timer[2]);
          return true;
        }
      }

      return false;
    },

    _runExpiredTimers: function () {
      this._timerTimeoutId = undefined;
      this.run(this, this._scheduleExpiredTimers);
    },

    _scheduleExpiredTimers: function () {
      var n = Date.now();
      var timers = this._timers;
      var i = 0;
      var l = timers.length;
      for (; i < l; i += 2) {
        var executeAt = timers[i];
        var fn = timers[i + 1];
        if (executeAt <= n) {
          this.schedule(this.options.defaultQueue, null, fn);
        } else {
          break;
        }
      }
      timers.splice(0, i);
      this._installTimerTimeout();
    },

    _reinstallTimerTimeout: function () {
      this._clearTimerTimeout();
      this._installTimerTimeout();
    },

    _clearTimerTimeout: function () {
      if (!this._timerTimeoutId) {
        return;
      }
      this._platform.clearTimeout(this._timerTimeoutId);
      this._timerTimeoutId = undefined;
    },

    _installTimerTimeout: function () {
      if (!this._timers.length) {
        return;
      }
      var minExpiresAt = this._timers[0];
      var n = Date.now();
      var wait = Math.max(0, minExpiresAt - n);
      this._timerTimeoutId = this._platform.setTimeout(this._boundRunExpiredTimers, wait);
    }
  };

  Backburner.prototype.schedule = Backburner.prototype.defer;
  Backburner.prototype.scheduleOnce = Backburner.prototype.deferOnce;
  Backburner.prototype.later = Backburner.prototype.setTimeout;

  function getOnError(options) {
    return options.onError || options.onErrorTarget && options.onErrorTarget[options.onErrorMethod];
  }

  function createAutorun(backburner) {
    backburner.begin();
    backburner._autorun = backburner._platform.setTimeout(function () {
      backburner._autorun = null;
      backburner.end();
    });
  }

  function findDebouncee(target, method, debouncees) {
    return findItem(target, method, debouncees);
  }

  function findThrottler(target, method, throttlers) {
    return findItem(target, method, throttlers);
  }

  function findItem(target, method, collection) {
    var item;
    var index = -1;

    for (var i = 0, l = collection.length; i < l; i++) {
      item = collection[i];
      if (item[0] === target && item[1] === method) {
        index = i;
        break;
      }
    }

    return index;
  }

  function clearItems(item) {
    this._platform.clearTimeout(item[2]);
  }
});
enifed("backburner/binary-search", ["exports"], function (exports) {
  "use strict";

  exports.default = binarySearch;

  function binarySearch(time, timers) {
    var start = 0;
    var end = timers.length - 2;
    var middle, l;

    while (start < end) {
      // since timers is an array of pairs 'l' will always
      // be an integer
      l = (end - start) / 2;

      // compensate for the index in case even number
      // of pairs inside timers
      middle = start + l - l % 2;

      if (time >= timers[middle]) {
        start = middle + 2;
      } else {
        end = middle;
      }
    }

    return time >= timers[start] ? start + 2 : start;
  }
});
enifed('backburner/deferred-action-queues', ['exports', 'backburner/utils', 'backburner/queue'], function (exports, _backburnerUtils, _backburnerQueue) {
  'use strict';

  exports.default = DeferredActionQueues;

  function DeferredActionQueues(queueNames, options) {
    var queues = this.queues = {};
    this.queueNames = queueNames = queueNames || [];

    this.options = options;

    _backburnerUtils.each(queueNames, function (queueName) {
      queues[queueName] = new _backburnerQueue.default(queueName, options[queueName], options);
    });
  }

  function noSuchQueue(name) {
    throw new Error('You attempted to schedule an action in a queue (' + name + ') that doesn\'t exist');
  }

  function noSuchMethod(name) {
    throw new Error('You attempted to schedule an action in a queue (' + name + ') for a method that doesn\'t exist');
  }

  DeferredActionQueues.prototype = {
    schedule: function (name, target, method, args, onceFlag, stack) {
      var queues = this.queues;
      var queue = queues[name];

      if (!queue) {
        noSuchQueue(name);
      }

      if (!method) {
        noSuchMethod(name);
      }

      if (onceFlag) {
        return queue.pushUnique(target, method, args, stack);
      } else {
        return queue.push(target, method, args, stack);
      }
    },

    flush: function () {
      var queues = this.queues;
      var queueNames = this.queueNames;
      var queueName, queue;
      var queueNameIndex = 0;
      var numberOfQueues = queueNames.length;

      while (queueNameIndex < numberOfQueues) {
        queueName = queueNames[queueNameIndex];
        queue = queues[queueName];

        var numberOfQueueItems = queue._queue.length;

        if (numberOfQueueItems === 0) {
          queueNameIndex++;
        } else {
          queue.flush(false /* async */);
          queueNameIndex = 0;
        }
      }
    }
  };
});
enifed('backburner/platform', ['exports'], function (exports) {
  'use strict';

  var GlobalContext;

  /* global self */
  if (typeof self === 'object') {
    GlobalContext = self;

    /* global global */
  } else if (typeof global === 'object') {
      GlobalContext = global;

      /* global window */
    } else if (typeof window === 'object') {
        GlobalContext = window;
      } else {
        throw new Error('no global: `self`, `global` nor `window` was found');
      }

  exports.default = GlobalContext;
});
enifed('backburner/queue', ['exports', 'backburner/utils'], function (exports, _backburnerUtils) {
  'use strict';

  exports.default = Queue;

  function Queue(name, options, globalOptions) {
    this.name = name;
    this.globalOptions = globalOptions || {};
    this.options = options;
    this._queue = [];
    this.targetQueues = {};
    this._queueBeingFlushed = undefined;
  }

  Queue.prototype = {
    push: function (target, method, args, stack) {
      var queue = this._queue;
      queue.push(target, method, args, stack);

      return {
        queue: this,
        target: target,
        method: method
      };
    },

    pushUniqueWithoutGuid: function (target, method, args, stack) {
      var queue = this._queue;

      for (var i = 0, l = queue.length; i < l; i += 4) {
        var currentTarget = queue[i];
        var currentMethod = queue[i + 1];

        if (currentTarget === target && currentMethod === method) {
          queue[i + 2] = args; // replace args
          queue[i + 3] = stack; // replace stack
          return;
        }
      }

      queue.push(target, method, args, stack);
    },

    targetQueue: function (targetQueue, target, method, args, stack) {
      var queue = this._queue;

      for (var i = 0, l = targetQueue.length; i < l; i += 2) {
        var currentMethod = targetQueue[i];
        var currentIndex = targetQueue[i + 1];

        if (currentMethod === method) {
          queue[currentIndex + 2] = args; // replace args
          queue[currentIndex + 3] = stack; // replace stack
          return;
        }
      }

      targetQueue.push(method, queue.push(target, method, args, stack) - 4);
    },

    pushUniqueWithGuid: function (guid, target, method, args, stack) {
      var hasLocalQueue = this.targetQueues[guid];

      if (hasLocalQueue) {
        this.targetQueue(hasLocalQueue, target, method, args, stack);
      } else {
        this.targetQueues[guid] = [method, this._queue.push(target, method, args, stack) - 4];
      }

      return {
        queue: this,
        target: target,
        method: method
      };
    },

    pushUnique: function (target, method, args, stack) {
      var KEY = this.globalOptions.GUID_KEY;

      if (target && KEY) {
        var guid = target[KEY];
        if (guid) {
          return this.pushUniqueWithGuid(guid, target, method, args, stack);
        }
      }

      this.pushUniqueWithoutGuid(target, method, args, stack);

      return {
        queue: this,
        target: target,
        method: method
      };
    },

    invoke: function (target, method, args, _, _errorRecordedForStack) {
      if (args && args.length > 0) {
        method.apply(target, args);
      } else {
        method.call(target);
      }
    },

    invokeWithOnError: function (target, method, args, onError, errorRecordedForStack) {
      try {
        if (args && args.length > 0) {
          method.apply(target, args);
        } else {
          method.call(target);
        }
      } catch (error) {
        onError(error, errorRecordedForStack);
      }
    },

    flush: function (sync) {
      var queue = this._queue;
      var length = queue.length;

      if (length === 0) {
        return;
      }

      var globalOptions = this.globalOptions;
      var options = this.options;
      var before = options && options.before;
      var after = options && options.after;
      var onError = globalOptions.onError || globalOptions.onErrorTarget && globalOptions.onErrorTarget[globalOptions.onErrorMethod];
      var target, method, args, errorRecordedForStack;
      var invoke = onError ? this.invokeWithOnError : this.invoke;

      this.targetQueues = Object.create(null);
      var queueItems = this._queueBeingFlushed = this._queue.slice();
      this._queue = [];

      if (before) {
        before();
      }

      for (var i = 0; i < length; i += 4) {
        target = queueItems[i];
        method = queueItems[i + 1];
        args = queueItems[i + 2];
        errorRecordedForStack = queueItems[i + 3]; // Debugging assistance

        if (_backburnerUtils.isString(method)) {
          method = target[method];
        }

        // method could have been nullified / canceled during flush
        if (method) {
          //
          //    ** Attention intrepid developer **
          //
          //    To find out the stack of this task when it was scheduled onto
          //    the run loop, add the following to your app.js:
          //
          //    Ember.run.backburner.DEBUG = true; // NOTE: This slows your app, don't leave it on in production.
          //
          //    Once that is in place, when you are at a breakpoint and navigate
          //    here in the stack explorer, you can look at `errorRecordedForStack.stack`,
          //    which will be the captured stack when this job was scheduled.
          //
          invoke(target, method, args, onError, errorRecordedForStack);
        }
      }

      if (after) {
        after();
      }

      this._queueBeingFlushed = undefined;

      if (sync !== false && this._queue.length > 0) {
        // check if new items have been added
        this.flush(true);
      }
    },

    cancel: function (actionToCancel) {
      var queue = this._queue,
          currentTarget,
          currentMethod,
          i,
          l;
      var target = actionToCancel.target;
      var method = actionToCancel.method;
      var GUID_KEY = this.globalOptions.GUID_KEY;

      if (GUID_KEY && this.targetQueues && target) {
        var targetQueue = this.targetQueues[target[GUID_KEY]];

        if (targetQueue) {
          for (i = 0, l = targetQueue.length; i < l; i++) {
            if (targetQueue[i] === method) {
              targetQueue.splice(i, 1);
            }
          }
        }
      }

      for (i = 0, l = queue.length; i < l; i += 4) {
        currentTarget = queue[i];
        currentMethod = queue[i + 1];

        if (currentTarget === target && currentMethod === method) {
          queue.splice(i, 4);
          return true;
        }
      }

      // if not found in current queue
      // could be in the queue that is being flushed
      queue = this._queueBeingFlushed;

      if (!queue) {
        return;
      }

      for (i = 0, l = queue.length; i < l; i += 4) {
        currentTarget = queue[i];
        currentMethod = queue[i + 1];

        if (currentTarget === target && currentMethod === method) {
          // don't mess with array during flush
          // just nullify the method
          queue[i + 1] = null;
          return true;
        }
      }
    }
  };
});
enifed('backburner/utils', ['exports'], function (exports) {
  'use strict';

  exports.each = each;
  exports.isString = isString;
  exports.isFunction = isFunction;
  exports.isNumber = isNumber;
  exports.isCoercableNumber = isCoercableNumber;
  var NUMBER = /\d+/;

  function each(collection, callback) {
    for (var i = 0; i < collection.length; i++) {
      callback(collection[i]);
    }
  }

  function isString(suspect) {
    return typeof suspect === 'string';
  }

  function isFunction(suspect) {
    return typeof suspect === 'function';
  }

  function isNumber(suspect) {
    return typeof suspect === 'number';
  }

  function isCoercableNumber(number) {
    return isNumber(number) || NUMBER.test(number);
  }
});
enifed('ember-debug/deprecate', ['exports', 'ember-metal/core', 'ember-metal/error', 'ember-metal/logger', 'ember-debug/handlers'], function (exports, _emberMetalCore, _emberMetalError, _emberMetalLogger, _emberDebugHandlers) {
  /*global __fail__*/

  'use strict';

  var _slice = Array.prototype.slice;
  exports.registerHandler = registerHandler;
  exports.default = deprecate;

  function registerHandler(handler) {
    _emberDebugHandlers.registerHandler('deprecate', handler);
  }

  function formatMessage(_message, options) {
    var message = _message;

    if (options && options.id) {
      message = message + (' [deprecation id: ' + options.id + ']');
    }

    if (options && options.url) {
      message += ' See ' + options.url + ' for more details.';
    }

    return message;
  }

  registerHandler(function logDeprecationToConsole(message, options) {
    var updatedMessage = formatMessage(message, options);

    _emberMetalLogger.default.warn('DEPRECATION: ' + updatedMessage);
  });

  var captureErrorForStack = undefined;

  if (new Error().stack) {
    captureErrorForStack = function () {
      return new Error();
    };
  } else {
    captureErrorForStack = function () {
      try {
        __fail__.fail();
      } catch (e) {
        return e;
      }
    };
  }

  registerHandler(function logDeprecationStackTrace(message, options, next) {
    if (_emberMetalCore.default.LOG_STACKTRACE_ON_DEPRECATION) {
      var stackStr = '';
      var error = captureErrorForStack();
      var stack = undefined;

      if (error.stack) {
        if (error['arguments']) {
          // Chrome
          stack = error.stack.replace(/^\s+at\s+/gm, '').replace(/^([^\(]+?)([\n$])/gm, '{anonymous}($1)$2').replace(/^Object.<anonymous>\s*\(([^\)]+)\)/gm, '{anonymous}($1)').split('\n');
          stack.shift();
        } else {
          // Firefox
          stack = error.stack.replace(/(?:\n@:0)?\s+$/m, '').replace(/^\(/gm, '{anonymous}(').split('\n');
        }

        stackStr = '\n    ' + stack.slice(2).join('\n    ');
      }

      var updatedMessage = formatMessage(message, options);

      _emberMetalLogger.default.warn('DEPRECATION: ' + updatedMessage + stackStr);
    } else {
      next.apply(undefined, arguments);
    }
  });

  registerHandler(function raiseOnDeprecation(message, options, next) {
    if (_emberMetalCore.default.ENV.RAISE_ON_DEPRECATION) {
      var updatedMessage = formatMessage(message);

      throw new _emberMetalError.default(updatedMessage);
    } else {
      next.apply(undefined, arguments);
    }
  });

  var missingOptionsDeprecation = 'When calling `Ember.deprecate` you ' + 'must provide an `options` hash as the third parameter.  ' + '`options` should include `id` and `until` properties.';
  exports.missingOptionsDeprecation = missingOptionsDeprecation;
  var missingOptionsIdDeprecation = 'When calling `Ember.deprecate` you must provide `id` in options.';
  exports.missingOptionsIdDeprecation = missingOptionsIdDeprecation;
  var missingOptionsUntilDeprecation = 'When calling `Ember.deprecate` you must provide `until` in options.';

  exports.missingOptionsUntilDeprecation = missingOptionsUntilDeprecation;
  /**
  @module ember
  @submodule ember-debug
  */

  /**
    Display a deprecation warning with the provided message and a stack trace
    (Chrome and Firefox only).
  
    * In a production build, this method is defined as an empty function (NOP).
    Uses of this method in Ember itself are stripped from the ember.prod.js build.
  
    @method deprecate
    @param {String} message A description of the deprecation.
    @param {Boolean} test A boolean. If falsy, the deprecation
      will be displayed.
    @param {Object} options An object that can be used to pass
      in a `url` to the transition guide on the emberjs.com website, and a unique
      `id` for this deprecation. The `id` can be used by Ember debugging tools
      to change the behavior (raise, log or silence) for that specific deprecation.
      The `id` should be namespaced by dots, e.g. "view.helper.select".
    @for Ember
    @public
  */

  function deprecate(message, test, options) {
    if (!options || !options.id && !options.until) {
      deprecate(missingOptionsDeprecation, false, {
        id: 'ember-debug.deprecate-options-missing',
        until: '3.0.0',
        url: 'http://emberjs.com/deprecations/v2.x/#toc_ember-debug-function-options'
      });
    }

    if (options && !options.id) {
      deprecate(missingOptionsIdDeprecation, false, {
        id: 'ember-debug.deprecate-id-missing',
        until: '3.0.0',
        url: 'http://emberjs.com/deprecations/v2.x/#toc_ember-debug-function-options'
      });
    }

    if (options && !options.until) {
      deprecate(missingOptionsUntilDeprecation, options && options.until, {
        id: 'ember-debug.deprecate-until-missing',
        until: '3.0.0',
        url: 'http://emberjs.com/deprecations/v2.x/#toc_ember-debug-function-options'
      });
    }

    _emberDebugHandlers.invoke.apply(undefined, ['deprecate'].concat(_slice.call(arguments)));
  }
});
enifed("ember-debug/handlers", ["exports"], function (exports) {
  "use strict";

  exports.registerHandler = registerHandler;
  exports.invoke = invoke;
  var HANDLERS = {};

  exports.HANDLERS = HANDLERS;

  function registerHandler(type, callback) {
    var nextHandler = HANDLERS[type] || function () {};

    HANDLERS[type] = function (message, options) {
      callback(message, options, nextHandler);
    };
  }

  function invoke(type, message, test, options) {
    if (test) {
      return;
    }

    var handlerForType = HANDLERS[type];

    if (!handlerForType) {
      return;
    }

    if (handlerForType) {
      handlerForType(message, options);
    }
  }
});
enifed('ember-debug/index', ['exports', 'ember-metal/core', 'ember-metal/debug', 'ember-metal/features', 'ember-metal/error', 'ember-metal/logger', 'ember-metal/environment', 'ember-debug/deprecate', 'ember-debug/warn'], function (exports, _emberMetalCore, _emberMetalDebug, _emberMetalFeatures, _emberMetalError, _emberMetalLogger, _emberMetalEnvironment, _emberDebugDeprecate, _emberDebugWarn) {
  'use strict';

  exports._warnIfUsingStrippedFeatureFlags = _warnIfUsingStrippedFeatureFlags;

  /**
  @module ember
  @submodule ember-debug
  */

  /**
  @class Ember
  @public
  */

  /**
    Define an assertion that will throw an exception if the condition is not met.
  
    * In a production build, this method is defined as an empty function (NOP).
    Uses of this method in Ember itself are stripped from the ember.prod.js build.
  
    ```javascript
    // Test for truthiness
    Ember.assert('Must pass a valid object', obj);
  
    // Fail unconditionally
    Ember.assert('This code path should never be run');
    ```
  
    @method assert
    @param {String} desc A description of the assertion. This will become
      the text of the Error thrown if the assertion fails.
    @param {Boolean} test Must be truthy for the assertion to pass. If
      falsy, an exception will be thrown.
    @public
  */
  _emberMetalDebug.setDebugFunction('assert', function assert(desc, test) {
    if (!test) {
      throw new _emberMetalError.default('Assertion Failed: ' + desc);
    }
  });

  /**
    Display a debug notice.
  
    * In a production build, this method is defined as an empty function (NOP).
    Uses of this method in Ember itself are stripped from the ember.prod.js build.
  
    ```javascript
    Ember.debug('I\'m a debug notice!');
    ```
  
    @method debug
    @param {String} message A debug message to display.
    @public
  */
  _emberMetalDebug.setDebugFunction('debug', function debug(message) {
    _emberMetalLogger.default.debug('DEBUG: ' + message);
  });

  /**
    Display an info notice.
  
    * In a production build, this method is defined as an empty function (NOP).
    Uses of this method in Ember itself are stripped from the ember.prod.js build.
  
    @method info
    @private
  */
  _emberMetalDebug.setDebugFunction('info', function info() {
    _emberMetalLogger.default.info.apply(undefined, arguments);
  });

  /**
    Alias an old, deprecated method with its new counterpart.
  
    Display a deprecation warning with the provided message and a stack trace
    (Chrome and Firefox only) when the assigned method is called.
  
    * In a production build, this method is defined as an empty function (NOP).
  
    ```javascript
    Ember.oldMethod = Ember.deprecateFunc('Please use the new, updated method', Ember.newMethod);
    ```
  
    @method deprecateFunc
    @param {String} message A description of the deprecation.
    @param {Object} [options] The options object for Ember.deprecate.
    @param {Function} func The new function called to replace its deprecated counterpart.
    @return {Function} A new function that wraps the original function with a deprecation warning
    @private
  */
  _emberMetalDebug.setDebugFunction('deprecateFunc', function deprecateFunc() {
    for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    if (args.length === 3) {
      var _ret = (function () {
        var message = args[0];
        var options = args[1];
        var func = args[2];

        return {
          v: function () {
            _emberMetalDebug.deprecate(message, false, options);
            return func.apply(this, arguments);
          }
        };
      })();

      if (typeof _ret === 'object') return _ret.v;
    } else {
      var _ret2 = (function () {
        var message = args[0];
        var func = args[1];

        return {
          v: function () {
            _emberMetalDebug.deprecate(message);
            return func.apply(this, arguments);
          }
        };
      })();

      if (typeof _ret2 === 'object') return _ret2.v;
    }
  });

  /**
    Run a function meant for debugging.
  
    * In a production build, this method is defined as an empty function (NOP).
    Uses of this method in Ember itself are stripped from the ember.prod.js build.
  
    ```javascript
    Ember.runInDebug(() => {
      Ember.Component.reopen({
        didInsertElement() {
          console.log("I'm happy");
        }
      });
    });
    ```
  
    @method runInDebug
    @param {Function} func The function to be executed.
    @since 1.5.0
    @public
  */
  _emberMetalDebug.setDebugFunction('runInDebug', function runInDebug(func) {
    func();
  });

  _emberMetalDebug.setDebugFunction('debugSeal', function debugSeal(obj) {
    Object.seal(obj);
  });

  _emberMetalDebug.setDebugFunction('deprecate', _emberDebugDeprecate.default);

  _emberMetalDebug.setDebugFunction('warn', _emberDebugWarn.default);

  /**
    Will call `Ember.warn()` if ENABLE_OPTIONAL_FEATURES or
    any specific FEATURES flag is truthy.
  
    This method is called automatically in debug canary builds.
  
    @private
    @method _warnIfUsingStrippedFeatureFlags
    @return {void}
  */

  function _warnIfUsingStrippedFeatureFlags(FEATURES, knownFeatures, featuresWereStripped) {
    if (featuresWereStripped) {
      _emberMetalDebug.warn('Ember.ENV.ENABLE_OPTIONAL_FEATURES is only available in canary builds.', !_emberMetalCore.default.ENV.ENABLE_OPTIONAL_FEATURES, { id: 'ember-debug.feature-flag-with-features-stripped' });

      var keys = Object.keys(FEATURES || {});
      for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        if (key === 'isEnabled' || !(key in knownFeatures)) {
          continue;
        }

        _emberMetalDebug.warn('FEATURE["' + key + '"] is set as enabled, but FEATURE flags are only available in canary builds.', !FEATURES[key], { id: 'ember-debug.feature-flag-with-features-stripped' });
      }
    }
  }

  if (!_emberMetalCore.default.testing) {
    // Complain if they're using FEATURE flags in builds other than canary
    _emberMetalFeatures.FEATURES['features-stripped-test'] = true;
    var featuresWereStripped = true;

    delete _emberMetalFeatures.FEATURES['features-stripped-test'];
    _warnIfUsingStrippedFeatureFlags(_emberMetalCore.default.ENV.FEATURES, _emberMetalFeatures.KNOWN_FEATURES, featuresWereStripped);

    // Inform the developer about the Ember Inspector if not installed.
    var isFirefox = _emberMetalEnvironment.default.isFirefox;
    var isChrome = _emberMetalEnvironment.default.isChrome;

    if (typeof window !== 'undefined' && (isFirefox || isChrome) && window.addEventListener) {
      window.addEventListener('load', function () {
        if (document.documentElement && document.documentElement.dataset && !document.documentElement.dataset.emberExtension) {
          var downloadURL;

          if (isChrome) {
            downloadURL = 'https://chrome.google.com/webstore/detail/ember-inspector/bmdblncegkenkacieihfhpjfppoconhi';
          } else if (isFirefox) {
            downloadURL = 'https://addons.mozilla.org/en-US/firefox/addon/ember-inspector/';
          }

          _emberMetalDebug.debug('For more advanced debugging, install the Ember Inspector from ' + downloadURL);
        }
      }, false);
    }
  }
  /**
    @public
    @class Ember.Debug
  */
  _emberMetalCore.default.Debug = {};

  /**
    Allows for runtime registration of handler functions that override the default deprecation behavior.
    Deprecations are invoked by calls to [Ember.deprecate](http://emberjs.com/api/classes/Ember.html#method_deprecate).
    The following example demonstrates its usage by registering a handler that throws an error if the
    message contains the word "should", otherwise defers to the default handler.
  
    ```javascript
    Ember.Debug.registerDeprecationHandler((message, options, next) => {
      if (message.indexOf('should') !== -1) {
        throw new Error(`Deprecation message with should: ${message}`);
      } else {
        // defer to whatever handler was registered before this one
        next(message, options);
      }
    }
    ```
  
    The handler function takes the following arguments:
  
    <ul>
      <li> <code>message</code> - The message received from the deprecation call.</li>
      <li> <code>options</code> - An object passed in with the deprecation call containing additional information including:</li>
        <ul>
          <li> <code>id</code> - An id of the deprecation in the form of <code>package-name.specific-deprecation</code>.</li>
          <li> <code>until</code> - The Ember version number the feature and deprecation will be removed in.</li>
        </ul>
      <li> <code>next</code> - A function that calls into the previously registered handler.</li>
    </ul>
  
    @public
    @static
    @method registerDeprecationHandler
    @param handler {Function} A function to handle deprecation calls.
    @since 2.1.0
  */
  _emberMetalCore.default.Debug.registerDeprecationHandler = _emberDebugDeprecate.registerHandler;
  /**
    Allows for runtime registration of handler functions that override the default warning behavior.
    Warnings are invoked by calls made to [Ember.warn](http://emberjs.com/api/classes/Ember.html#method_warn).
    The following example demonstrates its usage by registering a handler that does nothing overriding Ember's
    default warning behavior.
  
    ```javascript
    // next is not called, so no warnings get the default behavior
    Ember.Debug.registerWarnHandler(() => {});
    ```
  
    The handler function takes the following arguments:
  
    <ul>
      <li> <code>message</code> - The message received from the warn call. </li>
      <li> <code>options</code> - An object passed in with the warn call containing additional information including:</li>
        <ul>
          <li> <code>id</code> - An id of the warning in the form of <code>package-name.specific-warning</code>.</li>
        </ul>
      <li> <code>next</code> - A function that calls into the previously registered handler.</li>
    </ul>
  
    @public
    @static
    @method registerWarnHandler
    @param handler {Function} A function to handle warnings.
    @since 2.1.0
  */
  _emberMetalCore.default.Debug.registerWarnHandler = _emberDebugWarn.registerHandler;

  /*
    We are transitioning away from `ember.js` to `ember.debug.js` to make
    it much clearer that it is only for local development purposes.
  
    This flag value is changed by the tooling (by a simple string replacement)
    so that if `ember.js` (which must be output for backwards compat reasons) is
    used a nice helpful warning message will be printed out.
  */
  var runningNonEmberDebugJS = false;
  exports.runningNonEmberDebugJS = runningNonEmberDebugJS;
  if (runningNonEmberDebugJS) {
    _emberMetalDebug.warn('Please use `ember.debug.js` instead of `ember.js` for development and debugging.');
  }
});
enifed('ember-debug/warn', ['exports', 'ember-metal/logger', 'ember-metal/debug', 'ember-debug/handlers'], function (exports, _emberMetalLogger, _emberMetalDebug, _emberDebugHandlers) {
  'use strict';

  var _slice = Array.prototype.slice;
  exports.registerHandler = registerHandler;
  exports.default = warn;

  function registerHandler(handler) {
    _emberDebugHandlers.registerHandler('warn', handler);
  }

  registerHandler(function logWarning(message, options) {
    _emberMetalLogger.default.warn('WARNING: ' + message);
    if ('trace' in _emberMetalLogger.default) {
      _emberMetalLogger.default.trace();
    }
  });

  var missingOptionsDeprecation = 'When calling `Ember.warn` you ' + 'must provide an `options` hash as the third parameter.  ' + '`options` should include an `id` property.';
  exports.missingOptionsDeprecation = missingOptionsDeprecation;
  var missingOptionsIdDeprecation = 'When calling `Ember.warn` you must provide `id` in options.';

  exports.missingOptionsIdDeprecation = missingOptionsIdDeprecation;
  /**
  @module ember
  @submodule ember-debug
  */

  /**
    Display a warning with the provided message.
  
    * In a production build, this method is defined as an empty function (NOP).
    Uses of this method in Ember itself are stripped from the ember.prod.js build.
  
    @method warn
    @param {String} message A warning to display.
    @param {Boolean} test An optional boolean. If falsy, the warning
      will be displayed.
    @param {Object} options An object that can be used to pass a unique
      `id` for this warning.  The `id` can be used by Ember debugging tools
      to change the behavior (raise, log, or silence) for that specific warning.
      The `id` should be namespaced by dots, e.g. "ember-debug.feature-flag-with-features-stripped"
    @for Ember
    @public
  */

  function warn(message, test, options) {
    if (!options) {
      _emberMetalDebug.deprecate(missingOptionsDeprecation, false, {
        id: 'ember-debug.warn-options-missing',
        until: '3.0.0',
        url: 'http://emberjs.com/deprecations/v2.x/#toc_ember-debug-function-options'
      });
    }

    if (options && !options.id) {
      _emberMetalDebug.deprecate(missingOptionsIdDeprecation, false, {
        id: 'ember-debug.warn-id-missing',
        until: '3.0.0',
        url: 'http://emberjs.com/deprecations/v2.x/#toc_ember-debug-function-options'
      });
    }

    _emberDebugHandlers.invoke.apply(undefined, ['warn'].concat(_slice.call(arguments)));
  }
});
enifed('ember-metal/alias', ['exports', 'ember-metal/debug', 'ember-metal/property_get', 'ember-metal/property_set', 'ember-metal/error', 'ember-metal/properties', 'ember-metal/computed', 'ember-metal/utils', 'ember-metal/meta', 'ember-metal/dependent_keys'], function (exports, _emberMetalDebug, _emberMetalProperty_get, _emberMetalProperty_set, _emberMetalError, _emberMetalProperties, _emberMetalComputed, _emberMetalUtils, _emberMetalMeta, _emberMetalDependent_keys) {
  'use strict';

  exports.default = alias;
  exports.AliasedProperty = AliasedProperty;

  function alias(altKey) {
    return new AliasedProperty(altKey);
  }

  function AliasedProperty(altKey) {
    this.isDescriptor = true;
    this.altKey = altKey;
    this._dependentKeys = [altKey];
  }

  AliasedProperty.prototype = Object.create(_emberMetalProperties.Descriptor.prototype);

  AliasedProperty.prototype.get = function AliasedProperty_get(obj, keyName) {
    return _emberMetalProperty_get.get(obj, this.altKey);
  };

  AliasedProperty.prototype.set = function AliasedProperty_set(obj, keyName, value) {
    return _emberMetalProperty_set.set(obj, this.altKey, value);
  };

  AliasedProperty.prototype.willWatch = function (obj, keyName) {
    _emberMetalDependent_keys.addDependentKeys(this, obj, keyName, _emberMetalMeta.meta(obj));
  };

  AliasedProperty.prototype.didUnwatch = function (obj, keyName) {
    _emberMetalDependent_keys.removeDependentKeys(this, obj, keyName, _emberMetalMeta.meta(obj));
  };

  AliasedProperty.prototype.setup = function (obj, keyName) {
    _emberMetalDebug.assert('Setting alias \'' + keyName + '\' on self', this.altKey !== keyName);
    var m = _emberMetalMeta.meta(obj);
    if (m.peekWatching(keyName)) {
      _emberMetalDependent_keys.addDependentKeys(this, obj, keyName, m);
    }
  };

  AliasedProperty.prototype.teardown = function (obj, keyName) {
    var m = _emberMetalMeta.meta(obj);
    if (m.peekWatching(keyName)) {
      _emberMetalDependent_keys.removeDependentKeys(this, obj, keyName, m);
    }
  };

  AliasedProperty.prototype.readOnly = function () {
    this.set = AliasedProperty_readOnlySet;
    return this;
  };

  function AliasedProperty_readOnlySet(obj, keyName, value) {
    throw new _emberMetalError.default('Cannot set read-only property \'' + keyName + '\' on object: ' + _emberMetalUtils.inspect(obj));
  }

  AliasedProperty.prototype.oneWay = function () {
    this.set = AliasedProperty_oneWaySet;
    return this;
  };

  function AliasedProperty_oneWaySet(obj, keyName, value) {
    _emberMetalProperties.defineProperty(obj, keyName, null);
    return _emberMetalProperty_set.set(obj, keyName, value);
  }

  // Backwards compatibility with Ember Data.
  AliasedProperty.prototype._meta = undefined;
  AliasedProperty.prototype.meta = _emberMetalComputed.ComputedProperty.prototype.meta;
});
enifed("ember-metal/assign", ["exports"], function (exports) {
  /**
    Copy properties from a source object to a target object.
  
    ```javascript
    var a = {first: 'Yehuda'};
    var b = {last: 'Katz'};
    var c = {company: 'Tilde Inc.'};
    Ember.assign(a, b, c); // a === {first: 'Yehuda', last: 'Katz', company: 'Tilde Inc.'}, b === {last: 'Katz'}, c === {company: 'Tilde Inc.'}
    ```
  
    @method assign
    @for Ember
    @param {Object} original The object to assign into
    @param {Object} ...args The objects to copy properties from
    @return {Object}
    @public
  */
  "use strict";

  exports.default = assign;

  function assign(original) {
    for (var _len = arguments.length, args = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
      args[_key - 1] = arguments[_key];
    }

    for (var i = 0, l = args.length; i < l; i++) {
      var arg = args[i];
      if (!arg) {
        continue;
      }

      var updates = Object.keys(arg);

      for (var _i = 0, _l = updates.length; _i < _l; _i++) {
        var prop = updates[_i];
        original[prop] = arg[prop];
      }
    }

    return original;
  }
});
enifed('ember-metal/binding', ['exports', 'ember-metal/core', 'ember-metal/logger', 'ember-metal/run_loop', 'ember-metal/debug', 'ember-metal/property_get', 'ember-metal/property_set', 'ember-metal/utils', 'ember-metal/events', 'ember-metal/observer', 'ember-metal/path_cache'], function (exports, _emberMetalCore, _emberMetalLogger, _emberMetalRun_loop, _emberMetalDebug, _emberMetalProperty_get, _emberMetalProperty_set, _emberMetalUtils, _emberMetalEvents, _emberMetalObserver, _emberMetalPath_cache) {
  'use strict';

  exports.bind = bind;

  // ES6TODO: where is Ember.lookup defined?
  /**
  @module ember
  @submodule ember-metal
  */

  // ..........................................................
  // CONSTANTS
  //

  /**
    Debug parameter you can turn on. This will log all bindings that fire to
    the console. This should be disabled in production code. Note that you
    can also enable this from the console or temporarily.
  
    @property LOG_BINDINGS
    @for Ember
    @type Boolean
    @default false
    @public
  */
  _emberMetalCore.default.LOG_BINDINGS = false || !!_emberMetalCore.default.ENV.LOG_BINDINGS;

  // ..........................................................
  // BINDING
  //

  function Binding(toPath, fromPath) {
    // Configuration
    this._from = fromPath;
    this._to = toPath;
    this._oneWay = undefined;

    // State
    this._direction = undefined;
    this._readyToSync = undefined;
    this._fromObj = undefined;
    this._fromPath = undefined;
    this._toObj = undefined;
  }

  /**
    @class Binding
    @namespace Ember
    @public
  */

  Binding.prototype = {
    /**
      This copies the Binding so it can be connected to another object.
       @method copy
      @return {Ember.Binding} `this`
      @public
    */
    copy: function () {
      var copy = new Binding(this._to, this._from);
      if (this._oneWay) {
        copy._oneWay = true;
      }
      return copy;
    },

    // ..........................................................
    // CONFIG
    //

    /**
      This will set `from` property path to the specified value. It will not
      attempt to resolve this property path to an actual object until you
      connect the binding.
       The binding will search for the property path starting at the root object
      you pass when you `connect()` the binding. It follows the same rules as
      `get()` - see that method for more information.
       @method from
      @param {String} path The property path to connect to.
      @return {Ember.Binding} `this`
      @public
    */
    from: function (path) {
      this._from = path;
      return this;
    },

    /**
      This will set the `to` property path to the specified value. It will not
      attempt to resolve this property path to an actual object until you
      connect the binding.
       The binding will search for the property path starting at the root object
      you pass when you `connect()` the binding. It follows the same rules as
      `get()` - see that method for more information.
       @method to
      @param {String|Tuple} path A property path or tuple.
      @return {Ember.Binding} `this`
      @public
    */
    to: function (path) {
      this._to = path;
      return this;
    },

    /**
      Configures the binding as one way. A one-way binding will relay changes
      on the `from` side to the `to` side, but not the other way around. This
      means that if you change the `to` side directly, the `from` side may have
      a different value.
       @method oneWay
      @return {Ember.Binding} `this`
      @public
    */
    oneWay: function () {
      this._oneWay = true;
      return this;
    },

    /**
      @method toString
      @return {String} string representation of binding
      @public
    */
    toString: function () {
      var oneWay = this._oneWay ? '[oneWay]' : '';
      return 'Ember.Binding<' + _emberMetalUtils.guidFor(this) + '>(' + this._from + ' -> ' + this._to + ')' + oneWay;
    },

    // ..........................................................
    // CONNECT AND SYNC
    //

    /**
      Attempts to connect this binding instance so that it can receive and relay
      changes. This method will raise an exception if you have not set the
      from/to properties yet.
       @method connect
      @param {Object} obj The root object for this binding.
      @return {Ember.Binding} `this`
      @public
    */
    connect: function (obj) {
      _emberMetalDebug.assert('Must pass a valid object to Ember.Binding.connect()', !!obj);

      var fromObj = undefined,
          fromPath = undefined;

      // If the binding's "from" path could be interpreted as a global, verify
      // whether the path refers to a global or not by consulting `Ember.lookup`.
      if (_emberMetalPath_cache.isGlobalPath(this._from)) {
        var _name = _emberMetalPath_cache.getFirstKey(this._from);
        var possibleGlobal = _emberMetalCore.default.lookup[_name];

        if (possibleGlobal) {
          fromObj = possibleGlobal;
          fromPath = _emberMetalPath_cache.getTailPath(this._from);
        }
      }

      if (fromObj === undefined) {
        fromObj = obj;
        fromPath = this._from;
      }

      _emberMetalProperty_set.trySet(obj, this._to, _emberMetalProperty_get.get(fromObj, fromPath));

      // Add an observer on the object to be notified when the binding should be updated.
      _emberMetalObserver.addObserver(fromObj, fromPath, this, 'fromDidChange');

      // If the binding is a two-way binding, also set up an observer on the target.
      if (!this._oneWay) {
        _emberMetalObserver.addObserver(obj, this._to, this, 'toDidChange');
      }

      _emberMetalEvents.addListener(obj, 'willDestroy', this, 'disconnect');

      this._readyToSync = true;
      this._fromObj = fromObj;
      this._fromPath = fromPath;
      this._toObj = obj;

      return this;
    },

    /**
      Disconnects the binding instance. Changes will no longer be relayed. You
      will not usually need to call this method.
       @method disconnect
      @return {Ember.Binding} `this`
      @public
    */
    disconnect: function () {
      _emberMetalDebug.assert('Must pass a valid object to Ember.Binding.disconnect()', !!this._toObj);

      // Remove an observer on the object so we're no longer notified of
      // changes that should update bindings.
      _emberMetalObserver.removeObserver(this._fromObj, this._fromPath, this, 'fromDidChange');

      // If the binding is two-way, remove the observer from the target as well.
      if (!this._oneWay) {
        _emberMetalObserver.removeObserver(this._toObj, this._to, this, 'toDidChange');
      }

      this._readyToSync = false; // Disable scheduled syncs...
      return this;
    },

    // ..........................................................
    // PRIVATE
    //

    /* Called when the from side changes. */
    fromDidChange: function (target) {
      this._scheduleSync('fwd');
    },

    /* Called when the to side changes. */
    toDidChange: function (target) {
      this._scheduleSync('back');
    },

    _scheduleSync: function (dir) {
      var existingDir = this._direction;

      // If we haven't scheduled the binding yet, schedule it.
      if (existingDir === undefined) {
        _emberMetalRun_loop.default.schedule('sync', this, '_sync');
        this._direction = dir;
      }

      // If both a 'back' and 'fwd' sync have been scheduled on the same object,
      // default to a 'fwd' sync so that it remains deterministic.
      if (existingDir === 'back' && dir === 'fwd') {
        this._direction = 'fwd';
      }
    },

    _sync: function () {
      var log = _emberMetalCore.default.LOG_BINDINGS;

      var toObj = this._toObj;

      // Don't synchronize destroyed objects or disconnected bindings.
      if (toObj.isDestroyed || !this._readyToSync) {
        return;
      }

      // Get the direction of the binding for the object we are
      // synchronizing from.
      var direction = this._direction;

      var fromObj = this._fromObj;
      var fromPath = this._fromPath;

      this._direction = undefined;

      // If we're synchronizing from the remote object...
      if (direction === 'fwd') {
        var fromValue = _emberMetalProperty_get.get(fromObj, fromPath);
        if (log) {
          _emberMetalLogger.default.log(' ', this.toString(), '->', fromValue, fromObj);
        }
        if (this._oneWay) {
          _emberMetalProperty_set.trySet(toObj, this._to, fromValue);
        } else {
          _emberMetalObserver._suspendObserver(toObj, this._to, this, 'toDidChange', function () {
            _emberMetalProperty_set.trySet(toObj, this._to, fromValue);
          });
        }
        // If we're synchronizing *to* the remote object.
      } else if (direction === 'back') {
          var toValue = _emberMetalProperty_get.get(toObj, this._to);
          if (log) {
            _emberMetalLogger.default.log(' ', this.toString(), '<-', toValue, toObj);
          }
          _emberMetalObserver._suspendObserver(fromObj, fromPath, this, 'fromDidChange', function () {
            _emberMetalProperty_set.trySet(fromObj, fromPath, toValue);
          });
        }
    }

  };

  function mixinProperties(to, from) {
    for (var key in from) {
      if (from.hasOwnProperty(key)) {
        to[key] = from[key];
      }
    }
  }

  mixinProperties(Binding, {

    /*
      See `Ember.Binding.from`.
       @method from
      @static
    */
    from: function (from) {
      var C = this;
      return new C(undefined, from);
    },

    /*
      See `Ember.Binding.to`.
       @method to
      @static
    */
    to: function (to) {
      var C = this;
      return new C(to, undefined);
    }
  });
  /**
    An `Ember.Binding` connects the properties of two objects so that whenever
    the value of one property changes, the other property will be changed also.
  
    ## Automatic Creation of Bindings with `/^*Binding/`-named Properties.
  
    You do not usually create Binding objects directly but instead describe
    bindings in your class or object definition using automatic binding
    detection.
  
    Properties ending in a `Binding` suffix will be converted to `Ember.Binding`
    instances. The value of this property should be a string representing a path
    to another object or a custom binding instance created using Binding helpers
    (see "One Way Bindings"):
  
    ```
    valueBinding: "MyApp.someController.title"
    ```
  
    This will create a binding from `MyApp.someController.title` to the `value`
    property of your object instance automatically. Now the two values will be
    kept in sync.
  
    ## One Way Bindings
  
    One especially useful binding customization you can use is the `oneWay()`
    helper. This helper tells Ember that you are only interested in
    receiving changes on the object you are binding from. For example, if you
    are binding to a preference and you want to be notified if the preference
    has changed, but your object will not be changing the preference itself, you
    could do:
  
    ```
    bigTitlesBinding: Ember.Binding.oneWay("MyApp.preferencesController.bigTitles")
    ```
  
    This way if the value of `MyApp.preferencesController.bigTitles` changes the
    `bigTitles` property of your object will change also. However, if you
    change the value of your `bigTitles` property, it will not update the
    `preferencesController`.
  
    One way bindings are almost twice as fast to setup and twice as fast to
    execute because the binding only has to worry about changes to one side.
  
    You should consider using one way bindings anytime you have an object that
    may be created frequently and you do not intend to change a property; only
    to monitor it for changes (such as in the example above).
  
    ## Adding Bindings Manually
  
    All of the examples above show you how to configure a custom binding, but the
    result of these customizations will be a binding template, not a fully active
    Binding instance. The binding will actually become active only when you
    instantiate the object the binding belongs to. It is useful, however, to
    understand what actually happens when the binding is activated.
  
    For a binding to function it must have at least a `from` property and a `to`
    property. The `from` property path points to the object/key that you want to
    bind from while the `to` path points to the object/key you want to bind to.
  
    When you define a custom binding, you are usually describing the property
    you want to bind from (such as `MyApp.someController.value` in the examples
    above). When your object is created, it will automatically assign the value
    you want to bind `to` based on the name of your binding key. In the
    examples above, during init, Ember objects will effectively call
    something like this on your binding:
  
    ```javascript
    binding = Ember.Binding.from("valueBinding").to("value");
    ```
  
    This creates a new binding instance based on the template you provide, and
    sets the to path to the `value` property of the new object. Now that the
    binding is fully configured with a `from` and a `to`, it simply needs to be
    connected to become active. This is done through the `connect()` method:
  
    ```javascript
    binding.connect(this);
    ```
  
    Note that when you connect a binding you pass the object you want it to be
    connected to. This object will be used as the root for both the from and
    to side of the binding when inspecting relative paths. This allows the
    binding to be automatically inherited by subclassed objects as well.
  
    This also allows you to bind between objects using the paths you declare in
    `from` and `to`:
  
    ```javascript
    // Example 1
    binding = Ember.Binding.from("App.someObject.value").to("value");
    binding.connect(this);
  
    // Example 2
    binding = Ember.Binding.from("parentView.value").to("App.someObject.value");
    binding.connect(this);
    ```
  
    Now that the binding is connected, it will observe both the from and to side
    and relay changes.
  
    If you ever needed to do so (you almost never will, but it is useful to
    understand this anyway), you could manually create an active binding by
    using the `Ember.bind()` helper method. (This is the same method used by
    to setup your bindings on objects):
  
    ```javascript
    Ember.bind(MyApp.anotherObject, "value", "MyApp.someController.value");
    ```
  
    Both of these code fragments have the same effect as doing the most friendly
    form of binding creation like so:
  
    ```javascript
    MyApp.anotherObject = Ember.Object.create({
      valueBinding: "MyApp.someController.value",
  
      // OTHER CODE FOR THIS OBJECT...
    });
    ```
  
    Ember's built in binding creation method makes it easy to automatically
    create bindings for you. You should always use the highest-level APIs
    available, even if you understand how it works underneath.
  
    @class Binding
    @namespace Ember
    @since Ember 0.9
    @public
  */
  // Ember.Binding = Binding; ES6TODO: where to put this?

  /**
    Global helper method to create a new binding. Just pass the root object
    along with a `to` and `from` path to create and connect the binding.
  
    @method bind
    @for Ember
    @param {Object} obj The root object of the transform.
    @param {String} to The path to the 'to' side of the binding.
      Must be relative to obj.
    @param {String} from The path to the 'from' side of the binding.
      Must be relative to obj or a global path.
    @return {Ember.Binding} binding instance
    @public
  */

  function bind(obj, to, from) {
    return new Binding(to, from).connect(obj);
  }

  exports.Binding = Binding;
});
// Ember.LOG_BINDINGS
enifed('ember-metal/cache', ['exports', 'ember-metal/empty_object'], function (exports, _emberMetalEmpty_object) {
  'use strict';

  exports.default = Cache;

  function Cache(limit, func) {
    this.store = new _emberMetalEmpty_object.default();
    this.size = 0;
    this.misses = 0;
    this.hits = 0;
    this.limit = limit;
    this.func = func;
  }

  var UNDEFINED = function () {};

  Cache.prototype = {
    set: function (key, value) {
      if (this.limit > this.size) {
        this.size++;
        if (value === undefined) {
          this.store[key] = UNDEFINED;
        } else {
          this.store[key] = value;
        }
      }

      return value;
    },

    get: function (key) {
      var value = this.store[key];

      if (value === undefined) {
        this.misses++;
        value = this.set(key, this.func(key));
      } else if (value === UNDEFINED) {
        this.hits++;
        value = undefined;
      } else {
        this.hits++;
        // nothing to translate
      }

      return value;
    },

    purge: function () {
      this.store = new _emberMetalEmpty_object.default();
      this.size = 0;
      this.hits = 0;
      this.misses = 0;
    }
  };
});
enifed('ember-metal/chains', ['exports', 'ember-metal/property_get', 'ember-metal/meta', 'ember-metal/watch_key', 'ember-metal/empty_object'], function (exports, _emberMetalProperty_get, _emberMetalMeta, _emberMetalWatch_key, _emberMetalEmpty_object) {
  'use strict';

  exports.finishChains = finishChains;

  var FIRST_KEY = /^([^\.]+)/;

  function firstKey(path) {
    return path.match(FIRST_KEY)[0];
  }

  function isObject(obj) {
    return obj && typeof obj === 'object';
  }

  function isVolatile(obj) {
    return !(isObject(obj) && obj.isDescriptor && obj._volatile === false);
  }

  function ChainWatchers() {
    // chain nodes that reference a key in this obj by key
    // we only create ChainWatchers when we are going to add them
    // so create this upfront
    this.chains = new _emberMetalEmpty_object.default();
  }

  ChainWatchers.prototype = {
    add: function (key, node) {
      var nodes = this.chains[key];
      if (nodes === undefined) {
        this.chains[key] = [node];
      } else {
        nodes.push(node);
      }
    },

    remove: function (key, node) {
      var nodes = this.chains[key];
      if (nodes) {
        for (var i = 0, l = nodes.length; i < l; i++) {
          if (nodes[i] === node) {
            nodes.splice(i, 1);
            break;
          }
        }
      }
    },

    has: function (key, node) {
      var nodes = this.chains[key];
      if (nodes) {
        for (var i = 0, l = nodes.length; i < l; i++) {
          if (nodes[i] === node) {
            return true;
          }
        }
      }
      return false;
    },

    revalidateAll: function () {
      for (var key in this.chains) {
        this.notify(key, true, undefined);
      }
    },

    revalidate: function (key) {
      this.notify(key, true, undefined);
    },

    // key: the string key that is part of a path changed
    // revalidate: boolean; the chains that are watching this value should revalidate
    // callback: function that will be called with the object and path that
    //           will be/are invalidated by this key change, depending on
    //           whether the revalidate flag is passed
    notify: function (key, revalidate, callback) {
      var nodes = this.chains[key];
      if (nodes === undefined || nodes.length === 0) {
        return;
      }

      var affected = undefined;

      if (callback) {
        affected = [];
      }

      for (var i = 0, l = nodes.length; i < l; i++) {
        nodes[i].notify(revalidate, affected);
      }

      if (callback === undefined) {
        return;
      }

      // we gather callbacks so we don't notify them during revalidation
      for (var i = 0, l = affected.length; i < l; i += 2) {
        var obj = affected[i];
        var path = affected[i + 1];
        callback(obj, path);
      }
    }
  };

  function makeChainWatcher() {
    return new ChainWatchers();
  }

  function addChainWatcher(obj, keyName, node) {
    if (!isObject(obj)) {
      return;
    }

    var m = _emberMetalMeta.meta(obj);
    m.writableChainWatchers(makeChainWatcher).add(keyName, node);
    _emberMetalWatch_key.watchKey(obj, keyName, m);
  }

  function removeChainWatcher(obj, keyName, node) {
    if (!isObject(obj)) {
      return;
    }

    var m = _emberMetalMeta.peekMeta(obj);

    if (!m || !m.readableChainWatchers()) {
      return;
    }

    // make meta writable
    m = _emberMetalMeta.meta(obj);

    m.readableChainWatchers().remove(keyName, node);

    _emberMetalWatch_key.unwatchKey(obj, keyName, m);
  }

  // A ChainNode watches a single key on an object. If you provide a starting
  // value for the key then the node won't actually watch it. For a root node
  // pass null for parent and key and object for value.
  function ChainNode(parent, key, value) {
    this._parent = parent;
    this._key = key;

    // _watching is true when calling get(this._parent, this._key) will
    // return the value of this node.
    //
    // It is false for the root of a chain (because we have no parent)
    // and for global paths (because the parent node is the object with
    // the observer on it)
    this._watching = value === undefined;

    this._chains = undefined;
    this._object = undefined;
    this.count = 0;

    this._value = value;
    this._paths = {};
    if (this._watching) {
      this._object = parent.value();
      if (this._object) {
        addChainWatcher(this._object, this._key, this);
      }
    }
  }

  function lazyGet(obj, key) {
    if (!obj) {
      return;
    }

    var meta = _emberMetalMeta.peekMeta(obj);

    // check if object meant only to be a prototype
    if (meta && meta.proto === obj) {
      return;
    }

    // Use `get` if the return value is an EachProxy or an uncacheable value.
    if (isVolatile(obj[key])) {
      return _emberMetalProperty_get.get(obj, key);
      // Otherwise attempt to get the cached value of the computed property
    } else {
        var cache = meta.readableCache();
        if (cache && key in cache) {
          return cache[key];
        }
      }
  }

  ChainNode.prototype = {
    value: function () {
      if (this._value === undefined && this._watching) {
        var obj = this._parent.value();
        this._value = lazyGet(obj, this._key);
      }
      return this._value;
    },

    destroy: function () {
      if (this._watching) {
        var obj = this._object;
        if (obj) {
          removeChainWatcher(obj, this._key, this);
        }
        this._watching = false; // so future calls do nothing
      }
    },

    // copies a top level object only
    copy: function (obj) {
      var ret = new ChainNode(null, null, obj);
      var paths = this._paths;
      var path;

      for (path in paths) {
        // this check will also catch non-number vals.
        if (paths[path] <= 0) {
          continue;
        }
        ret.add(path);
      }
      return ret;
    },

    // called on the root node of a chain to setup watchers on the specified
    // path.
    add: function (path) {
      var paths = this._paths;
      paths[path] = (paths[path] || 0) + 1;

      var key = firstKey(path);
      var tail = path.slice(key.length + 1);

      this.chain(key, tail);
    },

    // called on the root node of a chain to teardown watcher on the specified
    // path
    remove: function (path) {
      var paths = this._paths;
      if (paths[path] > 0) {
        paths[path]--;
      }

      var key = firstKey(path);
      var tail = path.slice(key.length + 1);

      this.unchain(key, tail);
    },

    chain: function (key, path) {
      var chains = this._chains;
      var node;
      if (chains === undefined) {
        chains = this._chains = new _emberMetalEmpty_object.default();
      } else {
        node = chains[key];
      }

      if (node === undefined) {
        node = chains[key] = new ChainNode(this, key, undefined);
      }

      node.count++; // count chains...

      // chain rest of path if there is one
      if (path) {
        key = firstKey(path);
        path = path.slice(key.length + 1);
        node.chain(key, path);
      }
    },

    unchain: function (key, path) {
      var chains = this._chains;
      var node = chains[key];

      // unchain rest of path first...
      if (path && path.length > 1) {
        var nextKey = firstKey(path);
        var nextPath = path.slice(nextKey.length + 1);
        node.unchain(nextKey, nextPath);
      }

      // delete node if needed.
      node.count--;
      if (node.count <= 0) {
        chains[node._key] = undefined;
        node.destroy();
      }
    },

    notify: function (revalidate, affected) {
      if (revalidate && this._watching) {
        var obj = this._parent.value();
        if (obj !== this._object) {
          removeChainWatcher(this._object, this._key, this);
          this._object = obj;
          addChainWatcher(obj, this._key, this);
        }
        this._value = undefined;
      }

      // then notify chains...
      var chains = this._chains;
      var node;
      if (chains) {
        for (var key in chains) {
          node = chains[key];
          if (node !== undefined) {
            node.notify(revalidate, affected);
          }
        }
      }

      if (affected && this._parent) {
        this._parent.populateAffected(this._key, 1, affected);
      }
    },

    populateAffected: function (path, depth, affected) {
      if (this._key) {
        path = this._key + '.' + path;
      }

      if (this._parent) {
        this._parent.populateAffected(path, depth + 1, affected);
      } else {
        if (depth > 1) {
          affected.push(this.value(), path);
        }
      }
    }
  };

  function finishChains(obj) {
    // We only create meta if we really have to
    var m = _emberMetalMeta.peekMeta(obj);
    if (m) {
      m = _emberMetalMeta.meta(obj);

      // finish any current chains node watchers that reference obj
      var chainWatchers = m.readableChainWatchers();
      if (chainWatchers) {
        chainWatchers.revalidateAll();
      }
      // ensure that if we have inherited any chains they have been
      // copied onto our own meta.
      if (m.readableChains()) {
        m.writableChains();
      }
    }
  }

  exports.removeChainWatcher = removeChainWatcher;
  exports.ChainNode = ChainNode;
});
enifed('ember-metal/computed', ['exports', 'ember-metal/debug', 'ember-metal/property_set', 'ember-metal/utils', 'ember-metal/meta', 'ember-metal/expand_properties', 'ember-metal/error', 'ember-metal/properties', 'ember-metal/property_events', 'ember-metal/dependent_keys'], function (exports, _emberMetalDebug, _emberMetalProperty_set, _emberMetalUtils, _emberMetalMeta, _emberMetalExpand_properties, _emberMetalError, _emberMetalProperties, _emberMetalProperty_events, _emberMetalDependent_keys) {
  'use strict';

  exports.default = computed;

  /**
  @module ember
  @submodule ember-metal
  */

  function UNDEFINED() {}

  var DEEP_EACH_REGEX = /\.@each\.[^.]+\./;

  /**
    A computed property transforms an object literal with object's accessor function(s) into a property.
  
    By default the function backing the computed property will only be called
    once and the result will be cached. You can specify various properties
    that your computed property depends on. This will force the cached
    result to be recomputed if the dependencies are modified.
  
    In the following example we declare a computed property - `fullName` - by calling
    `.Ember.computed()` with property dependencies (`firstName` and `lastName`) as leading arguments and getter accessor function. The `fullName` getter function
    will be called once (regardless of how many times it is accessed) as long
    as its dependencies have not changed. Once `firstName` or `lastName` are updated
    any future calls (or anything bound) to `fullName` will incorporate the new
    values.
  
    ```javascript
    let Person = Ember.Object.extend({
      // these will be supplied by `create`
      firstName: null,
      lastName: null,
  
      fullName: Ember.computed('firstName', 'lastName', function() {
        let firstName = this.get('firstName'),
            lastName  = this.get('lastName');
  
        return firstName + ' ' + lastName;
      })
    });
  
    let tom = Person.create({
      firstName: 'Tom',
      lastName: 'Dale'
    });
  
    tom.get('fullName') // 'Tom Dale'
    ```
  
    You can also define what Ember should do when setting a computed property by providing additional function (`set`) in hash argument.
    If you try to set a computed property, it will try to invoke setter accessor function with the key and
    value you want to set it to as arguments.
  
    ```javascript
    let Person = Ember.Object.extend({
      // these will be supplied by `create`
      firstName: null,
      lastName: null,
  
      fullName: Ember.computed('firstName', 'lastName', {
        get(key) {
          let firstName = this.get('firstName'),
              lastName  = this.get('lastName');
  
          return firstName + ' ' + lastName;
        },
        set(key, value) {
          let [firstName, lastName] = value.split(' ');
  
          this.set('firstName', firstName);
          this.set('lastName', lastName);
  
          return value;
        }
      })
    });
  
    let person = Person.create();
  
    person.set('fullName', 'Peter Wagenet');
    person.get('firstName'); // 'Peter'
    person.get('lastName');  // 'Wagenet'
    ```
  
    You can overwrite computed property with normal property (no longer computed), that won't change if dependencies change, if you set computed property and it won't have setter accessor function defined.
  
    You can also mark computed property as `.readOnly()` and block all attempts to set it.
  
    ```javascript
    let Person = Ember.Object.extend({
      // these will be supplied by `create`
      firstName: null,
      lastName: null,
  
      fullName: Ember.computed('firstName', 'lastName', {
        get(key) {
          let firstName = this.get('firstName');
          let lastName  = this.get('lastName');
  
          return firstName + ' ' + lastName;
        }
      }).readOnly()
    });
  
    let person = Person.create();
    person.set('fullName', 'Peter Wagenet'); // Uncaught Error: Cannot set read-only property "fullName" on object: <(...):emberXXX>
    ```
  
    Additional resources:
    - [New CP syntax RFC](https://github.com/emberjs/rfcs/blob/master/text/0011-improved-cp-syntax.md)
    - [New computed syntax explained in "Ember 1.12 released" ](http://emberjs.com/blog/2015/05/13/ember-1-12-released.html#toc_new-computed-syntax)
  
    @class ComputedProperty
    @namespace Ember
    @public
  */
  function ComputedProperty(config, opts) {
    this.isDescriptor = true;
    if (typeof config === 'function') {
      this._getter = config;
    } else {
      _emberMetalDebug.assert('Ember.computed expects a function or an object as last argument.', typeof config === 'object' && !Array.isArray(config));
      _emberMetalDebug.assert('Config object pased to a Ember.computed can only contain `get` or `set` keys.', (function () {
        var keys = Object.keys(config);
        for (var i = 0; i < keys.length; i++) {
          if (keys[i] !== 'get' && keys[i] !== 'set') {
            return false;
          }
        }
        return true;
      })());
      this._getter = config.get;
      this._setter = config.set;
    }
    _emberMetalDebug.assert('Computed properties must receive a getter or a setter, you passed none.', !!this._getter || !!this._setter);
    this._dependentKeys = undefined;
    this._suspended = undefined;
    this._meta = undefined;
    this._volatile = false;
    this._dependentKeys = opts && opts.dependentKeys;
    this._readOnly = false;
  }

  ComputedProperty.prototype = new _emberMetalProperties.Descriptor();

  var ComputedPropertyPrototype = ComputedProperty.prototype;

  /**
    Call on a computed property to set it into non-cached mode. When in this
    mode the computed property will not automatically cache the return value.
  
    It also does not automatically fire any change events. You must manually notify
    any changes if you want to observe this property.
  
    Dependency keys have no effect on volatile properties as they are for cache
    invalidation and notification when cached value is invalidated.
  
    ```javascript
    let outsideService = Ember.Object.extend({
      value: Ember.computed(function() {
        return OutsideService.getValue();
      }).volatile()
    }).create();
    ```
  
    @method volatile
    @return {Ember.ComputedProperty} this
    @chainable
    @public
  */
  ComputedPropertyPrototype.volatile = function () {
    this._volatile = true;
    return this;
  };

  /**
    Call on a computed property to set it into read-only mode. When in this
    mode the computed property will throw an error when set.
  
    ```javascript
    let Person = Ember.Object.extend({
      guid: Ember.computed(function() {
        return 'guid-guid-guid';
      }).readOnly()
    });
  
    let person = Person.create();
  
    person.set('guid', 'new-guid'); // will throw an exception
    ```
  
    @method readOnly
    @return {Ember.ComputedProperty} this
    @chainable
    @public
  */
  ComputedPropertyPrototype.readOnly = function () {
    this._readOnly = true;
    _emberMetalDebug.assert('Computed properties that define a setter using the new syntax cannot be read-only', !(this._readOnly && this._setter && this._setter !== this._getter));
    return this;
  };

  /**
    Sets the dependent keys on this computed property. Pass any number of
    arguments containing key paths that this computed property depends on.
  
    ```javascript
    let President = Ember.Object.extend({
      fullName: Ember.computed(function() {
        return this.get('firstName') + ' ' + this.get('lastName');
  
        // Tell Ember that this computed property depends on firstName
        // and lastName
      }).property('firstName', 'lastName')
    });
  
    let president = President.create({
      firstName: 'Barack',
      lastName: 'Obama'
    });
  
    president.get('fullName'); // 'Barack Obama'
    ```
  
    @method property
    @param {String} path* zero or more property paths
    @return {Ember.ComputedProperty} this
    @chainable
    @public
  */
  ComputedPropertyPrototype.property = function () {
    var args;

    var addArg = function (property) {
      _emberMetalDebug.warn('Dependent keys containing @each only work one level deep. ' + 'You cannot use nested forms like todos.@each.owner.name or todos.@each.owner.@each.name. ' + 'Please create an intermediary computed property.', DEEP_EACH_REGEX.test(property) === false, { id: 'ember-metal.computed-deep-each' });
      args.push(property);
    };

    args = [];
    for (var i = 0, l = arguments.length; i < l; i++) {
      _emberMetalExpand_properties.default(arguments[i], addArg);
    }

    this._dependentKeys = args;
    return this;
  };

  /**
    In some cases, you may want to annotate computed properties with additional
    metadata about how they function or what values they operate on. For example,
    computed property functions may close over variables that are then no longer
    available for introspection.
  
    You can pass a hash of these values to a computed property like this:
  
    ```
    person: Ember.computed(function() {
      let personId = this.get('personId');
      return App.Person.create({ id: personId });
    }).meta({ type: App.Person })
    ```
  
    The hash that you pass to the `meta()` function will be saved on the
    computed property descriptor under the `_meta` key. Ember runtime
    exposes a public API for retrieving these values from classes,
    via the `metaForProperty()` function.
  
    @method meta
    @param {Object} meta
    @chainable
    @public
  */
  ComputedPropertyPrototype.meta = function (meta) {
    if (arguments.length === 0) {
      return this._meta || {};
    } else {
      this._meta = meta;
      return this;
    }
  };

  // invalidate cache when CP key changes
  ComputedPropertyPrototype.didChange = function (obj, keyName) {
    // _suspended is set via a CP.set to ensure we don't clear
    // the cached value set by the setter
    if (this._volatile || this._suspended === obj) {
      return;
    }

    // don't create objects just to invalidate
    var meta = _emberMetalMeta.peekMeta(obj);
    if (!meta || meta.source !== obj) {
      return;
    }

    var cache = meta.readableCache();
    if (cache && cache[keyName] !== undefined) {
      cache[keyName] = undefined;
      _emberMetalDependent_keys.removeDependentKeys(this, obj, keyName, meta);
    }
  };

  ComputedPropertyPrototype.get = function (obj, keyName) {
    if (this._volatile) {
      return this._getter.call(obj, keyName);
    }

    var meta = _emberMetalMeta.meta(obj);
    var cache = meta.writableCache();

    var result = cache[keyName];
    if (result === UNDEFINED) {
      return undefined;
    } else if (result !== undefined) {
      return result;
    }

    var ret = this._getter.call(obj, keyName);
    if (ret === undefined) {
      cache[keyName] = UNDEFINED;
    } else {
      cache[keyName] = ret;
    }

    var chainWatchers = meta.readableChainWatchers();
    if (chainWatchers) {
      chainWatchers.revalidate(keyName);
    }
    _emberMetalDependent_keys.addDependentKeys(this, obj, keyName, meta);

    return ret;
  };

  ComputedPropertyPrototype.set = function computedPropertySetEntry(obj, keyName, value) {
    if (this._readOnly) {
      this._throwReadOnlyError(obj, keyName);
    }

    if (!this._setter) {
      return this.clobberSet(obj, keyName, value);
    }

    if (this._volatile) {
      return this.volatileSet(obj, keyName, value);
    }

    return this.setWithSuspend(obj, keyName, value);
  };

  ComputedPropertyPrototype._throwReadOnlyError = function computedPropertyThrowReadOnlyError(obj, keyName) {
    throw new _emberMetalError.default('Cannot set read-only property "' + keyName + '" on object: ' + _emberMetalUtils.inspect(obj));
  };

  ComputedPropertyPrototype.clobberSet = function computedPropertyClobberSet(obj, keyName, value) {
    var cachedValue = cacheFor(obj, keyName);
    _emberMetalProperties.defineProperty(obj, keyName, null, cachedValue);
    _emberMetalProperty_set.set(obj, keyName, value);
    return value;
  };

  ComputedPropertyPrototype.volatileSet = function computedPropertyVolatileSet(obj, keyName, value) {
    return this._setter.call(obj, keyName, value);
  };

  ComputedPropertyPrototype.setWithSuspend = function computedPropertySetWithSuspend(obj, keyName, value) {
    var oldSuspended = this._suspended;
    this._suspended = obj;
    try {
      return this._set(obj, keyName, value);
    } finally {
      this._suspended = oldSuspended;
    }
  };

  ComputedPropertyPrototype._set = function computedPropertySet(obj, keyName, value) {
    // cache requires own meta
    var meta = _emberMetalMeta.meta(obj);
    // either there is a writable cache or we need one to update
    var cache = meta.writableCache();
    var hadCachedValue = false;
    var cachedValue = undefined;
    if (cache[keyName] !== undefined) {
      if (cache[keyName] !== UNDEFINED) {
        cachedValue = cache[keyName];
      }
      hadCachedValue = true;
    }

    var ret = this._setter.call(obj, keyName, value, cachedValue);

    // allows setter to return the same value that is cached already
    if (hadCachedValue && cachedValue === ret) {
      return ret;
    }

    var watched = meta.peekWatching(keyName);
    if (watched) {
      _emberMetalProperty_events.propertyWillChange(obj, keyName);
    }

    if (hadCachedValue) {
      cache[keyName] = undefined;
    }

    if (!hadCachedValue) {
      _emberMetalDependent_keys.addDependentKeys(this, obj, keyName, meta);
    }

    if (ret === undefined) {
      cache[keyName] = UNDEFINED;
    } else {
      cache[keyName] = ret;
    }

    if (watched) {
      _emberMetalProperty_events.propertyDidChange(obj, keyName);
    }

    return ret;
  };

  /* called before property is overridden */
  ComputedPropertyPrototype.teardown = function (obj, keyName) {
    if (this._volatile) {
      return;
    }
    var meta = _emberMetalMeta.meta(obj);
    var cache = meta.readableCache();
    if (cache && cache[keyName] !== undefined) {
      _emberMetalDependent_keys.removeDependentKeys(this, obj, keyName, meta);
      cache[keyName] = undefined;
    }
  };

  /**
    This helper returns a new property descriptor that wraps the passed
    computed property function. You can use this helper to define properties
    with mixins or via `Ember.defineProperty()`.
  
    If you pass a function as an argument, it will be used as a getter. A computed
    property defined in this way might look like this:
  
    ```js
    let Person = Ember.Object.extend({
      init() {
        this._super(...arguments);
  
        this.firstName = 'Betty';
        this.lastName = 'Jones';
      },
  
      fullName: Ember.computed('firstName', 'lastName', function() {
        return `${this.get('firstName')} ${this.get('lastName')}`;
      })
    });
  
    let client = Person.create();
  
    client.get('fullName'); // 'Betty Jones'
  
    client.set('lastName', 'Fuller');
    client.get('fullName'); // 'Betty Fuller'
    ```
  
    You can pass a hash with two functions, `get` and `set`, as an
    argument to provide both a getter and setter:
  
    ```js
    let Person = Ember.Object.extend({
      init() {
        this._super(...arguments);
  
        this.firstName = 'Betty';
        this.lastName = 'Jones';
      },
  
      fullName: Ember.computed('firstName', 'lastName', {
        get(key) {
          return `${this.get('firstName')} ${this.get('lastName')}`;
        },
        set(key, value) {
          let [firstName, lastName] = value.split(/\s+/);
          this.setProperties({ firstName, lastName });
          return value;
        }
      });
    })
  
    let client = Person.create();
    client.get('firstName'); // 'Betty'
  
    client.set('fullName', 'Carroll Fuller');
    client.get('firstName'); // 'Carroll'
    ```
  
    The `set` function should accept two parameters, `key` and `value`. The value
    returned from `set` will be the new value of the property.
  
    _Note: This is the preferred way to define computed properties when writing third-party
    libraries that depend on or use Ember, since there is no guarantee that the user
    will have [prototype Extensions](http://emberjs.com/guides/configuring-ember/disabling-prototype-extensions/) enabled._
  
    The alternative syntax, with prototype extensions, might look like:
  
    ```js
    fullName: function() {
      return this.get('firstName') + ' ' + this.get('lastName');
    }.property('firstName', 'lastName')
    ```
  
    @class computed
    @namespace Ember
    @constructor
    @static
    @param {String} [dependentKeys*] Optional dependent keys that trigger this computed property.
    @param {Function} func The computed property function.
    @return {Ember.ComputedProperty} property descriptor instance
    @public
  */

  function computed(func) {
    var args;

    if (arguments.length > 1) {
      args = [].slice.call(arguments);
      func = args.pop();
    }

    var cp = new ComputedProperty(func);

    if (args) {
      cp.property.apply(cp, args);
    }

    return cp;
  }

  /**
    Returns the cached value for a property, if one exists.
    This can be useful for peeking at the value of a computed
    property that is generated lazily, without accidentally causing
    it to be created.
  
    @method cacheFor
    @for Ember
    @param {Object} obj the object whose property you want to check
    @param {String} key the name of the property whose cached value you want
      to return
    @return {Object} the cached value
    @public
  */
  function cacheFor(obj, key) {
    var meta = _emberMetalMeta.peekMeta(obj);
    var cache = meta && meta.source === obj && meta.readableCache();
    var ret = cache && cache[key];

    if (ret === UNDEFINED) {
      return undefined;
    }
    return ret;
  }

  cacheFor.set = function (cache, key, value) {
    if (value === undefined) {
      cache[key] = UNDEFINED;
    } else {
      cache[key] = value;
    }
  };

  cacheFor.get = function (cache, key) {
    var ret = cache[key];
    if (ret === UNDEFINED) {
      return undefined;
    }
    return ret;
  };

  cacheFor.remove = function (cache, key) {
    cache[key] = undefined;
  };

  exports.ComputedProperty = ComputedProperty;
  exports.computed = computed;
  exports.cacheFor = cacheFor;
});
enifed('ember-metal/computed_macros', ['exports', 'ember-metal/debug', 'ember-metal/property_get', 'ember-metal/property_set', 'ember-metal/computed', 'ember-metal/is_empty', 'ember-metal/is_none', 'ember-metal/alias', 'ember-metal/expand_properties'], function (exports, _emberMetalDebug, _emberMetalProperty_get, _emberMetalProperty_set, _emberMetalComputed, _emberMetalIs_empty, _emberMetalIs_none, _emberMetalAlias, _emberMetalExpand_properties) {
  'use strict';

  exports.empty = empty;
  exports.notEmpty = notEmpty;
  exports.none = none;
  exports.not = not;
  exports.bool = bool;
  exports.match = match;
  exports.equal = equal;
  exports.gt = gt;
  exports.gte = gte;
  exports.lt = lt;
  exports.lte = lte;
  exports.oneWay = oneWay;
  exports.readOnly = readOnly;
  exports.deprecatingAlias = deprecatingAlias;

  /**
  @module ember
  @submodule ember-metal
  */

  function getProperties(self, propertyNames) {
    var ret = {};
    for (var i = 0; i < propertyNames.length; i++) {
      ret[propertyNames[i]] = _emberMetalProperty_get.get(self, propertyNames[i]);
    }
    return ret;
  }

  function generateComputedWithProperties(macro) {
    return function () {
      var expandedProperties = [];
      var computedFunc = _emberMetalComputed.computed(function () {
        return macro.apply(this, [getProperties(this, expandedProperties)]);
      });

      function extractProperty(entry) {
        expandedProperties.push(entry);
      }

      for (var _len = arguments.length, properties = Array(_len), _key = 0; _key < _len; _key++) {
        properties[_key] = arguments[_key];
      }

      for (var i = 0; i < properties.length; i++) {
        _emberMetalExpand_properties.default(properties[i], extractProperty);
      }

      return computedFunc.property.apply(computedFunc, expandedProperties);
    };
  }

  /**
    A computed property that returns true if the value of the dependent
    property is null, an empty string, empty array, or empty function.
  
    Example
  
    ```javascript
    var ToDoList = Ember.Object.extend({
      isDone: Ember.computed.empty('todos')
    });
  
    var todoList = ToDoList.create({
      todos: ['Unit Test', 'Documentation', 'Release']
    });
  
    todoList.get('isDone'); // false
    todoList.get('todos').clear();
    todoList.get('isDone'); // true
    ```
  
    @since 1.6.0
    @method empty
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which negate
    the original value for property
    @public
  */

  function empty(dependentKey) {
    return _emberMetalComputed.computed(dependentKey + '.length', function () {
      return _emberMetalIs_empty.default(_emberMetalProperty_get.get(this, dependentKey));
    });
  }

  /**
    A computed property that returns true if the value of the dependent
    property is NOT null, an empty string, empty array, or empty function.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      hasStuff: Ember.computed.notEmpty('backpack')
    });
  
    var hamster = Hamster.create({ backpack: ['Food', 'Sleeping Bag', 'Tent'] });
  
    hamster.get('hasStuff');         // true
    hamster.get('backpack').clear(); // []
    hamster.get('hasStuff');         // false
    ```
  
    @method notEmpty
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which returns true if
    original value for property is not empty.
    @public
  */

  function notEmpty(dependentKey) {
    return _emberMetalComputed.computed(dependentKey + '.length', function () {
      return !_emberMetalIs_empty.default(_emberMetalProperty_get.get(this, dependentKey));
    });
  }

  /**
    A computed property that returns true if the value of the dependent
    property is null or undefined. This avoids errors from JSLint complaining
    about use of ==, which can be technically confusing.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      isHungry: Ember.computed.none('food')
    });
  
    var hamster = Hamster.create();
  
    hamster.get('isHungry'); // true
    hamster.set('food', 'Banana');
    hamster.get('isHungry'); // false
    hamster.set('food', null);
    hamster.get('isHungry'); // true
    ```
  
    @method none
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which
    returns true if original value for property is null or undefined.
    @public
  */

  function none(dependentKey) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return _emberMetalIs_none.default(_emberMetalProperty_get.get(this, dependentKey));
    });
  }

  /**
    A computed property that returns the inverse boolean value
    of the original value for the dependent property.
  
    Example
  
    ```javascript
    var User = Ember.Object.extend({
      isAnonymous: Ember.computed.not('loggedIn')
    });
  
    var user = User.create({loggedIn: false});
  
    user.get('isAnonymous'); // true
    user.set('loggedIn', true);
    user.get('isAnonymous'); // false
    ```
  
    @method not
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which returns
    inverse of the original value for property
    @public
  */

  function not(dependentKey) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return !_emberMetalProperty_get.get(this, dependentKey);
    });
  }

  /**
    A computed property that converts the provided dependent property
    into a boolean value.
  
    ```javascript
    var Hamster = Ember.Object.extend({
      hasBananas: Ember.computed.bool('numBananas')
    });
  
    var hamster = Hamster.create();
  
    hamster.get('hasBananas'); // false
    hamster.set('numBananas', 0);
    hamster.get('hasBananas'); // false
    hamster.set('numBananas', 1);
    hamster.get('hasBananas'); // true
    hamster.set('numBananas', null);
    hamster.get('hasBananas'); // false
    ```
  
    @method bool
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which converts
    to boolean the original value for property
    @public
  */

  function bool(dependentKey) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return !!_emberMetalProperty_get.get(this, dependentKey);
    });
  }

  /**
    A computed property which matches the original value for the
    dependent property against a given RegExp, returning `true`
    if the value matches the RegExp and `false` if it does not.
  
    Example
  
    ```javascript
    var User = Ember.Object.extend({
      hasValidEmail: Ember.computed.match('email', /^.+@.+\..+$/)
    });
  
    var user = User.create({loggedIn: false});
  
    user.get('hasValidEmail'); // false
    user.set('email', '');
    user.get('hasValidEmail'); // false
    user.set('email', 'ember_hamster@example.com');
    user.get('hasValidEmail'); // true
    ```
  
    @method match
    @for Ember.computed
    @param {String} dependentKey
    @param {RegExp} regexp
    @return {Ember.ComputedProperty} computed property which match
    the original value for property against a given RegExp
    @public
  */

  function match(dependentKey, regexp) {
    return _emberMetalComputed.computed(dependentKey, function () {
      var value = _emberMetalProperty_get.get(this, dependentKey);

      return typeof value === 'string' ? regexp.test(value) : false;
    });
  }

  /**
    A computed property that returns true if the provided dependent property
    is equal to the given value.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      napTime: Ember.computed.equal('state', 'sleepy')
    });
  
    var hamster = Hamster.create();
  
    hamster.get('napTime'); // false
    hamster.set('state', 'sleepy');
    hamster.get('napTime'); // true
    hamster.set('state', 'hungry');
    hamster.get('napTime'); // false
    ```
  
    @method equal
    @for Ember.computed
    @param {String} dependentKey
    @param {String|Number|Object} value
    @return {Ember.ComputedProperty} computed property which returns true if
    the original value for property is equal to the given value.
    @public
  */

  function equal(dependentKey, value) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return _emberMetalProperty_get.get(this, dependentKey) === value;
    });
  }

  /**
    A computed property that returns true if the provided dependent property
    is greater than the provided value.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      hasTooManyBananas: Ember.computed.gt('numBananas', 10)
    });
  
    var hamster = Hamster.create();
  
    hamster.get('hasTooManyBananas'); // false
    hamster.set('numBananas', 3);
    hamster.get('hasTooManyBananas'); // false
    hamster.set('numBananas', 11);
    hamster.get('hasTooManyBananas'); // true
    ```
  
    @method gt
    @for Ember.computed
    @param {String} dependentKey
    @param {Number} value
    @return {Ember.ComputedProperty} computed property which returns true if
    the original value for property is greater than given value.
    @public
  */

  function gt(dependentKey, value) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return _emberMetalProperty_get.get(this, dependentKey) > value;
    });
  }

  /**
    A computed property that returns true if the provided dependent property
    is greater than or equal to the provided value.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      hasTooManyBananas: Ember.computed.gte('numBananas', 10)
    });
  
    var hamster = Hamster.create();
  
    hamster.get('hasTooManyBananas'); // false
    hamster.set('numBananas', 3);
    hamster.get('hasTooManyBananas'); // false
    hamster.set('numBananas', 10);
    hamster.get('hasTooManyBananas'); // true
    ```
  
    @method gte
    @for Ember.computed
    @param {String} dependentKey
    @param {Number} value
    @return {Ember.ComputedProperty} computed property which returns true if
    the original value for property is greater or equal then given value.
    @public
  */

  function gte(dependentKey, value) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return _emberMetalProperty_get.get(this, dependentKey) >= value;
    });
  }

  /**
    A computed property that returns true if the provided dependent property
    is less than the provided value.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      needsMoreBananas: Ember.computed.lt('numBananas', 3)
    });
  
    var hamster = Hamster.create();
  
    hamster.get('needsMoreBananas'); // true
    hamster.set('numBananas', 3);
    hamster.get('needsMoreBananas'); // false
    hamster.set('numBananas', 2);
    hamster.get('needsMoreBananas'); // true
    ```
  
    @method lt
    @for Ember.computed
    @param {String} dependentKey
    @param {Number} value
    @return {Ember.ComputedProperty} computed property which returns true if
    the original value for property is less then given value.
    @public
  */

  function lt(dependentKey, value) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return _emberMetalProperty_get.get(this, dependentKey) < value;
    });
  }

  /**
    A computed property that returns true if the provided dependent property
    is less than or equal to the provided value.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      needsMoreBananas: Ember.computed.lte('numBananas', 3)
    });
  
    var hamster = Hamster.create();
  
    hamster.get('needsMoreBananas'); // true
    hamster.set('numBananas', 5);
    hamster.get('needsMoreBananas'); // false
    hamster.set('numBananas', 3);
    hamster.get('needsMoreBananas'); // true
    ```
  
    @method lte
    @for Ember.computed
    @param {String} dependentKey
    @param {Number} value
    @return {Ember.ComputedProperty} computed property which returns true if
    the original value for property is less or equal than given value.
    @public
  */

  function lte(dependentKey, value) {
    return _emberMetalComputed.computed(dependentKey, function () {
      return _emberMetalProperty_get.get(this, dependentKey) <= value;
    });
  }

  /**
    A computed property that performs a logical `and` on the
    original values for the provided dependent properties.
  
    You may pass in more than two properties and even use
    property brace expansion.  The computed property will
    returns the first falsy value or last truthy value
    just like JavaScript's `||` operator.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      readyForCamp: Ember.computed.and('hasTent', 'hasBackpack'),
      readyForHike: Ember.computed.and('hasWalkingStick', 'hasBackpack')
    });
  
    var tomster = Hamster.create();
  
    tomster.get('readyForCamp'); // false
    tomster.set('hasTent', true);
    tomster.get('readyForCamp'); // false
    tomster.set('hasBackpack', true);
    tomster.get('readyForCamp'); // true
    tomster.set('hasBackpack', 'Yes');
    tomster.get('readyForCamp'); // 'Yes'
    tomster.set('hasWalkingStick', null);
    tomster.get('readyForHike'); // null
    ```
  
    @method and
    @for Ember.computed
    @param {String} dependentKey*
    @return {Ember.ComputedProperty} computed property which performs
    a logical `and` on the values of all the original values for properties.
    @public
  */
  var and = generateComputedWithProperties(function (properties) {
    var value;
    for (var key in properties) {
      value = properties[key];
      if (properties.hasOwnProperty(key) && !value) {
        return value;
      }
    }
    return value;
  });

  exports.and = and;
  /**
    A computed property which performs a logical `or` on the
    original values for the provided dependent properties.
  
    You may pass in more than two properties and even use
    property brace expansion.  The computed property will
    returns the first truthy value or last falsy value just
    like JavaScript's `||` operator.
  
    Example
  
    ```javascript
    var Hamster = Ember.Object.extend({
      readyForRain: Ember.computed.or('hasJacket', 'hasUmbrella'),
      readyForBeach: Ember.computed.or('{hasSunscreen,hasUmbrella}')
    });
  
    var tomster = Hamster.create();
  
    tomster.get('readyForRain'); // undefined
    tomster.set('hasUmbrella', true);
    tomster.get('readyForRain'); // true
    tomster.set('hasJacket', 'Yes');
    tomster.get('readyForRain'); // 'Yes'
    tomster.set('hasSunscreen', 'Check');
    tomster.get('readyForBeach'); // 'Check'
    ```
  
    @method or
    @for Ember.computed
    @param {String} dependentKey*
    @return {Ember.ComputedProperty} computed property which performs
    a logical `or` on the values of all the original values for properties.
    @public
  */
  var or = generateComputedWithProperties(function (properties) {
    var value;
    for (var key in properties) {
      value = properties[key];
      if (properties.hasOwnProperty(key) && value) {
        return value;
      }
    }
    return value;
  });

  exports.or = or;
  /**
    Creates a new property that is an alias for another property
    on an object. Calls to `get` or `set` this property behave as
    though they were called on the original property.
  
    ```javascript
    var Person = Ember.Object.extend({
      name: 'Alex Matchneer',
      nomen: Ember.computed.alias('name')
    });
  
    var alex = Person.create();
  
    alex.get('nomen'); // 'Alex Matchneer'
    alex.get('name');  // 'Alex Matchneer'
  
    alex.set('nomen', '@machty');
    alex.get('name');  // '@machty'
    ```
  
    @method alias
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which creates an
    alias to the original value for property.
    @public
  */

  /**
    Where `computed.alias` aliases `get` and `set`, and allows for bidirectional
    data flow, `computed.oneWay` only provides an aliased `get`. The `set` will
    not mutate the upstream property, rather causes the current property to
    become the value set. This causes the downstream property to permanently
    diverge from the upstream property.
  
    Example
  
    ```javascript
    var User = Ember.Object.extend({
      firstName: null,
      lastName: null,
      nickName: Ember.computed.oneWay('firstName')
    });
  
    var teddy = User.create({
      firstName: 'Teddy',
      lastName:  'Zeenny'
    });
  
    teddy.get('nickName');              // 'Teddy'
    teddy.set('nickName', 'TeddyBear'); // 'TeddyBear'
    teddy.get('firstName');             // 'Teddy'
    ```
  
    @method oneWay
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which creates a
    one way computed property to the original value for property.
    @public
  */

  function oneWay(dependentKey) {
    return _emberMetalAlias.default(dependentKey).oneWay();
  }

  /**
    This is a more semantically meaningful alias of `computed.oneWay`,
    whose name is somewhat ambiguous as to which direction the data flows.
  
    @method reads
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which creates a
      one way computed property to the original value for property.
    @public
   */

  /**
    Where `computed.oneWay` provides oneWay bindings, `computed.readOnly` provides
    a readOnly one way binding. Very often when using `computed.oneWay` one does
    not also want changes to propagate back up, as they will replace the value.
  
    This prevents the reverse flow, and also throws an exception when it occurs.
  
    Example
  
    ```javascript
    var User = Ember.Object.extend({
      firstName: null,
      lastName: null,
      nickName: Ember.computed.readOnly('firstName')
    });
  
    var teddy = User.create({
      firstName: 'Teddy',
      lastName:  'Zeenny'
    });
  
    teddy.get('nickName');              // 'Teddy'
    teddy.set('nickName', 'TeddyBear'); // throws Exception
    // throw new Ember.Error('Cannot Set: nickName on: <User:ember27288>' );`
    teddy.get('firstName');             // 'Teddy'
    ```
  
    @method readOnly
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which creates a
    one way computed property to the original value for property.
    @since 1.5.0
    @public
  */

  function readOnly(dependentKey) {
    return _emberMetalAlias.default(dependentKey).readOnly();
  }

  /**
    Creates a new property that is an alias for another property
    on an object. Calls to `get` or `set` this property behave as
    though they were called on the original property, but also
    print a deprecation warning.
  
    @method deprecatingAlias
    @for Ember.computed
    @param {String} dependentKey
    @return {Ember.ComputedProperty} computed property which creates an
    alias with a deprecation to the original value for property.
    @since 1.7.0
    @public
  */

  function deprecatingAlias(dependentKey, options) {
    return _emberMetalComputed.computed(dependentKey, {
      get: function (key) {
        _emberMetalDebug.deprecate('Usage of `' + key + '` is deprecated, use `' + dependentKey + '` instead.', false, options);
        return _emberMetalProperty_get.get(this, dependentKey);
      },
      set: function (key, value) {
        _emberMetalDebug.deprecate('Usage of `' + key + '` is deprecated, use `' + dependentKey + '` instead.', false, options);
        _emberMetalProperty_set.set(this, dependentKey, value);
        return value;
      }
    });
  }
});
enifed('ember-metal/core', ['exports', 'require'], function (exports, _require) {
  /*globals Ember:true,ENV,EmberENV */

  'use strict';

  /**
  @module ember
  @submodule ember-metal
  */

  /**
    This namespace contains all Ember methods and functions. Future versions of
    Ember may overwrite this namespace and therefore, you should avoid adding any
    new properties.
  
    At the heart of Ember is Ember-Runtime, a set of core functions that provide
    cross-platform compatibility and object property observing.  Ember-Runtime is
    small and performance-focused so you can use it alongside other
    cross-platform libraries such as jQuery. For more details, see
    [Ember-Runtime](http://emberjs.com/api/modules/ember-runtime.html).
  
    @class Ember
    @static
    @version 2.6.2
    @public
  */

  if ('undefined' === typeof Ember) {
    // Create core object. Make it act like an instance of Ember.Namespace so that
    // objects assigned to it are given a sane string representation.
    Ember = {};
  }

  // Default imports, exports and lookup to the global object;
  var global = mainContext || {}; // jshint ignore:line
  Ember.imports = Ember.imports || global;
  Ember.lookup = Ember.lookup || global;
  var emExports = Ember.exports = Ember.exports || global;

  // aliases needed to keep minifiers from removing the global context
  emExports.Em = emExports.Ember = Ember;

  // Make sure these are set whether Ember was already defined or not

  Ember.isNamespace = true;

  Ember.toString = function () {
    return 'Ember';
  };

  // The debug functions are exported to globals with `require` to
  // prevent babel-plugin-filter-imports from removing them.
  var debugModule = _require.default('ember-metal/debug');
  Ember.assert = debugModule.assert;
  Ember.warn = debugModule.warn;
  Ember.debug = debugModule.debug;
  Ember.deprecate = debugModule.deprecate;
  Ember.deprecateFunc = debugModule.deprecateFunc;
  Ember.runInDebug = debugModule.runInDebug;

  /**
    The semantic version.
  
    @property VERSION
    @type String
    @default '2.6.2'
    @static
    @public
  */
  Ember.VERSION = '2.6.2';

  /**
    The hash of environment variables used to control various configuration
    settings. To specify your own or override default settings, add the
    desired properties to a global hash named `EmberENV` (or `ENV` for
    backwards compatibility with earlier versions of Ember). The `EmberENV`
    hash must be created before loading Ember.
  
    @property ENV
    @type Object
    @public
  */

  if (Ember.ENV) {
    // do nothing if Ember.ENV is already setup
    Ember.assert('Ember.ENV should be an object.', 'object' !== typeof Ember.ENV);
  } else if ('undefined' !== typeof EmberENV) {
    Ember.ENV = EmberENV;
  } else if ('undefined' !== typeof ENV) {
    Ember.ENV = ENV;
  } else {
    Ember.ENV = {};
  }

  // ENABLE_ALL_FEATURES was documented, but you can't actually enable non optional features.
  if (Ember.ENV.ENABLE_ALL_FEATURES) {
    Ember.ENV.ENABLE_OPTIONAL_FEATURES = Ember.ENV.ENABLE_ALL_FEATURES;
  }

  Ember.config = Ember.config || {};

  // ..........................................................
  // BOOTSTRAP
  //

  /**
    Determines whether Ember should add to `Array`, `Function`, and `String`
    native object prototypes, a few extra methods in order to provide a more
    friendly API.
  
    We generally recommend leaving this option set to true however, if you need
    to turn it off, you can add the configuration property
    `EXTEND_PROTOTYPES` to `EmberENV` and set it to `false`.
  
    Note, when disabled (the default configuration for Ember Addons), you will
    instead have to access all methods and functions from the Ember
    namespace.
  
    @property EXTEND_PROTOTYPES
    @type Boolean
    @default true
    @for Ember
    @public
  */
  Ember.EXTEND_PROTOTYPES = Ember.ENV.EXTEND_PROTOTYPES;

  if (typeof Ember.EXTEND_PROTOTYPES === 'undefined') {
    Ember.EXTEND_PROTOTYPES = true;
  }

  /**
    The `LOG_STACKTRACE_ON_DEPRECATION` property, when true, tells Ember to log
    a full stack trace during deprecation warnings.
  
    @property LOG_STACKTRACE_ON_DEPRECATION
    @type Boolean
    @default true
    @public
  */
  Ember.LOG_STACKTRACE_ON_DEPRECATION = Ember.ENV.LOG_STACKTRACE_ON_DEPRECATION !== false;

  /**
    The `LOG_VERSION` property, when true, tells Ember to log versions of all
    dependent libraries in use.
  
    @property LOG_VERSION
    @type Boolean
    @default true
    @public
  */
  Ember.LOG_VERSION = Ember.ENV.LOG_VERSION === false ? false : true;

  /**
    An empty function useful for some operations. Always returns `this`.
  
    @method K
    @return {Object}
    @public
  */
  function K() {
    return this;
  }
  exports.K = K;

  Ember.K = K;
  //TODO: ES6 GLOBAL TODO

  exports.default = Ember;
});
enifed("ember-metal/debug", ["exports"], function (exports) {
  "use strict";

  exports.getDebugFunction = getDebugFunction;
  exports.setDebugFunction = setDebugFunction;
  exports.assert = assert;
  exports.info = info;
  exports.warn = warn;
  exports.debug = debug;
  exports.deprecate = deprecate;
  exports.deprecateFunc = deprecateFunc;
  exports.runInDebug = runInDebug;
  exports.debugSeal = debugSeal;
  var debugFunctions = {
    assert: function () {},
    info: function () {},
    warn: function () {},
    debug: function () {},
    deprecate: function () {},
    deprecateFunc: function () {
      for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
        args[_key] = arguments[_key];
      }

      return args[args.length - 1];
    },
    runInDebug: function () {},
    debugSeal: function () {}
  };

  exports.debugFunctions = debugFunctions;

  function getDebugFunction(name) {
    return debugFunctions[name];
  }

  function setDebugFunction(name, fn) {
    debugFunctions[name] = fn;
  }

  function assert() {
    return debugFunctions.assert.apply(undefined, arguments);
  }

  function info() {
    return debugFunctions.info.apply(undefined, arguments);
  }

  function warn() {
    return debugFunctions.warn.apply(undefined, arguments);
  }

  function debug() {
    return debugFunctions.debug.apply(undefined, arguments);
  }

  function deprecate() {
    return debugFunctions.deprecate.apply(undefined, arguments);
  }

  function deprecateFunc() {
    return debugFunctions.deprecateFunc.apply(undefined, arguments);
  }

  function runInDebug() {
    return debugFunctions.runInDebug.apply(undefined, arguments);
  }

  function debugSeal() {
    return debugFunctions.debugSeal.apply(undefined, arguments);
  }
});
enifed('ember-metal/dependent_keys', ['exports', 'ember-metal/watching'], function (exports, _emberMetalWatching) {
  'no use strict';
  // Remove "use strict"; from transpiled module until
  // https://bugs.webkit.org/show_bug.cgi?id=138038 is fixed

  exports.addDependentKeys = addDependentKeys;
  exports.removeDependentKeys = removeDependentKeys;

  /**
  @module ember
  @submodule ember-metal
  */

  // ..........................................................
  // DEPENDENT KEYS
  //

  function addDependentKeys(desc, obj, keyName, meta) {
    // the descriptor has a list of dependent keys, so
    // add all of its dependent keys.
    var idx, len, depKey;
    var depKeys = desc._dependentKeys;
    if (!depKeys) {
      return;
    }

    for (idx = 0, len = depKeys.length; idx < len; idx++) {
      depKey = depKeys[idx];
      // Increment the number of times depKey depends on keyName.
      meta.writeDeps(depKey, keyName, (meta.peekDeps(depKey, keyName) || 0) + 1);
      // Watch the depKey
      _emberMetalWatching.watch(obj, depKey, meta);
    }
  }

  function removeDependentKeys(desc, obj, keyName, meta) {
    // the descriptor has a list of dependent keys, so
    // remove all of its dependent keys.
    var depKeys = desc._dependentKeys;
    var idx, len, depKey;
    if (!depKeys) {
      return;
    }

    for (idx = 0, len = depKeys.length; idx < len; idx++) {
      depKey = depKeys[idx];
      // Decrement the number of times depKey depends on keyName.
      meta.writeDeps(depKey, keyName, (meta.peekDeps(depKey, keyName) || 0) - 1);
      // Unwatch the depKey
      _emberMetalWatching.unwatch(obj, depKey, meta);
    }
  }
});
enifed('ember-metal/deprecate_property', ['exports', 'ember-metal/debug', 'ember-metal/property_get', 'ember-metal/property_set'], function (exports, _emberMetalDebug, _emberMetalProperty_get, _emberMetalProperty_set) {
  /**
  @module ember
  @submodule ember-metal
  */

  'use strict';

  exports.deprecateProperty = deprecateProperty;

  /**
    Used internally to allow changing properties in a backwards compatible way, and print a helpful
    deprecation warning.
  
    @method deprecateProperty
    @param {Object} object The object to add the deprecated property to.
    @param {String} deprecatedKey The property to add (and print deprecation warnings upon accessing).
    @param {String} newKey The property that will be aliased.
    @private
    @since 1.7.0
  */

  function deprecateProperty(object, deprecatedKey, newKey, options) {
    function _deprecate() {
      _emberMetalDebug.deprecate('Usage of `' + deprecatedKey + '` is deprecated, use `' + newKey + '` instead.', false, options);
    }

    Object.defineProperty(object, deprecatedKey, {
      configurable: true,
      enumerable: false,
      set: function (value) {
        _deprecate();
        _emberMetalProperty_set.set(this, newKey, value);
      },
      get: function () {
        _deprecate();
        return _emberMetalProperty_get.get(this, newKey);
      }
    });
  }
});
enifed('ember-metal/dictionary', ['exports', 'ember-metal/empty_object'], function (exports, _emberMetalEmpty_object) {
  'use strict';

  exports.default = makeDictionary;

  // the delete is meant to hint at runtimes that this object should remain in
  // dictionary mode. This is clearly a runtime specific hack, but currently it
  // appears worthwhile in some usecases. Please note, these deletes do increase
  // the cost of creation dramatically over a plain Object.create. And as this
  // only makes sense for long-lived dictionaries that aren't instantiated often.

  function makeDictionary(parent) {
    var dict;
    if (parent === null) {
      dict = new _emberMetalEmpty_object.default();
    } else {
      dict = Object.create(parent);
    }
    dict['_dict'] = null;
    delete dict['_dict'];
    return dict;
  }
});
enifed("ember-metal/empty_object", ["exports"], function (exports) {
  // This exists because `Object.create(null)` is absurdly slow compared
  // to `new EmptyObject()`. In either case, you want a null prototype
  // when you're treating the object instances as arbitrary dictionaries
  // and don't want your keys colliding with build-in methods on the
  // default object prototype.

  "use strict";

  var proto = Object.create(null, {
    // without this, we will always still end up with (new
    // EmptyObject()).constructor === Object
    constructor: {
      value: undefined,
      enumerable: false,
      writable: true
    }
  });

  function EmptyObject() {}
  EmptyObject.prototype = proto;
  exports.default = EmptyObject;
});
enifed('ember-metal/environment', ['exports', 'ember-metal/core'], function (exports, _emberMetalCore) {
  'use strict';

  /*
    Ember can run in many different environments, including environments like
    Node.js where the DOM is unavailable. This object serves as an abstraction
    over the browser features that Ember relies on, so that code does not
    explode when trying to boot in an environment that doesn't have them.
  
    This is a private abstraction. In the future, we hope that other
    abstractions (like `Location`, `Renderer`, `dom-helper`) can fully abstract
    over the differences in environment.
  */
  var environment;

  // This code attempts to automatically detect an environment with DOM
  // by searching for window and document.createElement. An environment
  // with DOM may disable the DOM functionality of Ember explicitly by
  // defining a `disableBrowserEnvironment` ENV.
  var hasDOM = typeof window !== 'undefined' && typeof document !== 'undefined' && typeof document.createElement !== 'undefined' && !_emberMetalCore.default.ENV.disableBrowserEnvironment;

  if (hasDOM) {
    environment = {
      hasDOM: true,
      isChrome: !!window.chrome && !window.opera,
      isFirefox: typeof InstallTrigger !== 'undefined',
      isPhantom: !!window.callPhantom,
      location: window.location,
      history: window.history,
      userAgent: window.navigator.userAgent,
      global: window
    };
  } else {
    environment = {
      hasDOM: false,
      isChrome: false,
      isFirefox: false,
      isPhantom: false,
      location: null,
      history: null,
      userAgent: 'Lynx (textmode)',
      global: null
    };
  }

  exports.default = environment;
});
enifed('ember-metal/error', ['exports', 'ember-metal/core'], function (exports, _emberMetalCore) {
  'use strict';

  exports.default = EmberError;

  var errorProps = ['description', 'fileName', 'lineNumber', 'message', 'name', 'number', 'stack'];

  /**
    A subclass of the JavaScript Error object for use in Ember.
  
    @class Error
    @namespace Ember
    @extends Error
    @constructor
    @public
  */

  function EmberError() {
    var tmp = Error.apply(this, arguments);

    // Adds a `stack` property to the given error object that will yield the
    // stack trace at the time captureStackTrace was called.
    // When collecting the stack trace all frames above the topmost call
    // to this function, including that call, will be left out of the
    // stack trace.
    // This is useful because we can hide Ember implementation details
    // that are not very helpful for the user.
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, _emberMetalCore.default.Error);
    }
    // Unfortunately errors are not enumerable in Chrome (at least), so `for prop in tmp` doesn't work.
    for (var idx = 0; idx < errorProps.length; idx++) {
      this[errorProps[idx]] = tmp[errorProps[idx]];
    }
  }

  EmberError.prototype = Object.create(Error.prototype);
});
enifed('ember-metal/events', ['exports', 'ember-metal/debug', 'ember-metal/utils', 'ember-metal/meta', 'ember-metal/meta_listeners'], function (exports, _emberMetalDebug, _emberMetalUtils, _emberMetalMeta, _emberMetalMeta_listeners) {
  'no use strict';
  // Remove "use strict"; from transpiled module until
  // https://bugs.webkit.org/show_bug.cgi?id=138038 is fixed

  /**
  @module ember
  @submodule ember-metal
  */
  exports.accumulateListeners = accumulateListeners;
  exports.addListener = addListener;
  exports.removeListener = removeListener;
  exports.suspendListener = suspendListener;
  exports.suspendListeners = suspendListeners;
  exports.watchedEvents = watchedEvents;
  exports.sendEvent = sendEvent;
  exports.hasListeners = hasListeners;
  exports.listenersFor = listenersFor;
  exports.on = on;

  /*
    The event system uses a series of nested hashes to store listeners on an
    object. When a listener is registered, or when an event arrives, these
    hashes are consulted to determine which target and action pair to invoke.
  
    The hashes are stored in the object's meta hash, and look like this:
  
        // Object's meta hash
        {
          listeners: {       // variable name: `listenerSet`
            "foo:changed": [ // variable name: `actions`
              target, method, flags
            ]
          }
        }
  
  */

  function indexOf(array, target, method) {
    var index = -1;
    // hashes are added to the end of the event array
    // so it makes sense to start searching at the end
    // of the array and search in reverse
    for (var i = array.length - 3; i >= 0; i -= 3) {
      if (target === array[i] && method === array[i + 1]) {
        index = i;
        break;
      }
    }
    return index;
  }

  function accumulateListeners(obj, eventName, otherActions) {
    var meta = _emberMetalMeta.peekMeta(obj);
    if (!meta) {
      return;
    }
    var actions = meta.matchingListeners(eventName);
    var newActions = [];

    for (var i = actions.length - 3; i >= 0; i -= 3) {
      var target = actions[i];
      var method = actions[i + 1];
      var flags = actions[i + 2];
      var actionIndex = indexOf(otherActions, target, method);

      if (actionIndex === -1) {
        otherActions.push(target, method, flags);
        newActions.push(target, method, flags);
      }
    }

    return newActions;
  }

  /**
    Add an event listener
  
    @method addListener
    @for Ember
    @param obj
    @param {String} eventName
    @param {Object|Function} target A target object or a function
    @param {Function|String} method A function or the name of a function to be called on `target`
    @param {Boolean} once A flag whether a function should only be called once
    @public
  */

  function addListener(obj, eventName, target, method, once) {
    _emberMetalDebug.assert('You must pass at least an object and event name to Ember.addListener', !!obj && !!eventName);

    _emberMetalDebug.deprecate('didInitAttrs called in ' + (obj && obj.toString && obj.toString()) + '.', eventName !== 'didInitAttrs', {
      id: 'ember-views.did-init-attrs',
      until: '3.0.0',
      url: 'http://emberjs.com/deprecations/v2.x#toc_ember-component-didinitattrs'
    });

    if (!method && 'function' === typeof target) {
      method = target;
      target = null;
    }

    var flags = 0;
    if (once) {
      flags |= _emberMetalMeta_listeners.ONCE;
    }

    _emberMetalMeta.meta(obj).addToListeners(eventName, target, method, flags);

    if ('function' === typeof obj.didAddListener) {
      obj.didAddListener(eventName, target, method);
    }
  }

  /**
    Remove an event listener
  
    Arguments should match those passed to `Ember.addListener`.
  
    @method removeListener
    @for Ember
    @param obj
    @param {String} eventName
    @param {Object|Function} target A target object or a function
    @param {Function|String} method A function or the name of a function to be called on `target`
    @public
  */

  function removeListener(obj, eventName, target, method) {
    _emberMetalDebug.assert('You must pass at least an object and event name to Ember.removeListener', !!obj && !!eventName);

    if (!method && 'function' === typeof target) {
      method = target;
      target = null;
    }

    _emberMetalMeta.meta(obj).removeFromListeners(eventName, target, method, function () {
      if ('function' === typeof obj.didRemoveListener) {
        obj.didRemoveListener.apply(obj, arguments);
      }
    });
  }

  /**
    Suspend listener during callback.
  
    This should only be used by the target of the event listener
    when it is taking an action that would cause the event, e.g.
    an object might suspend its property change listener while it is
    setting that property.
  
    @method suspendListener
    @for Ember
  
    @private
    @param obj
    @param {String} eventName
    @param {Object|Function} target A target object or a function
    @param {Function|String} method A function or the name of a function to be called on `target`
    @param {Function} callback
  */

  function suspendListener(obj, eventName, target, method, callback) {
    return suspendListeners(obj, [eventName], target, method, callback);
  }

  /**
    Suspends multiple listeners during a callback.
  
    @method suspendListeners
    @for Ember
  
    @private
    @param obj
    @param {Array} eventNames Array of event names
    @param {Object|Function} target A target object or a function
    @param {Function|String} method A function or the name of a function to be called on `target`
    @param {Function} callback
  */

  function suspendListeners(obj, eventNames, target, method, callback) {
    if (!method && 'function' === typeof target) {
      method = target;
      target = null;
    }
    return _emberMetalMeta.meta(obj).suspendListeners(eventNames, target, method, callback);
  }

  /**
    Return a list of currently watched events
  
    @private
    @method watchedEvents
    @for Ember
    @param obj
  */

  function watchedEvents(obj) {
    return _emberMetalMeta.meta(obj).watchedEvents();
  }

  /**
    Send an event. The execution of suspended listeners
    is skipped, and once listeners are removed. A listener without
    a target is executed on the passed object. If an array of actions
    is not passed, the actions stored on the passed object are invoked.
  
    @method sendEvent
    @for Ember
    @param obj
    @param {String} eventName
    @param {Array} params Optional parameters for each listener.
    @param {Array} actions Optional array of actions (listeners).
    @return true
    @public
  */

  function sendEvent(obj, eventName, params, actions) {
    if (!actions) {
      var meta = _emberMetalMeta.peekMeta(obj);
      actions = meta && meta.matchingListeners(eventName);
    }

    if (!actions || actions.length === 0) {
      return;
    }

    for (var i = actions.length - 3; i >= 0; i -= 3) {
      // looping in reverse for once listeners
      var target = actions[i];
      var method = actions[i + 1];
      var flags = actions[i + 2];

      if (!method) {
        continue;
      }
      if (flags & _emberMetalMeta_listeners.SUSPENDED) {
        continue;
      }
      if (flags & _emberMetalMeta_listeners.ONCE) {
        removeListener(obj, eventName, target, method);
      }
      if (!target) {
        target = obj;
      }
      if ('string' === typeof method) {
        if (params) {
          _emberMetalUtils.applyStr(target, method, params);
        } else {
          target[method]();
        }
      } else {
        if (params) {
          method.apply(target, params);
        } else {
          method.call(target);
        }
      }
    }
    return true;
  }

  /**
    @private
    @method hasListeners
    @for Ember
    @param obj
    @param {String} eventName
  */

  function hasListeners(obj, eventName) {
    var meta = _emberMetalMeta.peekMeta(obj);
    if (!meta) {
      return false;
    }
    return meta.matchingListeners(eventName).length > 0;
  }

  /**
    @private
    @method listenersFor
    @for Ember
    @param obj
    @param {String} eventName
  */

  function listenersFor(obj, eventName) {
    var ret = [];
    var meta = _emberMetalMeta.peekMeta(obj);
    var actions = meta && meta.matchingListeners(eventName);

    if (!actions) {
      return ret;
    }

    for (var i = 0, l = actions.length; i < l; i += 3) {
      var target = actions[i];
      var method = actions[i + 1];
      ret.push([target, method]);
    }

    return ret;
  }

  /**
    Define a property as a function that should be executed when
    a specified event or events are triggered.
  
  
    ``` javascript
    var Job = Ember.Object.extend({
      logCompleted: Ember.on('completed', function() {
        console.log('Job completed!');
      })
    });
  
    var job = Job.create();
  
    Ember.sendEvent(job, 'completed'); // Logs 'Job completed!'
   ```
  
    @method on
    @for Ember
    @param {String} eventNames*
    @param {Function} func
    @return func
    @public
  */

  function on() {
    for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    var func = args.pop();
    var events = args;
    func.__ember_listens__ = events;
    return func;
  }
});
enifed('ember-metal/expand_properties', ['exports', 'ember-metal/debug'], function (exports, _emberMetalDebug) {
  'use strict';

  exports.default = expandProperties;

  /**
  @module ember
  @submodule ember-metal
  */

  var SPLIT_REGEX = /\{|\}/;
  var END_WITH_EACH_REGEX = /\.@each$/;

  /**
    Expands `pattern`, invoking `callback` for each expansion.
  
    The only pattern supported is brace-expansion, anything else will be passed
    once to `callback` directly.
  
    Example
  
    ```js
    function echo(arg){ console.log(arg); }
  
    Ember.expandProperties('foo.bar', echo);              //=> 'foo.bar'
    Ember.expandProperties('{foo,bar}', echo);            //=> 'foo', 'bar'
    Ember.expandProperties('foo.{bar,baz}', echo);        //=> 'foo.bar', 'foo.baz'
    Ember.expandProperties('{foo,bar}.baz', echo);        //=> 'foo.baz', 'bar.baz'
    Ember.expandProperties('foo.{bar,baz}.[]', echo)      //=> 'foo.bar.[]', 'foo.baz.[]'
    Ember.expandProperties('{foo,bar}.{spam,eggs}', echo) //=> 'foo.spam', 'foo.eggs', 'bar.spam', 'bar.eggs'
    Ember.expandProperties('{foo}.bar.{baz}')             //=> 'foo.bar.baz'
    ```
  
    @method expandProperties
    @for Ember
    @private
    @param {String} pattern The property pattern to expand.
    @param {Function} callback The callback to invoke.  It is invoked once per
    expansion, and is passed the expansion.
  */

  function expandProperties(pattern, callback) {
    _emberMetalDebug.assert('A computed property key must be a string', typeof pattern === 'string');
    _emberMetalDebug.assert('Brace expanded properties cannot contain spaces, e.g. "user.{firstName, lastName}" should be "user.{firstName,lastName}"', pattern.indexOf(' ') === -1);

    var parts = pattern.split(SPLIT_REGEX);
    var properties = [parts];

    for (var i = 0; i < parts.length; i++) {
      var part = parts[i];
      if (part.indexOf(',') >= 0) {
        properties = duplicateAndReplace(properties, part.split(','), i);
      }
    }

    for (var i = 0; i < properties.length; i++) {
      callback(properties[i].join('').replace(END_WITH_EACH_REGEX, '.[]'));
    }
  }

  function duplicateAndReplace(properties, currentParts, index) {
    var all = [];

    properties.forEach(function (property) {
      currentParts.forEach(function (part) {
        var current = property.slice(0);
        current[index] = part;
        all.push(current);
      });
    });

    return all;
  }
});
enifed('ember-metal/features', ['exports', 'ember-metal/core', 'ember-metal/assign'], function (exports, _emberMetalCore, _emberMetalAssign) {
  'use strict';

  exports.default = isEnabled;

  /**
    The hash of enabled Canary features. Add to this, any canary features
    before creating your application.
  
    Alternatively (and recommended), you can also define `EmberENV.FEATURES`
    if you need to enable features flagged at runtime.
  
    @class FEATURES
    @namespace Ember
    @static
    @since 1.1.0
    @public
  */
  var KNOWN_FEATURES = {};exports.KNOWN_FEATURES = KNOWN_FEATURES;
  // jshint ignore:line
  var FEATURES = _emberMetalAssign.default(KNOWN_FEATURES, _emberMetalCore.default.ENV.FEATURES);

  exports.FEATURES = FEATURES;
  /**
    Determine whether the specified `feature` is enabled. Used by Ember's
    build tools to exclude experimental features from beta/stable builds.
  
    You can define the following configuration options:
  
    * `EmberENV.ENABLE_OPTIONAL_FEATURES` - enable any features that have not been explicitly
      enabled/disabled.
  
    @method isEnabled
    @param {String} feature The feature to check
    @return {Boolean}
    @for Ember.FEATURES
    @since 1.1.0
    @public
  */

  function isEnabled(feature) {
    var featureValue = FEATURES[feature];

    if (featureValue === true || featureValue === false || featureValue === undefined) {
      return featureValue;
    } else if (_emberMetalCore.default.ENV.ENABLE_OPTIONAL_FEATURES) {
      return true;
    } else {
      return false;
    }
  }
});
enifed('ember-metal/get_properties', ['exports', 'ember-metal/property_get'], function (exports, _emberMetalProperty_get) {
  'use strict';

  exports.default = getProperties;

  /**
    To get multiple properties at once, call `Ember.getProperties`
    with an object followed by a list of strings or an array:
  
    ```javascript
    Ember.getProperties(record, 'firstName', 'lastName', 'zipCode');
    // { firstName: 'John', lastName: 'Doe', zipCode: '10011' }
    ```
  
    is equivalent to:
  
    ```javascript
    Ember.getProperties(record, ['firstName', 'lastName', 'zipCode']);
    // { firstName: 'John', lastName: 'Doe', zipCode: '10011' }
    ```
  
    @method getProperties
    @for Ember
    @param {Object} obj
    @param {String...|Array} list of keys to get
    @return {Object}
    @public
  */

  function getProperties(obj) {
    var ret = {};
    var propertyNames = arguments;
    var i = 1;

    if (arguments.length === 2 && Array.isArray(arguments[1])) {
      i = 0;
      propertyNames = arguments[1];
    }
    for (var len = propertyNames.length; i < len; i++) {
      ret[propertyNames[i]] = _emberMetalProperty_get.get(obj, propertyNames[i]);
    }
    return ret;
  }
});
enifed('ember-metal/index', ['exports', 'require', 'ember-metal/core', 'ember-metal/debug', 'ember-metal/features', 'ember-metal/assign', 'ember-metal/merge', 'ember-metal/instrumentation', 'ember-metal/utils', 'ember-metal/meta', 'ember-metal/error', 'ember-metal/cache', 'ember-metal/logger', 'ember-metal/property_get', 'ember-metal/events', 'ember-metal/observer_set', 'ember-metal/property_events', 'ember-metal/properties', 'ember-metal/property_set', 'ember-metal/map', 'ember-metal/get_properties', 'ember-metal/set_properties', 'ember-metal/watch_key', 'ember-metal/chains', 'ember-metal/watch_path', 'ember-metal/watching', 'ember-metal/expand_properties', 'ember-metal/computed', 'ember-metal/alias', 'ember-metal/computed_macros', 'ember-metal/observer', 'ember-metal/mixin', 'ember-metal/binding', 'ember-metal/path_cache', 'ember-metal/run_loop', 'ember-metal/libraries', 'ember-metal/is_none', 'ember-metal/is_empty', 'ember-metal/is_blank', 'ember-metal/is_present', 'backburner'], function (exports, _require, _emberMetalCore, _emberMetalDebug, _emberMetalFeatures, _emberMetalAssign, _emberMetalMerge, _emberMetalInstrumentation, _emberMetalUtils, _emberMetalMeta, _emberMetalError, _emberMetalCache, _emberMetalLogger, _emberMetalProperty_get, _emberMetalEvents, _emberMetalObserver_set, _emberMetalProperty_events, _emberMetalProperties, _emberMetalProperty_set, _emberMetalMap, _emberMetalGet_properties, _emberMetalSet_properties, _emberMetalWatch_key, _emberMetalChains, _emberMetalWatch_path, _emberMetalWatching, _emberMetalExpand_properties, _emberMetalComputed, _emberMetalAlias, _emberMetalComputed_macros, _emberMetalObserver, _emberMetalMixin, _emberMetalBinding, _emberMetalPath_cache, _emberMetalRun_loop, _emberMetalLibraries, _emberMetalIs_none, _emberMetalIs_empty, _emberMetalIs_blank, _emberMetalIs_present, _backburner) {
  /**
  @module ember
  @submodule ember-metal
  */

  // BEGIN IMPORTS
  'use strict';

  _emberMetalComputed.computed.empty = _emberMetalComputed_macros.empty;
  _emberMetalComputed.computed.notEmpty = _emberMetalComputed_macros.notEmpty;
  _emberMetalComputed.computed.none = _emberMetalComputed_macros.none;
  _emberMetalComputed.computed.not = _emberMetalComputed_macros.not;
  _emberMetalComputed.computed.bool = _emberMetalComputed_macros.bool;
  _emberMetalComputed.computed.match = _emberMetalComputed_macros.match;
  _emberMetalComputed.computed.equal = _emberMetalComputed_macros.equal;
  _emberMetalComputed.computed.gt = _emberMetalComputed_macros.gt;
  _emberMetalComputed.computed.gte = _emberMetalComputed_macros.gte;
  _emberMetalComputed.computed.lt = _emberMetalComputed_macros.lt;
  _emberMetalComputed.computed.lte = _emberMetalComputed_macros.lte;
  _emberMetalComputed.computed.alias = _emberMetalAlias.default;
  _emberMetalComputed.computed.oneWay = _emberMetalComputed_macros.oneWay;
  _emberMetalComputed.computed.reads = _emberMetalComputed_macros.oneWay;
  _emberMetalComputed.computed.readOnly = _emberMetalComputed_macros.readOnly;
  _emberMetalComputed.computed.defaultTo = _emberMetalComputed_macros.defaultTo;
  _emberMetalComputed.computed.deprecatingAlias = _emberMetalComputed_macros.deprecatingAlias;
  _emberMetalComputed.computed.and = _emberMetalComputed_macros.and;
  _emberMetalComputed.computed.or = _emberMetalComputed_macros.or;
  _emberMetalComputed.computed.any = _emberMetalComputed_macros.any;

  // END IMPORTS

  // BEGIN EXPORTS
  var EmberInstrumentation = _emberMetalCore.default.Instrumentation = {};
  EmberInstrumentation.instrument = _emberMetalInstrumentation.instrument;
  EmberInstrumentation.subscribe = _emberMetalInstrumentation.subscribe;
  EmberInstrumentation.unsubscribe = _emberMetalInstrumentation.unsubscribe;
  EmberInstrumentation.reset = _emberMetalInstrumentation.reset;

  _emberMetalCore.default.instrument = _emberMetalInstrumentation.instrument;
  _emberMetalCore.default.subscribe = _emberMetalInstrumentation.subscribe;

  _emberMetalCore.default._Cache = _emberMetalCache.default;

  _emberMetalCore.default.generateGuid = _emberMetalUtils.generateGuid;
  _emberMetalCore.default.GUID_KEY = _emberMetalUtils.GUID_KEY;
  _emberMetalCore.default.platform = {
    defineProperty: true,
    hasPropertyAccessors: true
  };

  _emberMetalCore.default.Error = _emberMetalError.default;
  _emberMetalCore.default.guidFor = _emberMetalUtils.guidFor;
  _emberMetalCore.default.META_DESC = _emberMetalMeta.META_DESC;
  _emberMetalCore.default.meta = _emberMetalMeta.meta;
  _emberMetalCore.default.inspect = _emberMetalUtils.inspect;

  _emberMetalCore.default.tryCatchFinally = _emberMetalUtils.deprecatedTryCatchFinally;
  _emberMetalCore.default.makeArray = _emberMetalUtils.makeArray;
  _emberMetalCore.default.canInvoke = _emberMetalUtils.canInvoke;
  _emberMetalCore.default.tryInvoke = _emberMetalUtils.tryInvoke;
  _emberMetalCore.default.wrap = _emberMetalUtils.wrap;
  _emberMetalCore.default.apply = _emberMetalUtils.apply;
  _emberMetalCore.default.applyStr = _emberMetalUtils.applyStr;
  _emberMetalCore.default.uuid = _emberMetalUtils.uuid;

  _emberMetalCore.default.Logger = _emberMetalLogger.default;

  _emberMetalCore.default.get = _emberMetalProperty_get.get;
  _emberMetalCore.default.getWithDefault = _emberMetalProperty_get.getWithDefault;
  _emberMetalCore.default._getPath = _emberMetalProperty_get._getPath;

  _emberMetalCore.default.on = _emberMetalEvents.on;
  _emberMetalCore.default.addListener = _emberMetalEvents.addListener;
  _emberMetalCore.default.removeListener = _emberMetalEvents.removeListener;
  _emberMetalCore.default._suspendListener = _emberMetalEvents.suspendListener;
  _emberMetalCore.default._suspendListeners = _emberMetalEvents.suspendListeners;
  _emberMetalCore.default.sendEvent = _emberMetalEvents.sendEvent;
  _emberMetalCore.default.hasListeners = _emberMetalEvents.hasListeners;
  _emberMetalCore.default.watchedEvents = _emberMetalEvents.watchedEvents;
  _emberMetalCore.default.listenersFor = _emberMetalEvents.listenersFor;
  _emberMetalCore.default.accumulateListeners = _emberMetalEvents.accumulateListeners;

  _emberMetalCore.default._ObserverSet = _emberMetalObserver_set.default;

  _emberMetalCore.default.propertyWillChange = _emberMetalProperty_events.propertyWillChange;
  _emberMetalCore.default.propertyDidChange = _emberMetalProperty_events.propertyDidChange;
  _emberMetalCore.default.overrideChains = _emberMetalProperty_events.overrideChains;
  _emberMetalCore.default.beginPropertyChanges = _emberMetalProperty_events.beginPropertyChanges;
  _emberMetalCore.default.endPropertyChanges = _emberMetalProperty_events.endPropertyChanges;
  _emberMetalCore.default.changeProperties = _emberMetalProperty_events.changeProperties;

  _emberMetalCore.default.defineProperty = _emberMetalProperties.defineProperty;

  _emberMetalCore.default.set = _emberMetalProperty_set.set;
  _emberMetalCore.default.trySet = _emberMetalProperty_set.trySet;

  _emberMetalCore.default.OrderedSet = _emberMetalMap.OrderedSet;
  _emberMetalCore.default.Map = _emberMetalMap.Map;
  _emberMetalCore.default.MapWithDefault = _emberMetalMap.MapWithDefault;

  _emberMetalCore.default.getProperties = _emberMetalGet_properties.default;
  _emberMetalCore.default.setProperties = _emberMetalSet_properties.default;

  _emberMetalCore.default.watchKey = _emberMetalWatch_key.watchKey;
  _emberMetalCore.default.unwatchKey = _emberMetalWatch_key.unwatchKey;

  _emberMetalCore.default.removeChainWatcher = _emberMetalChains.removeChainWatcher;
  _emberMetalCore.default._ChainNode = _emberMetalChains.ChainNode;
  _emberMetalCore.default.finishChains = _emberMetalChains.finishChains;

  _emberMetalCore.default.watchPath = _emberMetalWatch_path.watchPath;
  _emberMetalCore.default.unwatchPath = _emberMetalWatch_path.unwatchPath;

  _emberMetalCore.default.watch = _emberMetalWatching.watch;
  _emberMetalCore.default.isWatching = _emberMetalWatching.isWatching;
  _emberMetalCore.default.unwatch = _emberMetalWatching.unwatch;
  _emberMetalCore.default.rewatch = _emberMetalWatching.rewatch;
  _emberMetalCore.default.destroy = _emberMetalWatching.destroy;

  _emberMetalCore.default.expandProperties = _emberMetalExpand_properties.default;

  _emberMetalCore.default.ComputedProperty = _emberMetalComputed.ComputedProperty;
  _emberMetalCore.default.computed = _emberMetalComputed.computed;
  _emberMetalCore.default.cacheFor = _emberMetalComputed.cacheFor;

  _emberMetalCore.default.addObserver = _emberMetalObserver.addObserver;
  _emberMetalCore.default.observersFor = _emberMetalObserver.observersFor;
  _emberMetalCore.default.removeObserver = _emberMetalObserver.removeObserver;
  _emberMetalCore.default._suspendObserver = _emberMetalObserver._suspendObserver;
  _emberMetalCore.default._suspendObservers = _emberMetalObserver._suspendObservers;

  _emberMetalCore.default.IS_BINDING = _emberMetalMixin.IS_BINDING;
  _emberMetalCore.default.required = _emberMetalMixin.required;
  _emberMetalCore.default.aliasMethod = _emberMetalMixin.aliasMethod;
  _emberMetalCore.default.observer = _emberMetalMixin.observer;
  _emberMetalCore.default.immediateObserver = _emberMetalMixin._immediateObserver;
  _emberMetalCore.default.mixin = _emberMetalMixin.mixin;
  _emberMetalCore.default.Mixin = _emberMetalMixin.Mixin;

  _emberMetalCore.default.bind = _emberMetalBinding.bind;
  _emberMetalCore.default.Binding = _emberMetalBinding.Binding;
  _emberMetalCore.default.isGlobalPath = _emberMetalPath_cache.isGlobalPath;

  _emberMetalCore.default.run = _emberMetalRun_loop.default;

  /**
  @class Backburner
  @for Ember
  @private
  */
  _emberMetalCore.default.Backburner = _backburner.default;
  // this is the new go forward, once Ember Data updates to using `_Backburner` we
  // can remove the non-underscored version.
  _emberMetalCore.default._Backburner = _backburner.default;

  _emberMetalCore.default.libraries = new _emberMetalLibraries.default();
  _emberMetalCore.default.libraries.registerCoreLibrary('Ember', _emberMetalCore.default.VERSION);

  _emberMetalCore.default.isNone = _emberMetalIs_none.default;
  _emberMetalCore.default.isEmpty = _emberMetalIs_empty.default;
  _emberMetalCore.default.isBlank = _emberMetalIs_blank.default;
  _emberMetalCore.default.isPresent = _emberMetalIs_present.default;

  _emberMetalCore.default.assign = Object.assign || _emberMetalAssign.default;
  _emberMetalCore.default.merge = _emberMetalMerge.default;

  _emberMetalCore.default.FEATURES = _emberMetalFeatures.FEATURES;
  _emberMetalCore.default.FEATURES.isEnabled = _emberMetalFeatures.default;

  /**
    A function may be assigned to `Ember.onerror` to be called when Ember
    internals encounter an error. This is useful for specialized error handling
    and reporting code.
  
    ```javascript
    Ember.onerror = function(error) {
      Em.$.ajax('/report-error', 'POST', {
        stack: error.stack,
        otherInformation: 'whatever app state you want to provide'
      });
    };
    ```
  
    Internally, `Ember.onerror` is used as Backburner's error handler.
  
    @event onerror
    @for Ember
    @param {Exception} error the error object
    @public
  */
  _emberMetalCore.default.onerror = null;
  // END EXPORTS

  // do this for side-effects of updating Ember.assert, warn, etc when
  // ember-debug is present
  // This needs to be called before any deprecateFunc
  if (_require.has('ember-debug')) {
    _require.default('ember-debug');
  } else {
    _emberMetalCore.default.Debug = {};
    _emberMetalCore.default.Debug.registerDeprecationHandler = function () {};
    _emberMetalCore.default.Debug.registerWarnHandler = function () {};
  }

  _emberMetalCore.default.create = _emberMetalDebug.deprecateFunc('Ember.create is deprecated in favor of Object.create', { id: 'ember-metal.ember-create', until: '3.0.0' }, Object.create);
  _emberMetalCore.default.keys = _emberMetalDebug.deprecateFunc('Ember.keys is deprecated in favor of Object.keys', { id: 'ember-metal.ember.keys', until: '3.0.0' }, Object.keys);

  exports.default = _emberMetalCore.default;
});
enifed('ember-metal/injected_property', ['exports', 'ember-metal/debug', 'ember-metal/computed', 'ember-metal/alias', 'ember-metal/properties', 'container/owner'], function (exports, _emberMetalDebug, _emberMetalComputed, _emberMetalAlias, _emberMetalProperties, _containerOwner) {
  'use strict';

  /**
    Read-only property that returns the result of a container lookup.
  
    @class InjectedProperty
    @namespace Ember
    @constructor
    @param {String} type The container type the property will lookup
    @param {String} name (optional) The name the property will lookup, defaults
           to the property's name
    @private
  */
  function InjectedProperty(type, name) {
    this.type = type;
    this.name = name;

    this._super$Constructor(injectedPropertyGet);
    AliasedPropertyPrototype.oneWay.call(this);
  }

  function injectedPropertyGet(keyName) {
    var desc = this[keyName];
    var owner = _containerOwner.getOwner(this) || this.container; // fallback to `container` for backwards compat

    _emberMetalDebug.assert('InjectedProperties should be defined with the Ember.inject computed property macros.', desc && desc.isDescriptor && desc.type);
    _emberMetalDebug.assert('Attempting to lookup an injected property on an object without a container, ensure that the object was instantiated via a container.', owner);

    return owner.lookup(desc.type + ':' + (desc.name || keyName));
  }

  InjectedProperty.prototype = Object.create(_emberMetalProperties.Descriptor.prototype);

  var InjectedPropertyPrototype = InjectedProperty.prototype;
  var ComputedPropertyPrototype = _emberMetalComputed.ComputedProperty.prototype;
  var AliasedPropertyPrototype = _emberMetalAlias.AliasedProperty.prototype;

  InjectedPropertyPrototype._super$Constructor = _emberMetalComputed.ComputedProperty;

  InjectedPropertyPrototype.get = ComputedPropertyPrototype.get;
  InjectedPropertyPrototype.readOnly = ComputedPropertyPrototype.readOnly;

  InjectedPropertyPrototype.teardown = ComputedPropertyPrototype.teardown;

  exports.default = InjectedProperty;
});
enifed('ember-metal/instrumentation', ['exports', 'ember-metal/core', 'ember-metal/features'], function (exports, _emberMetalCore, _emberMetalFeatures) {
  'use strict';

  exports.instrument = instrument;
  exports._instrumentStart = _instrumentStart;
  exports.subscribe = subscribe;
  exports.unsubscribe = unsubscribe;
  exports.reset = reset;

  /**
    The purpose of the Ember Instrumentation module is
    to provide efficient, general-purpose instrumentation
    for Ember.
  
    Subscribe to a listener by using `Ember.subscribe`:
  
    ```javascript
    Ember.subscribe("render", {
      before: function(name, timestamp, payload) {
  
      },
  
      after: function(name, timestamp, payload) {
  
      }
    });
    ```
  
    If you return a value from the `before` callback, that same
    value will be passed as a fourth parameter to the `after`
    callback.
  
    Instrument a block of code by using `Ember.instrument`:
  
    ```javascript
    Ember.instrument("render.handlebars", payload, function() {
      // rendering logic
    }, binding);
    ```
  
    Event names passed to `Ember.instrument` are namespaced
    by periods, from more general to more specific. Subscribers
    can listen for events by whatever level of granularity they
    are interested in.
  
    In the above example, the event is `render.handlebars`,
    and the subscriber listened for all events beginning with
    `render`. It would receive callbacks for events named
    `render`, `render.handlebars`, `render.container`, or
    even `render.handlebars.layout`.
  
    @class Instrumentation
    @namespace Ember
    @static
    @private
  */
  var subscribers = [];
  exports.subscribers = subscribers;
  var cache = {};

  var populateListeners = function (name) {
    var listeners = [];
    var subscriber;

    for (var i = 0, l = subscribers.length; i < l; i++) {
      subscriber = subscribers[i];
      if (subscriber.regex.test(name)) {
        listeners.push(subscriber.object);
      }
    }

    cache[name] = listeners;
    return listeners;
  };

  var time = (function () {
    var perf = 'undefined' !== typeof window ? window.performance || {} : {};
    var fn = perf.now || perf.mozNow || perf.webkitNow || perf.msNow || perf.oNow;
    // fn.bind will be available in all the browsers that support the advanced window.performance... ;-)
    return fn ? fn.bind(perf) : function () {
      return +new Date();
    };
  })();

  /**
    Notifies event's subscribers, calls `before` and `after` hooks.
  
    @method instrument
    @namespace Ember.Instrumentation
  
    @param {String} [name] Namespaced event name.
    @param {Object} _payload
    @param {Function} callback Function that you're instrumenting.
    @param {Object} binding Context that instrument function is called with.
    @private
  */

  function instrument(name, _payload, callback, binding) {
    if (arguments.length <= 3 && typeof _payload === 'function') {
      binding = callback;
      callback = _payload;
      _payload = undefined;
    }
    if (subscribers.length === 0) {
      return callback.call(binding);
    }
    var payload = _payload || {};
    var finalizer = _instrumentStart(name, function () {
      return payload;
    });

    if (finalizer) {
      return withFinalizer(callback, finalizer, payload, binding);
    } else {
      return callback.call(binding);
    }
  }

  var flaggedInstrument;

  exports.flaggedInstrument = flaggedInstrument = function (name, payload, callback) {
    return callback();
  };
  exports.flaggedInstrument = flaggedInstrument;

  function withFinalizer(callback, finalizer, payload, binding) {
    var result = undefined;
    try {
      result = callback.call(binding);
    } catch (e) {
      payload.exception = e;
      result = payload;
    } finally {
      finalizer();
      return result;
    }
  }

  // private for now

  function _instrumentStart(name, _payload) {
    var listeners = cache[name];

    if (!listeners) {
      listeners = populateListeners(name);
    }

    if (listeners.length === 0) {
      return;
    }

    var payload = _payload();

    var STRUCTURED_PROFILE = _emberMetalCore.default.STRUCTURED_PROFILE;
    var timeName;
    if (STRUCTURED_PROFILE) {
      timeName = name + ': ' + payload.object;
      console.time(timeName);
    }

    var l = listeners.length;
    var beforeValues = new Array(l);
    var i, listener;
    var timestamp = time();
    for (i = 0; i < l; i++) {
      listener = listeners[i];
      beforeValues[i] = listener.before(name, timestamp, payload);
    }

    return function _instrumentEnd() {
      var i, l, listener;
      var timestamp = time();
      for (i = 0, l = listeners.length; i < l; i++) {
        listener = listeners[i];
        if (typeof listener.after === 'function') {
          listener.after(name, timestamp, payload, beforeValues[i]);
        }
      }

      if (STRUCTURED_PROFILE) {
        console.timeEnd(timeName);
      }
    };
  }

  /**
    Subscribes to a particular event or instrumented block of code.
  
    @method subscribe
    @namespace Ember.Instrumentation
  
    @param {String} [pattern] Namespaced event name.
    @param {Object} [object] Before and After hooks.
  
    @return {Subscriber}
    @private
  */

  function subscribe(pattern, object) {
    var paths = pattern.split('.');
    var path;
    var regex = [];

    for (var i = 0, l = paths.length; i < l; i++) {
      path = paths[i];
      if (path === '*') {
        regex.push('[^\\.]*');
      } else {
        regex.push(path);
      }
    }

    regex = regex.join('\\.');
    regex = regex + '(\\..*)?';

    var subscriber = {
      pattern: pattern,
      regex: new RegExp('^' + regex + '$'),
      object: object
    };

    subscribers.push(subscriber);
    cache = {};

    return subscriber;
  }

  /**
    Unsubscribes from a particular event or instrumented block of code.
  
    @method unsubscribe
    @namespace Ember.Instrumentation
  
    @param {Object} [subscriber]
    @private
  */

  function unsubscribe(subscriber) {
    var index;

    for (var i = 0, l = subscribers.length; i < l; i++) {
      if (subscribers[i] === subscriber) {
        index = i;
      }
    }

    subscribers.splice(index, 1);
    cache = {};
  }

  /**
    Resets `Ember.Instrumentation` by flushing list of subscribers.
  
    @method reset
    @namespace Ember.Instrumentation
    @private
  */

  function reset() {
    subscribers.length = 0;
    cache = {};
  }
});
enifed('ember-metal/is_blank', ['exports', 'ember-metal/is_empty'], function (exports, _emberMetalIs_empty) {
  'use strict';

  exports.default = isBlank;

  /**
    A value is blank if it is empty or a whitespace string.
  
    ```javascript
    Ember.isBlank();                // true
    Ember.isBlank(null);            // true
    Ember.isBlank(undefined);       // true
    Ember.isBlank('');              // true
    Ember.isBlank([]);              // true
    Ember.isBlank('\n\t');          // true
    Ember.isBlank('  ');            // true
    Ember.isBlank({});              // false
    Ember.isBlank('\n\t Hello');    // false
    Ember.isBlank('Hello world');   // false
    Ember.isBlank([1,2,3]);         // false
    ```
  
    @method isBlank
    @for Ember
    @param {Object} obj Value to test
    @return {Boolean}
    @since 1.5.0
    @public
  */

  function isBlank(obj) {
    return _emberMetalIs_empty.default(obj) || typeof obj === 'string' && obj.match(/\S/) === null;
  }
});
enifed('ember-metal/is_empty', ['exports', 'ember-metal/property_get', 'ember-metal/is_none'], function (exports, _emberMetalProperty_get, _emberMetalIs_none) {
  'use strict';

  /**
    Verifies that a value is `null` or an empty string, empty array,
    or empty function.
  
    Constrains the rules on `Ember.isNone` by returning true for empty
    string and empty arrays.
  
    ```javascript
    Ember.isEmpty();                // true
    Ember.isEmpty(null);            // true
    Ember.isEmpty(undefined);       // true
    Ember.isEmpty('');              // true
    Ember.isEmpty([]);              // true
    Ember.isEmpty({});              // false
    Ember.isEmpty('Adam Hawkins');  // false
    Ember.isEmpty([0,1,2]);         // false
    Ember.isEmpty('\n\t');          // false
    Ember.isEmpty('  ');            // false
    ```
  
    @method isEmpty
    @for Ember
    @param {Object} obj Value to test
    @return {Boolean}
    @public
  */
  function isEmpty(obj) {
    var none = _emberMetalIs_none.default(obj);
    if (none) {
      return none;
    }

    if (typeof obj.size === 'number') {
      return !obj.size;
    }

    var objectType = typeof obj;

    if (objectType === 'object') {
      var size = _emberMetalProperty_get.get(obj, 'size');
      if (typeof size === 'number') {
        return !size;
      }
    }

    if (typeof obj.length === 'number' && objectType !== 'function') {
      return !obj.length;
    }

    if (objectType === 'object') {
      var length = _emberMetalProperty_get.get(obj, 'length');
      if (typeof length === 'number') {
        return !length;
      }
    }

    return false;
  }

  exports.default = isEmpty;
});
enifed("ember-metal/is_none", ["exports"], function (exports) {
  /**
    Returns true if the passed value is null or undefined. This avoids errors
    from JSLint complaining about use of ==, which can be technically
    confusing.
  
    ```javascript
    Ember.isNone();              // true
    Ember.isNone(null);          // true
    Ember.isNone(undefined);     // true
    Ember.isNone('');            // false
    Ember.isNone([]);            // false
    Ember.isNone(function() {}); // false
    ```
  
    @method isNone
    @for Ember
    @param {Object} obj Value to test
    @return {Boolean}
    @public
  */
  "use strict";

  exports.default = isNone;

  function isNone(obj) {
    return obj === null || obj === undefined;
  }
});
enifed('ember-metal/is_present', ['exports', 'ember-metal/is_blank'], function (exports, _emberMetalIs_blank) {
  'use strict';

  exports.default = isPresent;

  /**
    A value is present if it not `isBlank`.
  
    ```javascript
    Ember.isPresent();                // false
    Ember.isPresent(null);            // false
    Ember.isPresent(undefined);       // false
    Ember.isPresent('');              // false
    Ember.isPresent('  ');            // false
    Ember.isPresent('\n\t');          // false
    Ember.isPresent([]);              // false
    Ember.isPresent({ length: 0 })    // false
    Ember.isPresent(false);           // true
    Ember.isPresent(true);            // true
    Ember.isPresent('string');        // true
    Ember.isPresent(0);               // true
    Ember.isPresent(function() {})    // true
    Ember.isPresent({});              // true
    Ember.isPresent(false);           // true
    Ember.isPresent('\n\t Hello');    // true
    Ember.isPresent([1,2,3]);         // true
    ```
  
    @method isPresent
    @for Ember
    @param {Object} obj Value to test
    @return {Boolean}
    @since 1.8.0
    @public
  */

  function isPresent(obj) {
    return !_emberMetalIs_blank.default(obj);
  }
});
enifed('ember-metal/libraries', ['exports', 'ember-metal/debug', 'ember-metal/features'], function (exports, _emberMetalDebug, _emberMetalFeatures) {
  'use strict';

  /**
    Helper class that allows you to register your library with Ember.
  
    Singleton created at `Ember.libraries`.
  
    @class Libraries
    @constructor
    @private
  */
  function Libraries() {
    this._registry = [];
    this._coreLibIndex = 0;
  }

  Libraries.prototype = {
    constructor: Libraries,

    _getLibraryByName: function (name) {
      var libs = this._registry;
      var count = libs.length;

      for (var i = 0; i < count; i++) {
        if (libs[i].name === name) {
          return libs[i];
        }
      }
    },

    register: function (name, version, isCoreLibrary) {
      var index = this._registry.length;

      if (!this._getLibraryByName(name)) {
        if (isCoreLibrary) {
          index = this._coreLibIndex++;
        }
        this._registry.splice(index, 0, { name: name, version: version });
      } else {
        _emberMetalDebug.warn('Library "' + name + '" is already registered with Ember.', false, { id: 'ember-metal.libraries-register' });
      }
    },

    registerCoreLibrary: function (name, version) {
      this.register(name, version, true);
    },

    deRegister: function (name) {
      var lib = this._getLibraryByName(name);
      var index;

      if (lib) {
        index = this._registry.indexOf(lib);
        this._registry.splice(index, 1);
      }
    }
  };

  exports.default = Libraries;
});
enifed('ember-metal/logger', ['exports', 'ember-metal/core', 'ember-metal/error'], function (exports, _emberMetalCore, _emberMetalError) {
  'use strict';

  function K() {
    return this;
  }

  function consoleMethod(name) {
    var consoleObj, logToConsole;
    if (_emberMetalCore.default.imports.console) {
      consoleObj = _emberMetalCore.default.imports.console;
    } else if (typeof console !== 'undefined') {
      consoleObj = console;
    }

    var method = typeof consoleObj === 'object' ? consoleObj[name] : null;

    if (method) {
      // Older IE doesn't support bind, but Chrome needs it
      if (typeof method.bind === 'function') {
        logToConsole = method.bind(consoleObj);
        logToConsole.displayName = 'console.' + name;
        return logToConsole;
      } else if (typeof method.apply === 'function') {
        logToConsole = function () {
          method.apply(consoleObj, arguments);
        };
        logToConsole.displayName = 'console.' + name;
        return logToConsole;
      } else {
        return function () {
          var message = Array.prototype.join.call(arguments, ', ');
          method(message);
        };
      }
    }
  }

  function assertPolyfill(test, message) {
    if (!test) {
      try {
        // attempt to preserve the stack
        throw new _emberMetalError.default('assertion failed: ' + message);
      } catch (error) {
        setTimeout(function () {
          throw error;
        }, 0);
      }
    }
  }

  /**
    Inside Ember-Metal, simply uses the methods from `imports.console`.
    Override this to provide more robust logging functionality.
  
    @class Logger
    @namespace Ember
    @public
  */
  exports.default = {
    /**
     Logs the arguments to the console.
     You can pass as many arguments as you want and they will be joined together with a space.
       ```javascript
      var foo = 1;
      Ember.Logger.log('log value of foo:', foo);
      // "log value of foo: 1" will be printed to the console
      ```
      @method log
     @for Ember.Logger
     @param {*} arguments
     @public
    */
    log: consoleMethod('log') || K,

    /**
     Prints the arguments to the console with a warning icon.
     You can pass as many arguments as you want and they will be joined together with a space.
       ```javascript
      Ember.Logger.warn('Something happened!');
      // "Something happened!" will be printed to the console with a warning icon.
      ```
      @method warn
     @for Ember.Logger
     @param {*} arguments
     @public
    */
    warn: consoleMethod('warn') || K,

    /**
     Prints the arguments to the console with an error icon, red text and a stack trace.
     You can pass as many arguments as you want and they will be joined together with a space.
       ```javascript
      Ember.Logger.error('Danger! Danger!');
      // "Danger! Danger!" will be printed to the console in red text.
      ```
      @method error
     @for Ember.Logger
     @param {*} arguments
     @public
    */
    error: consoleMethod('error') || K,

    /**
     Logs the arguments to the console.
     You can pass as many arguments as you want and they will be joined together with a space.
       ```javascript
      var foo = 1;
      Ember.Logger.info('log value of foo:', foo);
      // "log value of foo: 1" will be printed to the console
      ```
      @method info
     @for Ember.Logger
     @param {*} arguments
     @public
    */
    info: consoleMethod('info') || K,

    /**
     Logs the arguments to the console in blue text.
     You can pass as many arguments as you want and they will be joined together with a space.
       ```javascript
      var foo = 1;
      Ember.Logger.debug('log value of foo:', foo);
      // "log value of foo: 1" will be printed to the console
      ```
      @method debug
     @for Ember.Logger
     @param {*} arguments
     @public
    */
    debug: consoleMethod('debug') || consoleMethod('info') || K,

    /**
     If the value passed into `Ember.Logger.assert` is not truthy it will throw an error with a stack trace.
       ```javascript
      Ember.Logger.assert(true); // undefined
      Ember.Logger.assert(true === false); // Throws an Assertion failed error.
      Ember.Logger.assert(true === false, 'Something invalid'); // Throws an Assertion failed error with message.
      ```
      @method assert
     @for Ember.Logger
     @param {Boolean} bool Value to test
     @param {String} message Assertion message on failed
     @public
    */
    assert: consoleMethod('assert') || assertPolyfill
  };
});
// Ember.imports
enifed('ember-metal/map', ['exports', 'ember-metal/core', 'ember-metal/utils', 'ember-metal/empty_object'], function (exports, _emberMetalCore, _emberMetalUtils, _emberMetalEmpty_object) {
  /**
  @module ember
  @submodule ember-metal
  */

  /*
    JavaScript (before ES6) does not have a Map implementation. Objects,
    which are often used as dictionaries, may only have Strings as keys.
  
    Because Ember has a way to get a unique identifier for every object
    via `Ember.guidFor`, we can implement a performant Map with arbitrary
    keys. Because it is commonly used in low-level bookkeeping, Map is
    implemented as a pure JavaScript object for performance.
  
    This implementation follows the current iteration of the ES6 proposal for
    maps (http://wiki.ecmascript.org/doku.php?id=harmony:simple_maps_and_sets),
    with one exception:  as we do not have the luxury of in-VM iteration, we implement a
    forEach method for iteration.
  
    Map is mocked out to look like an Ember object, so you can do
    `Ember.Map.create()` for symmetry with other Ember classes.
  */

  'use strict';

  function missingFunction(fn) {
    throw new TypeError(Object.prototype.toString.call(fn) + ' is not a function');
  }

  function missingNew(name) {
    throw new TypeError('Constructor ' + name + ' requires \'new\'');
  }

  function copyNull(obj) {
    var output = new _emberMetalEmpty_object.default();

    for (var prop in obj) {
      // hasOwnPropery is not needed because obj is new EmptyObject();
      output[prop] = obj[prop];
    }

    return output;
  }

  function copyMap(original, newObject) {
    var keys = original._keys.copy();
    var values = copyNull(original._values);

    newObject._keys = keys;
    newObject._values = values;
    newObject.size = original.size;

    return newObject;
  }

  /**
    This class is used internally by Ember and Ember Data.
    Please do not use it at this time. We plan to clean it up
    and add many tests soon.
  
    @class OrderedSet
    @namespace Ember
    @constructor
    @private
  */
  function OrderedSet() {
    if (this instanceof OrderedSet) {
      this.clear();
      this._silenceRemoveDeprecation = false;
    } else {
      missingNew('OrderedSet');
    }
  }

  /**
    @method create
    @static
    @return {Ember.OrderedSet}
    @private
  */
  OrderedSet.create = function () {
    var Constructor = this;

    return new Constructor();
  };

  OrderedSet.prototype = {
    constructor: OrderedSet,
    /**
      @method clear
      @private
    */
    clear: function () {
      this.presenceSet = new _emberMetalEmpty_object.default();
      this.list = [];
      this.size = 0;
    },

    /**
      @method add
      @param obj
      @param guid (optional, and for internal use)
      @return {Ember.OrderedSet}
      @private
    */
    add: function (obj, _guid) {
      var guid = _guid || _emberMetalUtils.guidFor(obj);
      var presenceSet = this.presenceSet;
      var list = this.list;

      if (presenceSet[guid] !== true) {
        presenceSet[guid] = true;
        this.size = list.push(obj);
      }

      return this;
    },

    /**
      @since 1.8.0
      @method delete
      @param obj
      @param _guid (optional and for internal use only)
      @return {Boolean}
      @private
    */
    delete: function (obj, _guid) {
      var guid = _guid || _emberMetalUtils.guidFor(obj);
      var presenceSet = this.presenceSet;
      var list = this.list;

      if (presenceSet[guid] === true) {
        delete presenceSet[guid];
        var index = list.indexOf(obj);
        if (index > -1) {
          list.splice(index, 1);
        }
        this.size = list.length;
        return true;
      } else {
        return false;
      }
    },

    /**
      @method isEmpty
      @return {Boolean}
      @private
    */
    isEmpty: function () {
      return this.size === 0;
    },

    /**
      @method has
      @param obj
      @return {Boolean}
      @private
    */
    has: function (obj) {
      if (this.size === 0) {
        return false;
      }

      var guid = _emberMetalUtils.guidFor(obj);
      var presenceSet = this.presenceSet;

      return presenceSet[guid] === true;
    },

    /**
      @method forEach
      @param {Function} fn
      @param self
      @private
    */
    forEach: function (fn /*, ...thisArg*/) {
      if (typeof fn !== 'function') {
        missingFunction(fn);
      }

      if (this.size === 0) {
        return;
      }

      var list = this.list;
      var length = arguments.length;
      var i;

      if (length === 2) {
        for (i = 0; i < list.length; i++) {
          fn.call(arguments[1], list[i]);
        }
      } else {
        for (i = 0; i < list.length; i++) {
          fn(list[i]);
        }
      }
    },

    /**
      @method toArray
      @return {Array}
      @private
    */
    toArray: function () {
      return this.list.slice();
    },

    /**
      @method copy
      @return {Ember.OrderedSet}
      @private
    */
    copy: function () {
      var Constructor = this.constructor;
      var set = new Constructor();

      set._silenceRemoveDeprecation = this._silenceRemoveDeprecation;
      set.presenceSet = copyNull(this.presenceSet);
      set.list = this.toArray();
      set.size = this.size;

      return set;
    }
  };

  /**
    A Map stores values indexed by keys. Unlike JavaScript's
    default Objects, the keys of a Map can be any JavaScript
    object.
  
    Internally, a Map has two data structures:
  
    1. `keys`: an OrderedSet of all of the existing keys
    2. `values`: a JavaScript Object indexed by the `Ember.guidFor(key)`
  
    When a key/value pair is added for the first time, we
    add the key to the `keys` OrderedSet, and create or
    replace an entry in `values`. When an entry is deleted,
    we delete its entry in `keys` and `values`.
  
    @class Map
    @namespace Ember
    @private
    @constructor
  */
  function Map() {
    if (this instanceof this.constructor) {
      this._keys = OrderedSet.create();
      this._keys._silenceRemoveDeprecation = true;
      this._values = new _emberMetalEmpty_object.default();
      this.size = 0;
    } else {
      missingNew('OrderedSet');
    }
  }

  _emberMetalCore.default.Map = Map;

  /**
    @method create
    @static
    @private
  */
  Map.create = function () {
    var Constructor = this;
    return new Constructor();
  };

  Map.prototype = {
    constructor: Map,

    /**
      This property will change as the number of objects in the map changes.
       @since 1.8.0
      @property size
      @type number
      @default 0
      @private
    */
    size: 0,

    /**
      Retrieve the value associated with a given key.
       @method get
      @param {*} key
      @return {*} the value associated with the key, or `undefined`
      @private
    */
    get: function (key) {
      if (this.size === 0) {
        return;
      }

      var values = this._values;
      var guid = _emberMetalUtils.guidFor(key);

      return values[guid];
    },

    /**
      Adds a value to the map. If a value for the given key has already been
      provided, the new value will replace the old value.
       @method set
      @param {*} key
      @param {*} value
      @return {Ember.Map}
      @private
    */
    set: function (key, value) {
      var keys = this._keys;
      var values = this._values;
      var guid = _emberMetalUtils.guidFor(key);

      // ensure we don't store -0
      var k = key === -0 ? 0 : key;

      keys.add(k, guid);

      values[guid] = value;

      this.size = keys.size;

      return this;
    },

    /**
      Removes a value from the map for an associated key.
       @since 1.8.0
      @method delete
      @param {*} key
      @return {Boolean} true if an item was removed, false otherwise
      @private
    */
    delete: function (key) {
      if (this.size === 0) {
        return false;
      }
      // don't use ES6 "delete" because it will be annoying
      // to use in browsers that are not ES6 friendly;
      var keys = this._keys;
      var values = this._values;
      var guid = _emberMetalUtils.guidFor(key);

      if (keys.delete(key, guid)) {
        delete values[guid];
        this.size = keys.size;
        return true;
      } else {
        return false;
      }
    },

    /**
      Check whether a key is present.
       @method has
      @param {*} key
      @return {Boolean} true if the item was present, false otherwise
      @private
    */
    has: function (key) {
      return this._keys.has(key);
    },

    /**
      Iterate over all the keys and values. Calls the function once
      for each key, passing in value, key, and the map being iterated over,
      in that order.
       The keys are guaranteed to be iterated over in insertion order.
       @method forEach
      @param {Function} callback
      @param {*} self if passed, the `this` value inside the
        callback. By default, `this` is the map.
      @private
    */
    forEach: function (callback /*, ...thisArg*/) {
      if (typeof callback !== 'function') {
        missingFunction(callback);
      }

      if (this.size === 0) {
        return;
      }

      var length = arguments.length;
      var map = this;
      var cb, thisArg;

      if (length === 2) {
        thisArg = arguments[1];
        cb = function (key) {
          callback.call(thisArg, map.get(key), key, map);
        };
      } else {
        cb = function (key) {
          callback(map.get(key), key, map);
        };
      }

      this._keys.forEach(cb);
    },

    /**
      @method clear
      @private
    */
    clear: function () {
      this._keys.clear();
      this._values = new _emberMetalEmpty_object.default();
      this.size = 0;
    },

    /**
      @method copy
      @return {Ember.Map}
      @private
    */
    copy: function () {
      return copyMap(this, new Map());
    }
  };

  /**
    @class MapWithDefault
    @namespace Ember
    @extends Ember.Map
    @private
    @constructor
    @param [options]
      @param {*} [options.defaultValue]
  */
  function MapWithDefault(options) {
    this._super$constructor();
    this.defaultValue = options.defaultValue;
  }

  /**
    @method create
    @static
    @param [options]
      @param {*} [options.defaultValue]
    @return {Ember.MapWithDefault|Ember.Map} If options are passed, returns
      `Ember.MapWithDefault` otherwise returns `Ember.Map`
    @private
  */
  MapWithDefault.create = function (options) {
    if (options) {
      return new MapWithDefault(options);
    } else {
      return new Map();
    }
  };

  MapWithDefault.prototype = Object.create(Map.prototype);
  MapWithDefault.prototype.constructor = MapWithDefault;
  MapWithDefault.prototype._super$constructor = Map;
  MapWithDefault.prototype._super$get = Map.prototype.get;

  /**
    Retrieve the value associated with a given key.
  
    @method get
    @param {*} key
    @return {*} the value associated with the key, or the default value
    @private
  */
  MapWithDefault.prototype.get = function (key) {
    var hasValue = this.has(key);

    if (hasValue) {
      return this._super$get(key);
    } else {
      var defaultValue = this.defaultValue(key);
      this.set(key, defaultValue);
      return defaultValue;
    }
  };

  /**
    @method copy
    @return {Ember.MapWithDefault}
    @private
  */
  MapWithDefault.prototype.copy = function () {
    var Constructor = this.constructor;
    return copyMap(this, new Constructor({
      defaultValue: this.defaultValue
    }));
  };

  exports.default = Map;
  exports.OrderedSet = OrderedSet;
  exports.Map = Map;
  exports.MapWithDefault = MapWithDefault;
});
enifed('ember-metal/merge', ['exports'], function (exports) {
  /**
    Merge the contents of two objects together into the first object.
  
    ```javascript
    Ember.merge({first: 'Tom'}, {last: 'Dale'}); // {first: 'Tom', last: 'Dale'}
    var a = {first: 'Yehuda'};
    var b = {last: 'Katz'};
    Ember.merge(a, b); // a == {first: 'Yehuda', last: 'Katz'}, b == {last: 'Katz'}
    ```
  
    @method merge
    @for Ember
    @param {Object} original The object to merge into
    @param {Object} updates The object to copy properties from
    @return {Object}
    @public
  */
  'use strict';

  exports.default = merge;

  function merge(original, updates) {
    if (!updates || typeof updates !== 'object') {
      return original;
    }

    var props = Object.keys(updates);
    var prop;
    var length = props.length;

    for (var i = 0; i < length; i++) {
      prop = props[i];
      original[prop] = updates[prop];
    }

    return original;
  }
});
enifed('ember-metal/meta', ['exports', 'ember-metal/meta_listeners', 'ember-metal/empty_object'], function (exports, _emberMetalMeta_listeners, _emberMetalEmpty_object) {
  'no use strict';
  // Remove "use strict"; from transpiled module until
  // https://bugs.webkit.org/show_bug.cgi?id=138038 is fixed

  exports.meta = meta;
  exports.peekMeta = peekMeta;
  exports.deleteMeta = deleteMeta;

  /**
  @module ember-metal
  */

  /*
   This declares several meta-programmed members on the Meta class. Such
   meta!
  
   In general, the `readable` variants will give you an object (if it
   already exists) that you can read but should not modify. The
   `writable` variants will give you a mutable object, and they will
   create it if it didn't already exist.
  
   The following methods will get generated metaprogrammatically, and
   I'm including them here for greppability:
  
   writableCache, readableCache, writeWatching,
   peekWatching, clearWatching, writeMixins,
   peekMixins, clearMixins, writeBindings,
   peekBindings, clearBindings, writeValues,
   peekValues, clearValues, writeDeps, forEachInDeps
   writableChainWatchers, readableChainWatchers, writableChains,
   readableChains, writableTag, readableTag
  
  */
  var members = {
    cache: ownMap,
    weak: ownMap,
    watching: inheritedMap,
    mixins: inheritedMap,
    bindings: inheritedMap,
    values: inheritedMap,
    deps: inheritedMapOfMaps,
    chainWatchers: ownCustomObject,
    chains: inheritedCustomObject,
    tag: ownCustomObject
  };

  var memberNames = Object.keys(members);
  var META_FIELD = '__ember_meta__';

  function Meta(obj, parentMeta) {
    this._cache = undefined;
    this._weak = undefined;
    this._watching = undefined;
    this._mixins = undefined;
    this._bindings = undefined;
    this._values = undefined;
    this._deps = undefined;
    this._chainWatchers = undefined;
    this._chains = undefined;
    this._tag = undefined;

    // used only internally
    this.source = obj;

    // when meta(obj).proto === obj, the object is intended to be only a
    // prototype and doesn't need to actually be observable itself
    this.proto = undefined;

    // The next meta in our inheritance chain. We (will) track this
    // explicitly instead of using prototypical inheritance because we
    // have detailed knowledge of how each property should really be
    // inherited, and we can optimize it much better than JS runtimes.
    this.parent = parentMeta;

    this._initializeListeners();
  }

  for (var _name in _emberMetalMeta_listeners.protoMethods) {
    Meta.prototype[_name] = _emberMetalMeta_listeners.protoMethods[_name];
  }
  memberNames.forEach(function (name) {
    return members[name](name, Meta);
  });

  // Implements a member that is a lazily created, non-inheritable
  // POJO.
  function ownMap(name, Meta) {
    var key = memberProperty(name);
    var capitalized = capitalize(name);
    Meta.prototype['writable' + capitalized] = function () {
      return this._getOrCreateOwnMap(key);
    };
    Meta.prototype['readable' + capitalized] = function () {
      return this[key];
    };
  }

  Meta.prototype._getOrCreateOwnMap = function (key) {
    var ret = this[key];
    if (!ret) {
      ret = this[key] = new _emberMetalEmpty_object.default();
    }
    return ret;
  };

  // Implements a member that is a lazily created POJO with inheritable
  // values.
  function inheritedMap(name, Meta) {
    var key = memberProperty(name);
    var capitalized = capitalize(name);

    Meta.prototype['write' + capitalized] = function (subkey, value) {
      var map = this._getOrCreateOwnMap(key);
      map[subkey] = value;
    };

    Meta.prototype['peek' + capitalized] = function (subkey) {
      return this._findInherited(key, subkey);
    };

    Meta.prototype['forEach' + capitalized] = function (fn) {
      var pointer = this;
      var seen = new _emberMetalEmpty_object.default();
      while (pointer !== undefined) {
        var map = pointer[key];
        if (map) {
          for (var _key in map) {
            if (!seen[_key]) {
              seen[_key] = true;
              fn(_key, map[_key]);
            }
          }
        }
        pointer = pointer.parent;
      }
    };

    Meta.prototype['clear' + capitalized] = function () {
      this[key] = undefined;
    };

    Meta.prototype['deleteFrom' + capitalized] = function (subkey) {
      delete this._getOrCreateOwnMap(key)[subkey];
    };

    Meta.prototype['hasIn' + capitalized] = function (subkey) {
      return this._findInherited(key, subkey) !== undefined;
    };
  }

  Meta.prototype._getInherited = function (key) {
    var pointer = this;
    while (pointer !== undefined) {
      if (pointer[key]) {
        return pointer[key];
      }
      pointer = pointer.parent;
    }
  };

  Meta.prototype._findInherited = function (key, subkey) {
    var pointer = this;
    while (pointer !== undefined) {
      var map = pointer[key];
      if (map) {
        var value = map[subkey];
        if (value !== undefined) {
          return value;
        }
      }
      pointer = pointer.parent;
    }
  };

  // Implements a member that provides a lazily created map of maps,
  // with inheritance at both levels.
  function inheritedMapOfMaps(name, Meta) {
    var key = memberProperty(name);
    var capitalized = capitalize(name);

    Meta.prototype['write' + capitalized] = function (subkey, itemkey, value) {
      var outerMap = this._getOrCreateOwnMap(key);
      var innerMap = outerMap[subkey];
      if (!innerMap) {
        innerMap = outerMap[subkey] = new _emberMetalEmpty_object.default();
      }
      innerMap[itemkey] = value;
    };

    Meta.prototype['peek' + capitalized] = function (subkey, itemkey) {
      var pointer = this;
      while (pointer !== undefined) {
        var map = pointer[key];
        if (map) {
          var value = map[subkey];
          if (value) {
            if (value[itemkey] !== undefined) {
              return value[itemkey];
            }
          }
        }
        pointer = pointer.parent;
      }
    };

    Meta.prototype['has' + capitalized] = function (subkey) {
      var pointer = this;
      while (pointer !== undefined) {
        if (pointer[key] && pointer[key][subkey]) {
          return true;
        }
        pointer = pointer.parent;
      }
      return false;
    };

    Meta.prototype['forEachIn' + capitalized] = function (subkey, fn) {
      return this._forEachIn(key, subkey, fn);
    };
  }

  Meta.prototype._forEachIn = function (key, subkey, fn) {
    var pointer = this;
    var seen = new _emberMetalEmpty_object.default();
    var calls = [];
    while (pointer !== undefined) {
      var map = pointer[key];
      if (map) {
        var innerMap = map[subkey];
        if (innerMap) {
          for (var innerKey in innerMap) {
            if (!seen[innerKey]) {
              seen[innerKey] = true;
              calls.push([innerKey, innerMap[innerKey]]);
            }
          }
        }
      }
      pointer = pointer.parent;
    }
    for (var i = 0; i < calls.length; i++) {
      var _calls$i = calls[i];
      var innerKey = _calls$i[0];
      var value = _calls$i[1];

      fn(innerKey, value);
    }
  };

  // Implements a member that provides a non-heritable, lazily-created
  // object using the method you provide.
  function ownCustomObject(name, Meta) {
    var key = memberProperty(name);
    var capitalized = capitalize(name);
    Meta.prototype['writable' + capitalized] = function (create) {
      var ret = this[key];
      if (!ret) {
        ret = this[key] = create(this.source);
      }
      return ret;
    };
    Meta.prototype['readable' + capitalized] = function () {
      return this[key];
    };
  }

  // Implements a member that provides an inheritable, lazily-created
  // object using the method you provide. We will derived children from
  // their parents by calling your object's `copy()` method.
  function inheritedCustomObject(name, Meta) {
    var key = memberProperty(name);
    var capitalized = capitalize(name);
    Meta.prototype['writable' + capitalized] = function (create) {
      var ret = this[key];
      if (!ret) {
        if (this.parent) {
          ret = this[key] = this.parent['writable' + capitalized](create).copy(this.source);
        } else {
          ret = this[key] = create(this.source);
        }
      }
      return ret;
    };
    Meta.prototype['readable' + capitalized] = function () {
      return this._getInherited(key);
    };
  }

  function memberProperty(name) {
    return '_' + name;
  }

  // there's a more general-purpose capitalize in ember-runtime, but we
  // don't want to make ember-metal depend on ember-runtime.
  function capitalize(name) {
    return name.replace(/^\w/, function (m) {
      return m.toUpperCase();
    });
  }

  var META_DESC = {
    writable: true,
    configurable: true,
    enumerable: false,
    value: null
  };

  exports.META_DESC = META_DESC;
  var EMBER_META_PROPERTY = {
    name: META_FIELD,
    descriptor: META_DESC
  };

  // choose the one appropriate for given platform
  var setMeta = function (obj, meta) {
    // if `null` already, just set it to the new value
    // otherwise define property first
    if (obj[META_FIELD] !== null) {
      if (obj.__defineNonEnumerable) {
        obj.__defineNonEnumerable(EMBER_META_PROPERTY);
      } else {
        Object.defineProperty(obj, META_FIELD, META_DESC);
      }
    }

    obj[META_FIELD] = meta;
  };

  /**
    Retrieves the meta hash for an object. If `writable` is true ensures the
    hash is writable for this object as well.
  
    The meta object contains information about computed property descriptors as
    well as any watched properties and other information. You generally will
    not access this information directly but instead work with higher level
    methods that manipulate this hash indirectly.
  
    @method meta
    @for Ember
    @private
  
    @param {Object} obj The object to retrieve meta for
    @param {Boolean} [writable=true] Pass `false` if you do not intend to modify
      the meta hash, allowing the method to avoid making an unnecessary copy.
    @return {Object} the meta hash for an object
  */

  function meta(obj) {
    var maybeMeta = peekMeta(obj);
    var parent = undefined;

    // remove this code, in-favor of explicit parent
    if (maybeMeta) {
      if (maybeMeta.source === obj) {
        return maybeMeta;
      }
      parent = maybeMeta;
    }

    var newMeta = new Meta(obj, parent);
    setMeta(obj, newMeta);
    return newMeta;
  }

  function peekMeta(obj) {
    return obj[META_FIELD];
  }

  function deleteMeta(obj) {
    if (typeof obj[META_FIELD] !== 'object') {
      return;
    }
    obj[META_FIELD] = null;
  }
});
enifed('ember-metal/meta_listeners', ['exports'], function (exports) {
  /*
   When we render a rich template hierarchy, the set of events that
   *might* happen tends to be much larger than the set of events that
   actually happen. This implies that we should make listener creation &
   destruction cheap, even at the cost of making event dispatch more
   expensive.
  
   Thus we store a new listener with a single push and no new
   allocations, without even bothering to do deduplication -- we can
   save that for dispatch time, if an event actually happens.
   */

  /* listener flags */
  'use strict';

  var ONCE = 1;
  exports.ONCE = ONCE;
  var SUSPENDED = 2;

  exports.SUSPENDED = SUSPENDED;
  var protoMethods = {

    addToListeners: function (eventName, target, method, flags) {
      if (!this._listeners) {
        this._listeners = [];
      }
      this._listeners.push(eventName, target, method, flags);
    },

    _finalizeListeners: function () {
      if (this._listenersFinalized) {
        return;
      }
      if (!this._listeners) {
        this._listeners = [];
      }
      var pointer = this.parent;
      while (pointer) {
        var listeners = pointer._listeners;
        if (listeners) {
          this._listeners = this._listeners.concat(listeners);
        }
        if (pointer._listenersFinalized) {
          break;
        }
        pointer = pointer.parent;
      }
      this._listenersFinalized = true;
    },

    removeFromListeners: function (eventName, target, method, didRemove) {
      var pointer = this;
      while (pointer) {
        var listeners = pointer._listeners;
        if (listeners) {
          for (var index = listeners.length - 4; index >= 0; index -= 4) {
            if (listeners[index] === eventName && (!method || listeners[index + 1] === target && listeners[index + 2] === method)) {
              if (pointer === this) {
                // we are modifying our own list, so we edit directly
                if (typeof didRemove === 'function') {
                  didRemove(eventName, target, listeners[index + 2]);
                }
                listeners.splice(index, 4);
              } else {
                // we are trying to remove an inherited listener, so we do
                // just-in-time copying to detach our own listeners from
                // our inheritance chain.
                this._finalizeListeners();
                return this.removeFromListeners(eventName, target, method);
              }
            }
          }
        }
        if (pointer._listenersFinalized) {
          break;
        }
        pointer = pointer.parent;
      }
    },

    matchingListeners: function (eventName) {
      var pointer = this;
      var result = [];
      while (pointer) {
        var listeners = pointer._listeners;
        if (listeners) {
          for (var index = 0; index < listeners.length - 3; index += 4) {
            if (listeners[index] === eventName) {
              pushUniqueListener(result, listeners, index);
            }
          }
        }
        if (pointer._listenersFinalized) {
          break;
        }
        pointer = pointer.parent;
      }
      var sus = this._suspendedListeners;
      if (sus) {
        for (var susIndex = 0; susIndex < sus.length - 2; susIndex += 3) {
          if (eventName === sus[susIndex]) {
            for (var resultIndex = 0; resultIndex < result.length - 2; resultIndex += 3) {
              if (result[resultIndex] === sus[susIndex + 1] && result[resultIndex + 1] === sus[susIndex + 2]) {
                result[resultIndex + 2] |= SUSPENDED;
              }
            }
          }
        }
      }
      return result;
    },

    suspendListeners: function (eventNames, target, method, callback) {
      var sus = this._suspendedListeners;
      if (!sus) {
        sus = this._suspendedListeners = [];
      }
      for (var i = 0; i < eventNames.length; i++) {
        sus.push(eventNames[i], target, method);
      }
      try {
        return callback.call(target);
      } finally {
        if (sus.length === eventNames.length) {
          this._suspendedListeners = undefined;
        } else {
          for (var i = sus.length - 3; i >= 0; i -= 3) {
            if (sus[i + 1] === target && sus[i + 2] === method && eventNames.indexOf(sus[i]) !== -1) {
              sus.splice(i, 3);
            }
          }
        }
      }
    },

    watchedEvents: function () {
      var pointer = this;
      var names = {};
      while (pointer) {
        var listeners = pointer._listeners;
        if (listeners) {
          for (var index = 0; index < listeners.length - 3; index += 4) {
            names[listeners[index]] = true;
          }
        }
        if (pointer._listenersFinalized) {
          break;
        }
        pointer = pointer.parent;
      }
      return Object.keys(names);
    },

    _initializeListeners: function () {
      this._listeners = undefined;
      this._listenersFinalized = undefined;
      this._suspendedListeners = undefined;
    }
  };

  exports.protoMethods = protoMethods;
  function pushUniqueListener(destination, source, index) {
    var target = source[index + 1];
    var method = source[index + 2];
    for (var destinationIndex = 0; destinationIndex < destination.length - 2; destinationIndex += 3) {
      if (destination[destinationIndex] === target && destination[destinationIndex + 1] === method) {
        return;
      }
    }
    destination.push(target, method, source[index + 3]);
  }
});
enifed('ember-metal/mixin', ['exports', 'ember-metal/core', 'ember-metal/error', 'ember-metal/debug', 'ember-metal/assign', 'ember-metal/empty_object', 'ember-metal/property_get', 'ember-metal/property_set', 'ember-metal/utils', 'ember-metal/meta', 'ember-metal/expand_properties', 'ember-metal/properties', 'ember-metal/computed', 'ember-metal/binding', 'ember-metal/observer', 'ember-metal/events', 'ember-metal/streams/utils'], function (exports, _emberMetalCore, _emberMetalError, _emberMetalDebug, _emberMetalAssign, _emberMetalEmpty_object, _emberMetalProperty_get, _emberMetalProperty_set, _emberMetalUtils, _emberMetalMeta, _emberMetalExpand_properties, _emberMetalProperties, _emberMetalComputed, _emberMetalBinding, _emberMetalObserver, _emberMetalEvents, _emberMetalStreamsUtils) {
  'no use strict';
  // Remove "use strict"; from transpiled module until
  // https://bugs.webkit.org/show_bug.cgi?id=138038 is fixed

  /**
  @module ember
  @submodule ember-metal
  */
  exports.mixin = mixin;
  exports.default = Mixin;
  exports.required = required;
  exports.aliasMethod = aliasMethod;
  exports.observer = observer;
  exports._immediateObserver = _immediateObserver;
  exports._beforeObserver = _beforeObserver;

  function ROOT() {}
  ROOT.__hasSuper = false;

  var REQUIRED;
  var a_slice = [].slice;

  function isMethod(obj) {
    return 'function' === typeof obj && obj.isMethod !== false && obj !== Boolean && obj !== Object && obj !== Number && obj !== Array && obj !== Date && obj !== String;
  }

  var CONTINUE = {};

  function mixinProperties(mixinsMeta, mixin) {
    var guid;

    if (mixin instanceof Mixin) {
      guid = _emberMetalUtils.guidFor(mixin);
      if (mixinsMeta.peekMixins(guid)) {
        return CONTINUE;
      }
      mixinsMeta.writeMixins(guid, mixin);
      return mixin.properties;
    } else {
      return mixin; // apply anonymous mixin properties
    }
  }

  function concatenatedMixinProperties(concatProp, props, values, base) {
    var concats;

    // reset before adding each new mixin to pickup concats from previous
    concats = values[concatProp] || base[concatProp];
    if (props[concatProp]) {
      concats = concats ? concats.concat(props[concatProp]) : props[concatProp];
    }

    return concats;
  }

  function giveDescriptorSuper(meta, key, property, values, descs, base) {
    var superProperty;

    // Computed properties override methods, and do not call super to them
    if (values[key] === undefined) {
      // Find the original descriptor in a parent mixin
      superProperty = descs[key];
    }

    // If we didn't find the original descriptor in a parent mixin, find
    // it on the original object.
    if (!superProperty) {
      var possibleDesc = base[key];
      var superDesc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;

      superProperty = superDesc;
    }

    if (superProperty === undefined || !(superProperty instanceof _emberMetalComputed.ComputedProperty)) {
      return property;
    }

    // Since multiple mixins may inherit from the same parent, we need
    // to clone the computed property so that other mixins do not receive
    // the wrapped version.
    property = Object.create(property);
    property._getter = _emberMetalUtils.wrap(property._getter, superProperty._getter);
    if (superProperty._setter) {
      if (property._setter) {
        property._setter = _emberMetalUtils.wrap(property._setter, superProperty._setter);
      } else {
        property._setter = superProperty._setter;
      }
    }

    return property;
  }

  function giveMethodSuper(obj, key, method, values, descs) {
    var superMethod;

    // Methods overwrite computed properties, and do not call super to them.
    if (descs[key] === undefined) {
      // Find the original method in a parent mixin
      superMethod = values[key];
    }

    // If we didn't find the original value in a parent mixin, find it in
    // the original object
    superMethod = superMethod || obj[key];

    // Only wrap the new method if the original method was a function
    if (superMethod === undefined || 'function' !== typeof superMethod) {
      return method;
    }

    return _emberMetalUtils.wrap(method, superMethod);
  }

  function applyConcatenatedProperties(obj, key, value, values) {
    var baseValue = values[key] || obj[key];

    if (baseValue) {
      if ('function' === typeof baseValue.concat) {
        if (value === null || value === undefined) {
          return baseValue;
        } else {
          return baseValue.concat(value);
        }
      } else {
        return _emberMetalUtils.makeArray(baseValue).concat(value);
      }
    } else {
      return _emberMetalUtils.makeArray(value);
    }
  }

  function applyMergedProperties(obj, key, value, values) {
    var baseValue = values[key] || obj[key];

    _emberMetalDebug.runInDebug(function () {
      if (Array.isArray(value)) {
        // use conditional to avoid stringifying every time
        _emberMetalDebug.assert('You passed in `' + JSON.stringify(value) + '` as the value for `' + key + '` but `' + key + '` cannot be an Array', false);
      }
    });

    if (!baseValue) {
      return value;
    }

    var newBase = _emberMetalAssign.default({}, baseValue);
    var hasFunction = false;

    for (var prop in value) {
      if (!value.hasOwnProperty(prop)) {
        continue;
      }

      var propValue = value[prop];
      if (isMethod(propValue)) {
        // TODO: support for Computed Properties, etc?
        hasFunction = true;
        newBase[prop] = giveMethodSuper(obj, prop, propValue, baseValue, {});
      } else {
        newBase[prop] = propValue;
      }
    }

    if (hasFunction) {
      newBase._super = ROOT;
    }

    return newBase;
  }

  function addNormalizedProperty(base, key, value, meta, descs, values, concats, mergings) {
    if (value instanceof _emberMetalProperties.Descriptor) {
      if (value === REQUIRED && descs[key]) {
        return CONTINUE;
      }

      // Wrap descriptor function to implement
      // _super() if needed
      if (value._getter) {
        value = giveDescriptorSuper(meta, key, value, values, descs, base);
      }

      descs[key] = value;
      values[key] = undefined;
    } else {
      if (concats && concats.indexOf(key) >= 0 || key === 'concatenatedProperties' || key === 'mergedProperties') {
        value = applyConcatenatedProperties(base, key, value, values);
      } else if (mergings && mergings.indexOf(key) >= 0) {
        value = applyMergedProperties(base, key, value, values);
      } else if (isMethod(value)) {
        value = giveMethodSuper(base, key, value, values, descs);
      }

      descs[key] = undefined;
      values[key] = value;
    }
  }

  function mergeMixins(mixins, m, descs, values, base, keys) {
    var currentMixin, props, key, concats, mergings, meta;

    function removeKeys(keyName) {
      delete descs[keyName];
      delete values[keyName];
    }

    for (var i = 0, l = mixins.length; i < l; i++) {
      currentMixin = mixins[i];
      _emberMetalDebug.assert('Expected hash or Mixin instance, got ' + Object.prototype.toString.call(currentMixin), typeof currentMixin === 'object' && currentMixin !== null && Object.prototype.toString.call(currentMixin) !== '[object Array]');

      props = mixinProperties(m, currentMixin);
      if (props === CONTINUE) {
        continue;
      }

      if (props) {
        meta = _emberMetalMeta.meta(base);
        if (base.willMergeMixin) {
          base.willMergeMixin(props);
        }
        concats = concatenatedMixinProperties('concatenatedProperties', props, values, base);
        mergings = concatenatedMixinProperties('mergedProperties', props, values, base);

        for (key in props) {
          if (!props.hasOwnProperty(key)) {
            continue;
          }
          keys.push(key);
          addNormalizedProperty(base, key, props[key], meta, descs, values, concats, mergings);
        }

        // manually copy toString() because some JS engines do not enumerate it
        if (props.hasOwnProperty('toString')) {
          base.toString = props.toString;
        }
      } else if (currentMixin.mixins) {
        mergeMixins(currentMixin.mixins, m, descs, values, base, keys);
        if (currentMixin._without) {
          currentMixin._without.forEach(removeKeys);
        }
      }
    }
  }

  var IS_BINDING = /^.+Binding$/;

  function detectBinding(obj, key, value, m) {
    if (IS_BINDING.test(key)) {
      m.writeBindings(key, value);
    }
  }

  function connectStreamBinding(obj, key, stream) {
    var onNotify = function (stream) {
      _emberMetalObserver._suspendObserver(obj, key, null, didChange, function () {
        _emberMetalProperty_set.trySet(obj, key, stream.value());
      });
    };

    var didChange = function () {
      stream.setValue(_emberMetalProperty_get.get(obj, key), onNotify);
    };

    // Initialize value
    _emberMetalProperty_set.set(obj, key, stream.value());

    _emberMetalObserver.addObserver(obj, key, null, didChange);

    stream.subscribe(onNotify);

    if (obj._streamBindingSubscriptions === undefined) {
      obj._streamBindingSubscriptions = new _emberMetalEmpty_object.default();
    }

    obj._streamBindingSubscriptions[key] = onNotify;
  }

  function connectBindings(obj, m) {
    // TODO Mixin.apply(instance) should disconnect binding if exists
    m.forEachBindings(function (key, binding) {
      if (binding) {
        var to = key.slice(0, -7); // strip Binding off end
        if (_emberMetalStreamsUtils.isStream(binding)) {
          connectStreamBinding(obj, to, binding);
          return;
        } else if (binding instanceof _emberMetalBinding.Binding) {
          binding = binding.copy(); // copy prototypes' instance
          binding.to(to);
        } else {
          // binding is string path
          binding = new _emberMetalBinding.Binding(to, binding);
        }
        binding.connect(obj);
        obj[key] = binding;
      }
    });
    // mark as applied
    m.clearBindings();
  }

  function finishPartial(obj, m) {
    connectBindings(obj, m || _emberMetalMeta.meta(obj));
    return obj;
  }

  function followAlias(obj, desc, m, descs, values) {
    var altKey = desc.methodName;
    var value;
    var possibleDesc;
    if (descs[altKey] || values[altKey]) {
      value = values[altKey];
      desc = descs[altKey];
    } else if ((possibleDesc = obj[altKey]) && possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor) {
      desc = possibleDesc;
      value = undefined;
    } else {
      desc = undefined;
      value = obj[altKey];
    }

    return { desc: desc, value: value };
  }

  function updateObserversAndListeners(obj, key, observerOrListener, pathsKey, updateMethod) {
    var paths = observerOrListener[pathsKey];

    if (paths) {
      for (var i = 0, l = paths.length; i < l; i++) {
        updateMethod(obj, paths[i], null, key);
      }
    }
  }

  function replaceObserversAndListeners(obj, key, observerOrListener) {
    var prev = obj[key];

    if ('function' === typeof prev) {
      updateObserversAndListeners(obj, key, prev, '__ember_observesBefore__', _emberMetalObserver._removeBeforeObserver);
      updateObserversAndListeners(obj, key, prev, '__ember_observes__', _emberMetalObserver.removeObserver);
      updateObserversAndListeners(obj, key, prev, '__ember_listens__', _emberMetalEvents.removeListener);
    }

    if ('function' === typeof observerOrListener) {
      updateObserversAndListeners(obj, key, observerOrListener, '__ember_observesBefore__', _emberMetalObserver._addBeforeObserver);
      updateObserversAndListeners(obj, key, observerOrListener, '__ember_observes__', _emberMetalObserver.addObserver);
      updateObserversAndListeners(obj, key, observerOrListener, '__ember_listens__', _emberMetalEvents.addListener);
    }
  }

  function applyMixin(obj, mixins, partial) {
    var descs = {};
    var values = {};
    var m = _emberMetalMeta.meta(obj);
    var keys = [];
    var key, value, desc;

    obj._super = ROOT;

    // Go through all mixins and hashes passed in, and:
    //
    // * Handle concatenated properties
    // * Handle merged properties
    // * Set up _super wrapping if necessary
    // * Set up computed property descriptors
    // * Copying `toString` in broken browsers
    mergeMixins(mixins, m, descs, values, obj, keys);

    for (var i = 0, l = keys.length; i < l; i++) {
      key = keys[i];
      if (key === 'constructor' || !values.hasOwnProperty(key)) {
        continue;
      }

      desc = descs[key];
      value = values[key];

      if (desc === REQUIRED) {
        continue;
      }

      while (desc && desc instanceof Alias) {
        var followed = followAlias(obj, desc, m, descs, values);
        desc = followed.desc;
        value = followed.value;
      }

      if (desc === undefined && value === undefined) {
        continue;
      }

      replaceObserversAndListeners(obj, key, value);
      detectBinding(obj, key, value, m);
      _emberMetalProperties.defineProperty(obj, key, desc, value, m);
    }

    if (!partial) {
      // don't apply to prototype
      finishPartial(obj, m);
    }

    return obj;
  }

  /**
    @method mixin
    @for Ember
    @param obj
    @param mixins*
    @return obj
    @private
  */

  function mixin(obj) {
    for (var _len = arguments.length, args = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
      args[_key - 1] = arguments[_key];
    }

    applyMixin(obj, args, false);
    return obj;
  }

  /**
    The `Ember.Mixin` class allows you to create mixins, whose properties can be
    added to other classes. For instance,
  
    ```javascript
    App.Editable = Ember.Mixin.create({
      edit: function() {
        console.log('starting to edit');
        this.set('isEditing', true);
      },
      isEditing: false
    });
  
    // Mix mixins into classes by passing them as the first arguments to
    // .extend.
    App.CommentView = Ember.View.extend(App.Editable, {
      template: Ember.Handlebars.compile('{{#if view.isEditing}}...{{else}}...{{/if}}')
    });
  
    commentView = App.CommentView.create();
    commentView.edit(); // outputs 'starting to edit'
    ```
  
    Note that Mixins are created with `Ember.Mixin.create`, not
    `Ember.Mixin.extend`.
  
    Note that mixins extend a constructor's prototype so arrays and object literals
    defined as properties will be shared amongst objects that implement the mixin.
    If you want to define a property in a mixin that is not shared, you can define
    it either as a computed property or have it be created on initialization of the object.
  
    ```javascript
    //filters array will be shared amongst any object implementing mixin
    App.Filterable = Ember.Mixin.create({
      filters: Ember.A()
    });
  
    //filters will be a separate  array for every object implementing the mixin
    App.Filterable = Ember.Mixin.create({
      filters: Ember.computed(function() {return Ember.A();})
    });
  
    //filters will be created as a separate array during the object's initialization
    App.Filterable = Ember.Mixin.create({
      init: function() {
        this._super(...arguments);
        this.set("filters", Ember.A());
      }
    });
    ```
  
    @class Mixin
    @namespace Ember
    @public
  */

  function Mixin(args, properties) {
    this.properties = properties;

    var length = args && args.length;

    if (length > 0) {
      var m = new Array(length);

      for (var i = 0; i < length; i++) {
        var x = args[i];
        if (x instanceof Mixin) {
          m[i] = x;
        } else {
          m[i] = new Mixin(undefined, x);
        }
      }

      this.mixins = m;
    } else {
      this.mixins = undefined;
    }
    this.ownerConstructor = undefined;
    this._without = undefined;
    this[_emberMetalUtils.GUID_KEY] = null;
    this[_emberMetalUtils.GUID_KEY + '_name'] = null;
    _emberMetalDebug.debugSeal(this);
  }

  Mixin._apply = applyMixin;

  Mixin.applyPartial = function (obj) {
    var args = a_slice.call(arguments, 1);
    return applyMixin(obj, args, true);
  };

  Mixin.finishPartial = finishPartial;

  // ES6TODO: this relies on a global state?
  _emberMetalCore.default.anyUnprocessedMixins = false;

  /**
    @method create
    @static
    @param arguments*
    @public
  */
  Mixin.create = function () {
    // ES6TODO: this relies on a global state?
    _emberMetalCore.default.anyUnprocessedMixins = true;
    var M = this;

    for (var _len2 = arguments.length, args = Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
      args[_key2] = arguments[_key2];
    }

    return new M(args, undefined);
  };

  var MixinPrototype = Mixin.prototype;

  /**
    @method reopen
    @param arguments*
    @private
  */
  MixinPrototype.reopen = function () {
    var currentMixin;

    if (this.properties) {
      currentMixin = new Mixin(undefined, this.properties);
      this.properties = undefined;
      this.mixins = [currentMixin];
    } else if (!this.mixins) {
      this.mixins = [];
    }

    var len = arguments.length;
    var mixins = this.mixins;
    var idx;

    for (idx = 0; idx < len; idx++) {
      currentMixin = arguments[idx];
      _emberMetalDebug.assert('Expected hash or Mixin instance, got ' + Object.prototype.toString.call(currentMixin), typeof currentMixin === 'object' && currentMixin !== null && Object.prototype.toString.call(currentMixin) !== '[object Array]');

      if (currentMixin instanceof Mixin) {
        mixins.push(currentMixin);
      } else {
        mixins.push(new Mixin(undefined, currentMixin));
      }
    }

    return this;
  };

  /**
    @method apply
    @param obj
    @return applied object
    @private
  */
  MixinPrototype.apply = function (obj) {
    return applyMixin(obj, [this], false);
  };

  MixinPrototype.applyPartial = function (obj) {
    return applyMixin(obj, [this], true);
  };

  MixinPrototype.toString = function Mixin_toString() {
    return '(unknown mixin)';
  };

  function _detect(curMixin, targetMixin, seen) {
    var guid = _emberMetalUtils.guidFor(curMixin);

    if (seen[guid]) {
      return false;
    }
    seen[guid] = true;

    if (curMixin === targetMixin) {
      return true;
    }
    var mixins = curMixin.mixins;
    var loc = mixins ? mixins.length : 0;
    while (--loc >= 0) {
      if (_detect(mixins[loc], targetMixin, seen)) {
        return true;
      }
    }
    return false;
  }

  /**
    @method detect
    @param obj
    @return {Boolean}
    @private
  */
  MixinPrototype.detect = function (obj) {
    if (!obj) {
      return false;
    }
    if (obj instanceof Mixin) {
      return _detect(obj, this, {});
    }
    var m = _emberMetalMeta.peekMeta(obj);
    if (!m) {
      return false;
    }
    return !!m.peekMixins(_emberMetalUtils.guidFor(this));
  };

  MixinPrototype.without = function () {
    var ret = new Mixin([this]);

    for (var _len3 = arguments.length, args = Array(_len3), _key3 = 0; _key3 < _len3; _key3++) {
      args[_key3] = arguments[_key3];
    }

    ret._without = args;
    return ret;
  };

  function _keys(ret, mixin, seen) {
    if (seen[_emberMetalUtils.guidFor(mixin)]) {
      return;
    }
    seen[_emberMetalUtils.guidFor(mixin)] = true;

    if (mixin.properties) {
      var props = Object.keys(mixin.properties);
      for (var i = 0; i < props.length; i++) {
        var key = props[i];
        ret[key] = true;
      }
    } else if (mixin.mixins) {
      mixin.mixins.forEach(function (x) {
        return _keys(ret, x, seen);
      });
    }
  }

  MixinPrototype.keys = function () {
    var keys = {};
    var seen = {};

    _keys(keys, this, seen);
    var ret = Object.keys(keys);
    return ret;
  };

  _emberMetalDebug.debugSeal(MixinPrototype);

  // returns the mixins currently applied to the specified object
  // TODO: Make Ember.mixin
  Mixin.mixins = function (obj) {
    var m = _emberMetalMeta.peekMeta(obj);
    var ret = [];
    if (!m) {
      return ret;
    }

    m.forEachMixins(function (key, currentMixin) {
      // skip primitive mixins since these are always anonymous
      if (!currentMixin.properties) {
        ret.push(currentMixin);
      }
    });

    return ret;
  };

  exports.REQUIRED = REQUIRED = new _emberMetalProperties.Descriptor();
  REQUIRED.toString = function () {
    return '(Required Property)';
  };

  /**
    Denotes a required property for a mixin
  
    @method required
    @for Ember
    @private
  */

  function required() {
    _emberMetalDebug.deprecate('Ember.required is deprecated as its behavior is inconsistent and unreliable.', false, { id: 'ember-metal.required', until: '3.0.0' });
    return REQUIRED;
  }

  function Alias(methodName) {
    this.isDescriptor = true;
    this.methodName = methodName;
  }

  Alias.prototype = new _emberMetalProperties.Descriptor();

  /**
    Makes a method available via an additional name.
  
    ```javascript
    App.Person = Ember.Object.extend({
      name: function() {
        return 'Tomhuda Katzdale';
      },
      moniker: Ember.aliasMethod('name')
    });
  
    var goodGuy = App.Person.create();
  
    goodGuy.name();    // 'Tomhuda Katzdale'
    goodGuy.moniker(); // 'Tomhuda Katzdale'
    ```
  
    @method aliasMethod
    @for Ember
    @param {String} methodName name of the method to alias
    @public
  */

  function aliasMethod(methodName) {
    return new Alias(methodName);
  }

  // ..........................................................
  // OBSERVER HELPER
  //

  /**
    Specify a method that observes property changes.
  
    ```javascript
    Ember.Object.extend({
      valueObserver: Ember.observer('value', function() {
        // Executes whenever the "value" property changes
      })
    });
    ```
  
    Also available as `Function.prototype.observes` if prototype extensions are
    enabled.
  
    @method observer
    @for Ember
    @param {String} propertyNames*
    @param {Function} func
    @return func
    @public
  */

  function observer() {
    for (var _len4 = arguments.length, args = Array(_len4), _key4 = 0; _key4 < _len4; _key4++) {
      args[_key4] = arguments[_key4];
    }

    var func = args.slice(-1)[0];
    var paths;

    var addWatchedProperty = function (path) {
      paths.push(path);
    };
    var _paths = args.slice(0, -1);

    if (typeof func !== 'function') {
      // revert to old, soft-deprecated argument ordering
      _emberMetalDebug.deprecate('Passing the dependentKeys after the callback function in Ember.observer is deprecated. Ensure the callback function is the last argument.', false, { id: 'ember-metal.observer-argument-order', until: '3.0.0' });

      func = args[0];
      _paths = args.slice(1);
    }

    paths = [];

    for (var i = 0; i < _paths.length; ++i) {
      _emberMetalExpand_properties.default(_paths[i], addWatchedProperty);
    }

    if (typeof func !== 'function') {
      throw new _emberMetalError.default('Ember.observer called without a function');
    }

    func.__ember_observes__ = paths;
    return func;
  }

  /**
    Specify a method that observes property changes.
  
    ```javascript
    Ember.Object.extend({
      valueObserver: Ember.immediateObserver('value', function() {
        // Executes whenever the "value" property changes
      })
    });
    ```
  
    In the future, `Ember.observer` may become asynchronous. In this event,
    `Ember.immediateObserver` will maintain the synchronous behavior.
  
    Also available as `Function.prototype.observesImmediately` if prototype extensions are
    enabled.
  
    @method _immediateObserver
    @for Ember
    @param {String} propertyNames*
    @param {Function} func
    @deprecated Use `Ember.observer` instead.
    @return func
    @private
  */

  function _immediateObserver() {
    _emberMetalDebug.deprecate('Usage of `Ember.immediateObserver` is deprecated, use `Ember.observer` instead.', false, { id: 'ember-metal.immediate-observer', until: '3.0.0' });

    for (var i = 0, l = arguments.length; i < l; i++) {
      var arg = arguments[i];
      _emberMetalDebug.assert('Immediate observers must observe internal properties only, not properties on other objects.', typeof arg !== 'string' || arg.indexOf('.') === -1);
    }

    return observer.apply(this, arguments);
  }

  /**
    When observers fire, they are called with the arguments `obj`, `keyName`.
  
    Note, `@each.property` observer is called per each add or replace of an element
    and it's not called with a specific enumeration item.
  
    A `_beforeObserver` fires before a property changes.
  
    A `_beforeObserver` is an alternative form of `.observesBefore()`.
  
    ```javascript
    App.PersonView = Ember.View.extend({
      friends: [{ name: 'Tom' }, { name: 'Stefan' }, { name: 'Kris' }],
  
      valueDidChange: Ember.observer('content.value', function(obj, keyName) {
          // only run if updating a value already in the DOM
          if (this.get('state') === 'inDOM') {
            var color = obj.get(keyName) > this.changingFrom ? 'green' : 'red';
            // logic
          }
      }),
  
      friendsDidChange: Ember.observer('friends.@each.name', function(obj, keyName) {
        // some logic
        // obj.get(keyName) returns friends array
      })
    });
    ```
  
    Also available as `Function.prototype.observesBefore` if prototype extensions are
    enabled.
  
    @method beforeObserver
    @for Ember
    @param {String} propertyNames*
    @param {Function} func
    @return func
    @deprecated
    @private
  */

  function _beforeObserver() {
    for (var _len5 = arguments.length, args = Array(_len5), _key5 = 0; _key5 < _len5; _key5++) {
      args[_key5] = arguments[_key5];
    }

    var func = args.slice(-1)[0];
    var paths;

    var addWatchedProperty = function (path) {
      paths.push(path);
    };

    var _paths = args.slice(0, -1);

    if (typeof func !== 'function') {
      // revert to old, soft-deprecated argument ordering

      func = args[0];
      _paths = args.slice(1);
    }

    paths = [];

    for (var i = 0; i < _paths.length; ++i) {
      _emberMetalExpand_properties.default(_paths[i], addWatchedProperty);
    }

    if (typeof func !== 'function') {
      throw new _emberMetalCore.default.Error('Ember.beforeObserver called without a function');
    }

    func.__ember_observesBefore__ = paths;
    return func;
  }

  exports.IS_BINDING = IS_BINDING;
  exports.Mixin = Mixin;
  exports.required = required;
  exports.REQUIRED = REQUIRED;
});
// warn, assert, wrap, et;
enifed('ember-metal/observer', ['exports', 'ember-metal/watching', 'ember-metal/events'], function (exports, _emberMetalWatching, _emberMetalEvents) {
  'use strict';

  exports.addObserver = addObserver;
  exports.observersFor = observersFor;
  exports.removeObserver = removeObserver;
  exports._addBeforeObserver = _addBeforeObserver;
  exports._suspendObserver = _suspendObserver;
  exports._suspendObservers = _suspendObservers;
  exports._removeBeforeObserver = _removeBeforeObserver;

  /**
  @module ember-metal
  */

  var AFTER_OBSERVERS = ':change';
  var BEFORE_OBSERVERS = ':before';

  function changeEvent(keyName) {
    return keyName + AFTER_OBSERVERS;
  }

  function beforeEvent(keyName) {
    return keyName + BEFORE_OBSERVERS;
  }

  /**
    @method addObserver
    @for Ember
    @param obj
    @param {String} _path
    @param {Object|Function} target
    @param {Function|String} [method]
    @public
  */

  function addObserver(obj, _path, target, method) {
    _emberMetalEvents.addListener(obj, changeEvent(_path), target, method);
    _emberMetalWatching.watch(obj, _path);

    return this;
  }

  function observersFor(obj, path) {
    return _emberMetalEvents.listenersFor(obj, changeEvent(path));
  }

  /**
    @method removeObserver
    @for Ember
    @param obj
    @param {String} path
    @param {Object|Function} target
    @param {Function|String} [method]
    @public
  */

  function removeObserver(obj, path, target, method) {
    _emberMetalWatching.unwatch(obj, path);
    _emberMetalEvents.removeListener(obj, changeEvent(path), target, method);

    return this;
  }

  /**
    @method _addBeforeObserver
    @for Ember
    @param obj
    @param {String} path
    @param {Object|Function} target
    @param {Function|String} [method]
    @deprecated
    @private
  */

  function _addBeforeObserver(obj, path, target, method) {
    _emberMetalEvents.addListener(obj, beforeEvent(path), target, method);
    _emberMetalWatching.watch(obj, path);

    return this;
  }

  // Suspend observer during callback.
  //
  // This should only be used by the target of the observer
  // while it is setting the observed path.

  function _suspendObserver(obj, path, target, method, callback) {
    return _emberMetalEvents.suspendListener(obj, changeEvent(path), target, method, callback);
  }

  function _suspendObservers(obj, paths, target, method, callback) {
    var events = paths.map(changeEvent);
    return _emberMetalEvents.suspendListeners(obj, events, target, method, callback);
  }

  /**
    @method removeBeforeObserver
    @for Ember
    @param obj
    @param {String} path
    @param {Object|Function} target
    @param {Function|String} [method]
    @deprecated
    @private
  */

  function _removeBeforeObserver(obj, path, target, method) {
    _emberMetalWatching.unwatch(obj, path);
    _emberMetalEvents.removeListener(obj, beforeEvent(path), target, method);

    return this;
  }
});
enifed('ember-metal/observer_set', ['exports', 'ember-metal/utils', 'ember-metal/events'], function (exports, _emberMetalUtils, _emberMetalEvents) {
  'use strict';

  /*
    this.observerSet = {
      [senderGuid]: { // variable name: `keySet`
        [keyName]: listIndex
      }
    },
    this.observers = [
      {
        sender: obj,
        keyName: keyName,
        eventName: eventName,
        listeners: [
          [target, method, flags]
        ]
      },
      ...
    ]
  */
  exports.default = ObserverSet;

  function ObserverSet() {
    this.clear();
  }

  ObserverSet.prototype.add = function (sender, keyName, eventName) {
    var observerSet = this.observerSet;
    var observers = this.observers;
    var senderGuid = _emberMetalUtils.guidFor(sender);
    var keySet = observerSet[senderGuid];
    var index;

    if (!keySet) {
      observerSet[senderGuid] = keySet = {};
    }
    index = keySet[keyName];
    if (index === undefined) {
      index = observers.push({
        sender: sender,
        keyName: keyName,
        eventName: eventName,
        listeners: []
      }) - 1;
      keySet[keyName] = index;
    }
    return observers[index].listeners;
  };

  ObserverSet.prototype.flush = function () {
    var observers = this.observers;
    var i, len, observer, sender;
    this.clear();
    for (i = 0, len = observers.length; i < len; ++i) {
      observer = observers[i];
      sender = observer.sender;
      if (sender.isDestroying || sender.isDestroyed) {
        continue;
      }
      _emberMetalEvents.sendEvent(sender, observer.eventName, [sender, observer.keyName], observer.listeners);
    }
  };

  ObserverSet.prototype.clear = function () {
    this.observerSet = {};
    this.observers = [];
  };
});
enifed('ember-metal/path_cache', ['exports', 'ember-metal/cache'], function (exports, _emberMetalCache) {
  'use strict';

  exports.isGlobal = isGlobal;
  exports.isGlobalPath = isGlobalPath;
  exports.hasThis = hasThis;
  exports.isPath = isPath;
  exports.getFirstKey = getFirstKey;
  exports.getTailPath = getTailPath;

  var IS_GLOBAL = /^[A-Z$]/;
  var IS_GLOBAL_PATH = /^[A-Z$].*[\.]/;
  var HAS_THIS = 'this.';

  var isGlobalCache = new _emberMetalCache.default(1000, function (key) {
    return IS_GLOBAL.test(key);
  });

  var isGlobalPathCache = new _emberMetalCache.default(1000, function (key) {
    return IS_GLOBAL_PATH.test(key);
  });

  var hasThisCache = new _emberMetalCache.default(1000, function (key) {
    return key.lastIndexOf(HAS_THIS, 0) === 0;
  });

  var firstDotIndexCache = new _emberMetalCache.default(1000, function (key) {
    return key.indexOf('.');
  });

  var firstKeyCache = new _emberMetalCache.default(1000, function (path) {
    var index = firstDotIndexCache.get(path);
    if (index === -1) {
      return path;
    } else {
      return path.slice(0, index);
    }
  });

  var tailPathCache = new _emberMetalCache.default(1000, function (path) {
    var index = firstDotIndexCache.get(path);
    if (index !== -1) {
      return path.slice(index + 1);
    }
  });

  var caches = {
    isGlobalCache: isGlobalCache,
    isGlobalPathCache: isGlobalPathCache,
    hasThisCache: hasThisCache,
    firstDotIndexCache: firstDotIndexCache,
    firstKeyCache: firstKeyCache,
    tailPathCache: tailPathCache
  };

  exports.caches = caches;

  function isGlobal(path) {
    return isGlobalCache.get(path);
  }

  function isGlobalPath(path) {
    return isGlobalPathCache.get(path);
  }

  function hasThis(path) {
    return hasThisCache.get(path);
  }

  function isPath(path) {
    return firstDotIndexCache.get(path) !== -1;
  }

  function getFirstKey(path) {
    return firstKeyCache.get(path);
  }

  function getTailPath(path) {
    return tailPathCache.get(path);
  }
});
enifed('ember-metal/properties', ['exports', 'ember-metal/debug', 'ember-metal/features', 'ember-metal/meta', 'ember-metal/property_events'], function (exports, _emberMetalDebug, _emberMetalFeatures, _emberMetalMeta, _emberMetalProperty_events) {
  /**
  @module ember-metal
  */

  'use strict';

  exports.Descriptor = Descriptor;
  exports.MANDATORY_SETTER_FUNCTION = MANDATORY_SETTER_FUNCTION;
  exports.DEFAULT_GETTER_FUNCTION = DEFAULT_GETTER_FUNCTION;
  exports.INHERITING_GETTER_FUNCTION = INHERITING_GETTER_FUNCTION;
  exports.defineProperty = defineProperty;

  // ..........................................................
  // DESCRIPTOR
  //

  /**
    Objects of this type can implement an interface to respond to requests to
    get and set. The default implementation handles simple properties.
  
    @class Descriptor
    @private
  */

  function Descriptor() {
    this.isDescriptor = true;
  }

  var REDEFINE_SUPPORTED = (function () {
    // https://github.com/spalger/kibana/commit/b7e35e6737df585585332857a4c397dc206e7ff9
    var a = Object.create(Object.prototype, {
      prop: {
        configurable: true,
        value: 1
      }
    });

    Object.defineProperty(a, 'prop', {
      configurable: true,
      value: 2
    });

    return a.prop === 2;
  })();
  // ..........................................................
  // DEFINING PROPERTIES API
  //

  function MANDATORY_SETTER_FUNCTION(name) {
    function SETTER_FUNCTION(value) {
      _emberMetalDebug.assert('You must use Ember.set() to set the `' + name + '` property (of ' + this + ') to `' + value + '`.', false);
    }

    SETTER_FUNCTION.isMandatorySetter = true;
    return SETTER_FUNCTION;
  }

  function DEFAULT_GETTER_FUNCTION(name) {
    return function GETTER_FUNCTION() {
      var meta = this['__ember_meta__'];
      return meta && meta.peekValues(name);
    };
  }

  function INHERITING_GETTER_FUNCTION(name) {
    function IGETTER_FUNCTION() {
      var proto = Object.getPrototypeOf(this);
      return proto && proto[name];
    }

    IGETTER_FUNCTION.isInheritingGetter = true;
    return IGETTER_FUNCTION;
  }

  /**
    NOTE: This is a low-level method used by other parts of the API. You almost
    never want to call this method directly. Instead you should use
    `Ember.mixin()` to define new properties.
  
    Defines a property on an object. This method works much like the ES5
    `Object.defineProperty()` method except that it can also accept computed
    properties and other special descriptors.
  
    Normally this method takes only three parameters. However if you pass an
    instance of `Descriptor` as the third param then you can pass an
    optional value as the fourth parameter. This is often more efficient than
    creating new descriptor hashes for each property.
  
    ## Examples
  
    ```javascript
    // ES5 compatible mode
    Ember.defineProperty(contact, 'firstName', {
      writable: true,
      configurable: false,
      enumerable: true,
      value: 'Charles'
    });
  
    // define a simple property
    Ember.defineProperty(contact, 'lastName', undefined, 'Jolley');
  
    // define a computed property
    Ember.defineProperty(contact, 'fullName', Ember.computed('firstName', 'lastName', function() {
      return this.firstName+' '+this.lastName;
    }));
    ```
  
    @private
    @method defineProperty
    @for Ember
    @param {Object} obj the object to define this property on. This may be a prototype.
    @param {String} keyName the name of the property
    @param {Descriptor} [desc] an instance of `Descriptor` (typically a
      computed property) or an ES5 descriptor.
      You must provide this or `data` but not both.
    @param {*} [data] something other than a descriptor, that will
      become the explicit value of this property.
  */

  function defineProperty(obj, keyName, desc, data, meta) {
    var possibleDesc, existingDesc, watching, value;

    if (!meta) {
      meta = _emberMetalMeta.meta(obj);
    }
    var watchEntry = meta.peekWatching(keyName);
    possibleDesc = obj[keyName];
    existingDesc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;

    watching = watchEntry !== undefined && watchEntry > 0;

    if (existingDesc) {
      existingDesc.teardown(obj, keyName);
    }

    if (desc instanceof Descriptor) {
      value = desc;

      if (watching) {
        Object.defineProperty(obj, keyName, {
          configurable: true,
          enumerable: true,
          writable: true,
          value: value
        });
      } else {
        obj[keyName] = value;
      }

      if (desc.setup) {
        desc.setup(obj, keyName);
      }
    } else {
      if (desc == null) {
        value = data;

        if (watching) {
          meta.writeValues(keyName, data);

          var defaultDescriptor = {
            configurable: true,
            enumerable: true,
            set: MANDATORY_SETTER_FUNCTION(keyName),
            get: DEFAULT_GETTER_FUNCTION(keyName)
          };

          if (REDEFINE_SUPPORTED) {
            Object.defineProperty(obj, keyName, defaultDescriptor);
          } else {
            handleBrokenPhantomDefineProperty(obj, keyName, defaultDescriptor);
          }
        } else {
          obj[keyName] = data;
        }
      } else {
        value = desc;

        // fallback to ES5
        Object.defineProperty(obj, keyName, desc);
      }
    }

    // if key is being watched, override chains that
    // were initialized with the prototype
    if (watching) {
      _emberMetalProperty_events.overrideChains(obj, keyName, meta);
    }

    // The `value` passed to the `didDefineProperty` hook is
    // either the descriptor or data, whichever was passed.
    if (obj.didDefineProperty) {
      obj.didDefineProperty(obj, keyName, value);
    }

    return this;
  }

  function handleBrokenPhantomDefineProperty(obj, keyName, desc) {
    // https://github.com/ariya/phantomjs/issues/11856
    Object.defineProperty(obj, keyName, { configurable: true, writable: true, value: 'iCry' });
    Object.defineProperty(obj, keyName, desc);
  }
});
enifed('ember-metal/property_events', ['exports', 'ember-metal/utils', 'ember-metal/meta', 'ember-metal/events', 'ember-metal/observer_set', 'ember-metal/symbol'], function (exports, _emberMetalUtils, _emberMetalMeta, _emberMetalEvents, _emberMetalObserver_set, _emberMetalSymbol) {
  'use strict';

  var PROPERTY_DID_CHANGE = _emberMetalSymbol.default('PROPERTY_DID_CHANGE');

  exports.PROPERTY_DID_CHANGE = PROPERTY_DID_CHANGE;
  var beforeObserverSet = new _emberMetalObserver_set.default();
  var observerSet = new _emberMetalObserver_set.default();
  var deferred = 0;

  // ..........................................................
  // PROPERTY CHANGES
  //

  /**
    This function is called just before an object property is about to change.
    It will notify any before observers and prepare caches among other things.
  
    Normally you will not need to call this method directly but if for some
    reason you can't directly watch a property you can invoke this method
    manually along with `Ember.propertyDidChange()` which you should call just
    after the property value changes.
  
    @method propertyWillChange
    @for Ember
    @param {Object} obj The object with the property that will change
    @param {String} keyName The property key (or path) that will change.
    @return {void}
    @private
  */
  function propertyWillChange(obj, keyName) {
    var m = _emberMetalMeta.peekMeta(obj);
    var watching = m && m.peekWatching(keyName) > 0 || keyName === 'length';
    var proto = m && m.proto;
    var possibleDesc = obj[keyName];
    var desc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;

    if (!watching) {
      return;
    }

    if (proto === obj) {
      return;
    }

    if (desc && desc.willChange) {
      desc.willChange(obj, keyName);
    }

    dependentKeysWillChange(obj, keyName, m);
    chainsWillChange(obj, keyName, m);
    notifyBeforeObservers(obj, keyName);
  }

  /**
    This function is called just after an object property has changed.
    It will notify any observers and clear caches among other things.
  
    Normally you will not need to call this method directly but if for some
    reason you can't directly watch a property you can invoke this method
    manually along with `Ember.propertyWillChange()` which you should call just
    before the property value changes.
  
    @method propertyDidChange
    @for Ember
    @param {Object} obj The object with the property that will change
    @param {String} keyName The property key (or path) that will change.
    @return {void}
    @private
  */
  function propertyDidChange(obj, keyName) {
    var m = _emberMetalMeta.peekMeta(obj);
    var watching = m && m.peekWatching(keyName) > 0 || keyName === 'length';
    var proto = m && m.proto;
    var possibleDesc = obj[keyName];
    var desc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;

    if (proto === obj) {
      return;
    }

    // shouldn't this mean that we're watching this key?
    if (desc && desc.didChange) {
      desc.didChange(obj, keyName);
    }

    if (obj[PROPERTY_DID_CHANGE]) {
      obj[PROPERTY_DID_CHANGE](keyName);
    }

    if (!watching && keyName !== 'length') {
      return;
    }

    if (m && m.hasDeps(keyName)) {
      dependentKeysDidChange(obj, keyName, m);
    }

    chainsDidChange(obj, keyName, m, false);
    notifyObservers(obj, keyName);
  }

  var WILL_SEEN, DID_SEEN;
  // called whenever a property is about to change to clear the cache of any dependent keys (and notify those properties of changes, etc...)
  function dependentKeysWillChange(obj, depKey, meta) {
    if (obj.isDestroying) {
      return;
    }

    if (meta && meta.hasDeps(depKey)) {
      var seen = WILL_SEEN;
      var top = !seen;

      if (top) {
        seen = WILL_SEEN = {};
      }

      iterDeps(propertyWillChange, obj, depKey, seen, meta);

      if (top) {
        WILL_SEEN = null;
      }
    }
  }

  // called whenever a property has just changed to update dependent keys
  function dependentKeysDidChange(obj, depKey, meta) {
    if (obj.isDestroying) {
      return;
    }

    if (meta && meta.hasDeps(depKey)) {
      var seen = DID_SEEN;
      var top = !seen;

      if (top) {
        seen = DID_SEEN = {};
      }

      iterDeps(propertyDidChange, obj, depKey, seen, meta);

      if (top) {
        DID_SEEN = null;
      }
    }
  }

  function iterDeps(method, obj, depKey, seen, meta) {
    var possibleDesc, desc;
    var guid = _emberMetalUtils.guidFor(obj);
    var current = seen[guid];

    if (!current) {
      current = seen[guid] = {};
    }

    if (current[depKey]) {
      return;
    }

    current[depKey] = true;

    meta.forEachInDeps(depKey, function (key, value) {
      if (!value) {
        return;
      }

      possibleDesc = obj[key];
      desc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;

      if (desc && desc._suspended === obj) {
        return;
      }

      method(obj, key);
    });
  }

  function chainsWillChange(obj, keyName, m) {
    var c = m.readableChainWatchers();
    if (c) {
      c.notify(keyName, false, propertyWillChange);
    }
  }

  function chainsDidChange(obj, keyName, m) {
    var c = m.readableChainWatchers();
    if (c) {
      c.notify(keyName, true, propertyDidChange);
    }
  }

  function overrideChains(obj, keyName, m) {
    var c = m.readableChainWatchers();
    if (c) {
      c.revalidate(keyName);
    }
  }

  /**
    @method beginPropertyChanges
    @chainable
    @private
  */
  function beginPropertyChanges() {
    deferred++;
  }

  /**
    @method endPropertyChanges
    @private
  */
  function endPropertyChanges() {
    deferred--;
    if (deferred <= 0) {
      beforeObserverSet.clear();
      observerSet.flush();
    }
  }

  /**
    Make a series of property changes together in an
    exception-safe way.
  
    ```javascript
    Ember.changeProperties(function() {
      obj1.set('foo', mayBlowUpWhenSet);
      obj2.set('bar', baz);
    });
    ```
  
    @method changeProperties
    @param {Function} callback
    @param [binding]
    @private
  */
  function changeProperties(callback, binding) {
    beginPropertyChanges();
    try {
      callback.call(binding);
    } finally {
      endPropertyChanges.call(binding);
    }
  }

  function notifyBeforeObservers(obj, keyName) {
    if (obj.isDestroying) {
      return;
    }

    var eventName = keyName + ':before';
    var listeners, added;
    if (deferred) {
      listeners = beforeObserverSet.add(obj, keyName, eventName);
      added = _emberMetalEvents.accumulateListeners(obj, eventName, listeners);
      _emberMetalEvents.sendEvent(obj, eventName, [obj, keyName], added);
    } else {
      _emberMetalEvents.sendEvent(obj, eventName, [obj, keyName]);
    }
  }

  function notifyObservers(obj, keyName) {
    if (obj.isDestroying) {
      return;
    }

    var eventName = keyName + ':change';
    var listeners;
    if (deferred) {
      listeners = observerSet.add(obj, keyName, eventName);
      _emberMetalEvents.accumulateListeners(obj, eventName, listeners);
    } else {
      _emberMetalEvents.sendEvent(obj, eventName, [obj, keyName]);
    }
  }

  exports.propertyWillChange = propertyWillChange;
  exports.propertyDidChange = propertyDidChange;
  exports.overrideChains = overrideChains;
  exports.beginPropertyChanges = beginPropertyChanges;
  exports.endPropertyChanges = endPropertyChanges;
  exports.changeProperties = changeProperties;
});
enifed('ember-metal/property_get', ['exports', 'ember-metal/debug', 'ember-metal/path_cache'], function (exports, _emberMetalDebug, _emberMetalPath_cache) {
  /**
  @module ember-metal
  */

  'use strict';

  exports.get = get;
  exports._getPath = _getPath;
  exports.getWithDefault = getWithDefault;

  var ALLOWABLE_TYPES = {
    object: true,
    function: true,
    string: true
  };

  // ..........................................................
  // GET AND SET
  //
  // If we are on a platform that supports accessors we can use those.
  // Otherwise simulate accessors by looking up the property directly on the
  // object.

  /**
    Gets the value of a property on an object. If the property is computed,
    the function will be invoked. If the property is not defined but the
    object implements the `unknownProperty` method then that will be invoked.
  
    If you plan to run on IE8 and older browsers then you should use this
    method anytime you want to retrieve a property on an object that you don't
    know for sure is private. (Properties beginning with an underscore '_'
    are considered private.)
  
    On all newer browsers, you only need to use this method to retrieve
    properties if the property might not be defined on the object and you want
    to respect the `unknownProperty` handler. Otherwise you can ignore this
    method.
  
    Note that if the object itself is `undefined`, this method will throw
    an error.
  
    @method get
    @for Ember
    @param {Object} obj The object to retrieve from.
    @param {String} keyName The property key to retrieve
    @return {Object} the property value or `null`.
    @public
  */

  function get(obj, keyName) {
    _emberMetalDebug.assert('Get must be called with two arguments; an object and a property key', arguments.length === 2);
    _emberMetalDebug.assert('Cannot call get with \'' + keyName + '\' on an undefined object.', obj !== undefined && obj !== null);
    _emberMetalDebug.assert('The key provided to get must be a string, you passed ' + keyName, typeof keyName === 'string');
    _emberMetalDebug.assert('\'this\' in paths is not supported', !_emberMetalPath_cache.hasThis(keyName));

    // Helpers that operate with 'this' within an #each
    if (keyName === '') {
      return obj;
    }

    var value = obj[keyName];
    var desc = value !== null && typeof value === 'object' && value.isDescriptor ? value : undefined;
    var ret;

    if (desc === undefined && _emberMetalPath_cache.isPath(keyName)) {
      return _getPath(obj, keyName);
    }

    if (desc) {
      return desc.get(obj, keyName);
    } else {
      ret = value;

      if (ret === undefined && 'object' === typeof obj && !(keyName in obj) && 'function' === typeof obj.unknownProperty) {
        return obj.unknownProperty(keyName);
      }

      return ret;
    }
  }

  function _getPath(root, path) {
    var obj = root;
    var parts = path.split('.');

    for (var i = 0; i < parts.length; i++) {
      if (!isGettable(obj)) {
        return undefined;
      }

      obj = get(obj, parts[i]);

      if (obj && obj.isDestroyed) {
        return undefined;
      }
    }

    return obj;
  }

  function isGettable(obj) {
    if (obj == null) {
      return false;
    }

    return ALLOWABLE_TYPES[typeof obj];
  }

  /**
    Retrieves the value of a property from an Object, or a default value in the
    case that the property returns `undefined`.
  
    ```javascript
    Ember.getWithDefault(person, 'lastName', 'Doe');
    ```
  
    @method getWithDefault
    @for Ember
    @param {Object} obj The object to retrieve from.
    @param {String} keyName The name of the property to retrieve
    @param {Object} defaultValue The value to return if the property value is undefined
    @return {Object} The property value or the defaultValue.
    @public
  */

  function getWithDefault(root, key, defaultValue) {
    var value = get(root, key);

    if (value === undefined) {
      return defaultValue;
    }
    return value;
  }

  exports.default = get;
});
enifed('ember-metal/property_set', ['exports', 'ember-metal/debug', 'ember-metal/features', 'ember-metal/property_get', 'ember-metal/property_events', 'ember-metal/properties', 'ember-metal/error', 'ember-metal/path_cache', 'ember-metal/meta', 'ember-metal/utils', 'ember-metal/tags'], function (exports, _emberMetalDebug, _emberMetalFeatures, _emberMetalProperty_get, _emberMetalProperty_events, _emberMetalProperties, _emberMetalError, _emberMetalPath_cache, _emberMetalMeta, _emberMetalUtils, _emberMetalTags) {
  'use strict';

  exports.set = set;
  exports.trySet = trySet;

  /**
    Sets the value of a property on an object, respecting computed properties
    and notifying observers and other listeners of the change. If the
    property is not defined but the object implements the `setUnknownProperty`
    method then that will be invoked as well.
  
    @method set
    @for Ember
    @param {Object} obj The object to modify.
    @param {String} keyName The property key to set
    @param {Object} value The value to set
    @return {Object} the passed value.
    @public
  */

  function set(obj, keyName, value, tolerant) {
    _emberMetalDebug.assert('Set must be called with three or four arguments; an object, a property key, a value and tolerant true/false', arguments.length === 3 || arguments.length === 4);
    _emberMetalDebug.assert('Cannot call set with \'' + keyName + '\' on an undefined object.', obj !== undefined && obj !== null);
    _emberMetalDebug.assert('The key provided to set must be a string, you passed ' + keyName, typeof keyName === 'string');
    _emberMetalDebug.assert('\'this\' in paths is not supported', !_emberMetalPath_cache.hasThis(keyName));

    var meta = undefined,
        possibleDesc = undefined,
        desc = undefined;

    if (obj) {
      meta = _emberMetalMeta.peekMeta(obj);
      possibleDesc = obj[keyName];
      desc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;
      _emberMetalTags.markObjectAsDirty(meta);
    }

    var isUnknown, currentValue;
    if (desc === undefined && _emberMetalPath_cache.isPath(keyName)) {
      return setPath(obj, keyName, value, tolerant);
    }

    _emberMetalDebug.assert('calling set on destroyed object: ' + _emberMetalUtils.toString(obj) + '.' + keyName + ' = ' + _emberMetalUtils.toString(value), !obj.isDestroyed);

    if (desc) {
      desc.set(obj, keyName, value);
    } else {
      if (value !== undefined && typeof obj === 'object' && obj[keyName] === value) {
        return value;
      }

      isUnknown = 'object' === typeof obj && !(keyName in obj);

      // setUnknownProperty is called if `obj` is an object,
      // the property does not already exist, and the
      // `setUnknownProperty` method exists on the object
      if (isUnknown && 'function' === typeof obj.setUnknownProperty) {
        obj.setUnknownProperty(keyName, value);
      } else if (meta && meta.peekWatching(keyName) > 0) {
        if (meta.proto !== obj) {
          currentValue = obj[keyName];
        }
        // only trigger a change if the value has changed
        if (value !== currentValue) {
          _emberMetalProperty_events.propertyWillChange(obj, keyName);

          if (currentValue === undefined && !(keyName in obj) || !Object.prototype.propertyIsEnumerable.call(obj, keyName)) {
            _emberMetalProperties.defineProperty(obj, keyName, null, value); // setup mandatory setter
          } else {
              var descriptor = _emberMetalUtils.lookupDescriptor(obj, keyName);
              var isMandatorySetter = descriptor && descriptor.set && descriptor.set.isMandatorySetter;
              if (isMandatorySetter) {
                meta.writeValues(keyName, value);
              } else {
                obj[keyName] = value;
              }
            }

          _emberMetalProperty_events.propertyDidChange(obj, keyName);
        }
      } else {
        obj[keyName] = value;
        if (obj[_emberMetalProperty_events.PROPERTY_DID_CHANGE]) {
          obj[_emberMetalProperty_events.PROPERTY_DID_CHANGE](keyName);
        }
      }
    }
    return value;
  }

  function setPath(root, path, value, tolerant) {
    var keyName;

    // get the last part of the path
    keyName = path.slice(path.lastIndexOf('.') + 1);

    // get the first part of the part
    path = path === keyName ? keyName : path.slice(0, path.length - (keyName.length + 1));

    // unless the path is this, look up the first part to
    // get the root
    if (path !== 'this') {
      root = _emberMetalProperty_get._getPath(root, path);
    }

    if (!keyName || keyName.length === 0) {
      throw new _emberMetalError.default('Property set failed: You passed an empty path');
    }

    if (!root) {
      if (tolerant) {
        return;
      } else {
        throw new _emberMetalError.default('Property set failed: object in path "' + path + '" could not be found or was destroyed.');
      }
    }

    return set(root, keyName, value);
  }

  /**
    Error-tolerant form of `Ember.set`. Will not blow up if any part of the
    chain is `undefined`, `null`, or destroyed.
  
    This is primarily used when syncing bindings, which may try to update after
    an object has been destroyed.
  
    @method trySet
    @for Ember
    @param {Object} root The object to modify.
    @param {String} path The property path to set
    @param {Object} value The value to set
    @public
  */

  function trySet(root, path, value) {
    return set(root, path, value, true);
  }
});
enifed("ember-metal/replace", ["exports"], function (exports) {
  "use strict";

  exports._replace = _replace;
  exports.default = replace;
  var splice = Array.prototype.splice;

  function _replace(array, idx, amt, objects) {
    var args = [].concat(objects);
    var ret = [];
    // https://code.google.com/p/chromium/issues/detail?id=56588
    var size = 60000;
    var start = idx;
    var ends = amt;
    var count, chunk;

    while (args.length) {
      count = ends > size ? size : ends;
      if (count <= 0) {
        count = 0;
      }

      chunk = args.splice(0, size);
      chunk = [start, count].concat(chunk);

      start += size;
      ends -= count;

      ret = ret.concat(splice.apply(array, chunk));
    }
    return ret;
  }

  /**
    Replaces objects in an array with the passed objects.
  
    ```javascript
      var array = [1,2,3];
      Ember.EnumerableUtils.replace(array, 1, 2, [4, 5]); // [1, 4, 5]
  
      var array = [1,2,3];
      Ember.EnumerableUtils.replace(array, 1, 1, [4, 5]); // [1, 4, 5, 3]
  
      var array = [1,2,3];
      Ember.EnumerableUtils.replace(array, 10, 1, [4, 5]); // [1, 2, 3, 4, 5]
    ```
  
    @method replace
    @deprecated
    @param {Array} array The array the objects should be inserted into.
    @param {Number} idx Starting index in the array to replace. If *idx* >=
    length, then append to the end of the array.
    @param {Number} amt Number of elements that should be removed from the array,
    starting at *idx*
    @param {Array} objects An array of zero or more objects that should be
    inserted into the array at *idx*
  
    @return {Array} The modified array.
    @public
  */

  function replace(array, idx, amt, objects) {
    if (array.replace) {
      return array.replace(idx, amt, objects);
    } else {
      return _replace(array, idx, amt, objects);
    }
  }
});
enifed('ember-metal/run_loop', ['exports', 'ember-metal/core', 'ember-metal/debug', 'ember-metal/utils', 'ember-metal/property_events', 'backburner'], function (exports, _emberMetalCore, _emberMetalDebug, _emberMetalUtils, _emberMetalProperty_events, _backburner) {
  'use strict';

  exports.default = run;

  function onBegin(current) {
    run.currentRunLoop = current;
  }

  function onEnd(current, next) {
    run.currentRunLoop = next;
  }

  // ES6TODO: should Backburner become es6?
  var backburner = new _backburner.default(['sync', 'actions', 'destroy'], {
    GUID_KEY: _emberMetalUtils.GUID_KEY,
    sync: {
      before: _emberMetalProperty_events.beginPropertyChanges,
      after: _emberMetalProperty_events.endPropertyChanges
    },
    defaultQueue: 'actions',
    onBegin: onBegin,
    onEnd: onEnd,
    onErrorTarget: _emberMetalCore.default,
    onErrorMethod: 'onerror'
  });

  // ..........................................................
  // run - this is ideally the only public API the dev sees
  //

  /**
    Runs the passed target and method inside of a RunLoop, ensuring any
    deferred actions including bindings and views updates are flushed at the
    end.
  
    Normally you should not need to invoke this method yourself. However if
    you are implementing raw event handlers when interfacing with other
    libraries or plugins, you should probably wrap all of your code inside this
    call.
  
    ```javascript
    run(function() {
      // code to be executed within a RunLoop
    });
    ```
  
    @class run
    @namespace Ember
    @static
    @constructor
    @param {Object} [target] target of method to call
    @param {Function|String} method Method to invoke.
      May be a function or a string. If you pass a string
      then it will be looked up on the passed target.
    @param {Object} [args*] Any additional arguments you wish to pass to the method.
    @return {Object} return value from invoking the passed function.
    @public
  */

  function run() {
    return backburner.run.apply(backburner, arguments);
  }

  /**
    If no run-loop is present, it creates a new one. If a run loop is
    present it will queue itself to run on the existing run-loops action
    queue.
  
    Please note: This is not for normal usage, and should be used sparingly.
  
    If invoked when not within a run loop:
  
    ```javascript
    run.join(function() {
      // creates a new run-loop
    });
    ```
  
    Alternatively, if called within an existing run loop:
  
    ```javascript
    run(function() {
      // creates a new run-loop
      run.join(function() {
        // joins with the existing run-loop, and queues for invocation on
        // the existing run-loops action queue.
      });
    });
    ```
  
    @method join
    @namespace Ember
    @param {Object} [target] target of method to call
    @param {Function|String} method Method to invoke.
      May be a function or a string. If you pass a string
      then it will be looked up on the passed target.
    @param {Object} [args*] Any additional arguments you wish to pass to the method.
    @return {Object} Return value from invoking the passed function. Please note,
    when called within an existing loop, no return value is possible.
    @public
  */
  run.join = function () {
    return backburner.join.apply(backburner, arguments);
  };

  /**
    Allows you to specify which context to call the specified function in while
    adding the execution of that function to the Ember run loop. This ability
    makes this method a great way to asynchronously integrate third-party libraries
    into your Ember application.
  
    `run.bind` takes two main arguments, the desired context and the function to
    invoke in that context. Any additional arguments will be supplied as arguments
    to the function that is passed in.
  
    Let's use the creation of a TinyMCE component as an example. Currently,
    TinyMCE provides a setup configuration option we can use to do some processing
    after the TinyMCE instance is initialized but before it is actually rendered.
    We can use that setup option to do some additional setup for our component.
    The component itself could look something like the following:
  
    ```javascript
    App.RichTextEditorComponent = Ember.Component.extend({
      initializeTinyMCE: Ember.on('didInsertElement', function() {
        tinymce.init({
          selector: '#' + this.$().prop('id'),
          setup: Ember.run.bind(this, this.setupEditor)
        });
      }),
  
      setupEditor: function(editor) {
        this.set('editor', editor);
  
        editor.on('change', function() {
          console.log('content changed!');
        });
      }
    });
    ```
  
    In this example, we use Ember.run.bind to bind the setupEditor method to the
    context of the App.RichTextEditorComponent and to have the invocation of that
    method be safely handled and executed by the Ember run loop.
  
    @method bind
    @namespace Ember
    @param {Object} [target] target of method to call
    @param {Function|String} method Method to invoke.
      May be a function or a string. If you pass a string
      then it will be looked up on the passed target.
    @param {Object} [args*] Any additional arguments you wish to pass to the method.
    @return {Function} returns a new function that will always have a particular context
    @since 1.4.0
    @public
  */
  run.bind = function () {
    for (var _len = arguments.length, curried = Array(_len), _key = 0; _key < _len; _key++) {
      curried[_key] = arguments[_key];
    }

    return function () {
      for (var _len2 = arguments.length, args = Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
        args[_key2] = arguments[_key2];
      }

      return run.join.apply(run, curried.concat(args));
    };
  };

  run.backburner = backburner;
  run.currentRunLoop = null;
  run.queues = backburner.queueNames;

  /**
    Begins a new RunLoop. Any deferred actions invoked after the begin will
    be buffered until you invoke a matching call to `run.end()`. This is
    a lower-level way to use a RunLoop instead of using `run()`.
  
    ```javascript
    run.begin();
    // code to be executed within a RunLoop
    run.end();
    ```
  
    @method begin
    @return {void}
    @public
  */
  run.begin = function () {
    backburner.begin();
  };

  /**
    Ends a RunLoop. This must be called sometime after you call
    `run.begin()` to flush any deferred actions. This is a lower-level way
    to use a RunLoop instead of using `run()`.
  
    ```javascript
    run.begin();
    // code to be executed within a RunLoop
    run.end();
    ```
  
    @method end
    @return {void}
    @public
  */
  run.end = function () {
    backburner.end();
  };

  /**
    Array of named queues. This array determines the order in which queues
    are flushed at the end of the RunLoop. You can define your own queues by
    simply adding the queue name to this array. Normally you should not need
    to inspect or modify this property.
  
    @property queues
    @type Array
    @default ['sync', 'actions', 'destroy']
    @private
  */

  /**
    Adds the passed target/method and any optional arguments to the named
    queue to be executed at the end of the RunLoop. If you have not already
    started a RunLoop when calling this method one will be started for you
    automatically.
  
    At the end of a RunLoop, any methods scheduled in this way will be invoked.
    Methods will be invoked in an order matching the named queues defined in
    the `run.queues` property.
  
    ```javascript
    run.schedule('sync', this, function() {
      // this will be executed in the first RunLoop queue, when bindings are synced
      console.log('scheduled on sync queue');
    });
  
    run.schedule('actions', this, function() {
      // this will be executed in the 'actions' queue, after bindings have synced.
      console.log('scheduled on actions queue');
    });
  
    // Note the functions will be run in order based on the run queues order.
    // Output would be:
    //   scheduled on sync queue
    //   scheduled on actions queue
    ```
  
    @method schedule
    @param {String} queue The name of the queue to schedule against.
      Default queues are 'sync' and 'actions'
    @param {Object} [target] target object to use as the context when invoking a method.
    @param {String|Function} method The method to invoke. If you pass a string it
      will be resolved on the target object at the time the scheduled item is
      invoked allowing you to change the target function.
    @param {Object} [arguments*] Optional arguments to be passed to the queued method.
    @return {void}
    @public
  */
  run.schedule = function () /* queue, target, method */{
    checkAutoRun();
    backburner.schedule.apply(backburner, arguments);
  };

  // Used by global test teardown
  run.hasScheduledTimers = function () {
    return backburner.hasTimers();
  };

  // Used by global test teardown
  run.cancelTimers = function () {
    backburner.cancelTimers();
  };

  /**
    Immediately flushes any events scheduled in the 'sync' queue. Bindings
    use this queue so this method is a useful way to immediately force all
    bindings in the application to sync.
  
    You should call this method anytime you need any changed state to propagate
    throughout the app immediately without repainting the UI (which happens
    in the later 'render' queue added by the `ember-views` package).
  
    ```javascript
    run.sync();
    ```
  
    @method sync
    @return {void}
    @private
  */
  run.sync = function () {
    if (backburner.currentInstance) {
      backburner.currentInstance.queues.sync.flush();
    }
  };

  /**
    Invokes the passed target/method and optional arguments after a specified
    period of time. The last parameter of this method must always be a number
    of milliseconds.
  
    You should use this method whenever you need to run some action after a
    period of time instead of using `setTimeout()`. This method will ensure that
    items that expire during the same script execution cycle all execute
    together, which is often more efficient than using a real setTimeout.
  
    ```javascript
    run.later(myContext, function() {
      // code here will execute within a RunLoop in about 500ms with this == myContext
    }, 500);
    ```
  
    @method later
    @param {Object} [target] target of method to invoke
    @param {Function|String} method The method to invoke.
      If you pass a string it will be resolved on the
      target at the time the method is invoked.
    @param {Object} [args*] Optional arguments to pass to the timeout.
    @param {Number} wait Number of milliseconds to wait.
    @return {*} Timer information for use in cancelling, see `run.cancel`.
    @public
  */
  run.later = function () /*target, method*/{
    return backburner.later.apply(backburner, arguments);
  };

  /**
    Schedule a function to run one time during the current RunLoop. This is equivalent
    to calling `scheduleOnce` with the "actions" queue.
  
    @method once
    @param {Object} [target] The target of the method to invoke.
    @param {Function|String} method The method to invoke.
      If you pass a string it will be resolved on the
      target at the time the method is invoked.
    @param {Object} [args*] Optional arguments to pass to the timeout.
    @return {Object} Timer information for use in cancelling, see `run.cancel`.
    @public
  */
  run.once = function () {
    checkAutoRun();

    for (var _len3 = arguments.length, args = Array(_len3), _key3 = 0; _key3 < _len3; _key3++) {
      args[_key3] = arguments[_key3];
    }

    args.unshift('actions');
    return backburner.scheduleOnce.apply(backburner, args);
  };

  /**
    Schedules a function to run one time in a given queue of the current RunLoop.
    Calling this method with the same queue/target/method combination will have
    no effect (past the initial call).
  
    Note that although you can pass optional arguments these will not be
    considered when looking for duplicates. New arguments will replace previous
    calls.
  
    ```javascript
    function sayHi() {
      console.log('hi');
    }
  
    run(function() {
      run.scheduleOnce('afterRender', myContext, sayHi);
      run.scheduleOnce('afterRender', myContext, sayHi);
      // sayHi will only be executed once, in the afterRender queue of the RunLoop
    });
    ```
  
    Also note that passing an anonymous function to `run.scheduleOnce` will
    not prevent additional calls with an identical anonymous function from
    scheduling the items multiple times, e.g.:
  
    ```javascript
    function scheduleIt() {
      run.scheduleOnce('actions', myContext, function() {
        console.log('Closure');
      });
    }
  
    scheduleIt();
    scheduleIt();
  
    // "Closure" will print twice, even though we're using `run.scheduleOnce`,
    // because the function we pass to it is anonymous and won't match the
    // previously scheduled operation.
    ```
  
    Available queues, and their order, can be found at `run.queues`
  
    @method scheduleOnce
    @param {String} [queue] The name of the queue to schedule against. Default queues are 'sync' and 'actions'.
    @param {Object} [target] The target of the method to invoke.
    @param {Function|String} method The method to invoke.
      If you pass a string it will be resolved on the
      target at the time the method is invoked.
    @param {Object} [args*] Optional arguments to pass to the timeout.
    @return {Object} Timer information for use in cancelling, see `run.cancel`.
    @public
  */
  run.scheduleOnce = function () /*queue, target, method*/{
    checkAutoRun();
    return backburner.scheduleOnce.apply(backburner, arguments);
  };

  /**
    Schedules an item to run from within a separate run loop, after
    control has been returned to the system. This is equivalent to calling
    `run.later` with a wait time of 1ms.
  
    ```javascript
    run.next(myContext, function() {
      // code to be executed in the next run loop,
      // which will be scheduled after the current one
    });
    ```
  
    Multiple operations scheduled with `run.next` will coalesce
    into the same later run loop, along with any other operations
    scheduled by `run.later` that expire right around the same
    time that `run.next` operations will fire.
  
    Note that there are often alternatives to using `run.next`.
    For instance, if you'd like to schedule an operation to happen
    after all DOM element operations have completed within the current
    run loop, you can make use of the `afterRender` run loop queue (added
    by the `ember-views` package, along with the preceding `render` queue
    where all the DOM element operations happen).
  
    Example:
  
    ```javascript
    export default Ember.Component.extend({
      didInsertElement() {
        this._super(...arguments);
        run.scheduleOnce('afterRender', this, 'processChildElements');
      },
  
      processChildElements() {
        // ... do something with component's child component
        // elements after they've finished rendering, which
        // can't be done within this component's
        // `didInsertElement` hook because that gets run
        // before the child elements have been added to the DOM.
      }
    });
    ```
  
    One benefit of the above approach compared to using `run.next` is
    that you will be able to perform DOM/CSS operations before unprocessed
    elements are rendered to the screen, which may prevent flickering or
    other artifacts caused by delaying processing until after rendering.
  
    The other major benefit to the above approach is that `run.next`
    introduces an element of non-determinism, which can make things much
    harder to test, due to its reliance on `setTimeout`; it's much harder
    to guarantee the order of scheduled operations when they are scheduled
    outside of the current run loop, i.e. with `run.next`.
  
    @method next
    @param {Object} [target] target of method to invoke
    @param {Function|String} method The method to invoke.
      If you pass a string it will be resolved on the
      target at the time the method is invoked.
    @param {Object} [args*] Optional arguments to pass to the timeout.
    @return {Object} Timer information for use in cancelling, see `run.cancel`.
    @public
  */
  run.next = function () {
    for (var _len4 = arguments.length, args = Array(_len4), _key4 = 0; _key4 < _len4; _key4++) {
      args[_key4] = arguments[_key4];
    }

    args.push(1);
    return backburner.later.apply(backburner, args);
  };

  /**
    Cancels a scheduled item. Must be a value returned by `run.later()`,
    `run.once()`, `run.scheduleOnce()`, `run.next()`, `run.debounce()`, or
    `run.throttle()`.
  
    ```javascript
    var runNext = run.next(myContext, function() {
      // will not be executed
    });
  
    run.cancel(runNext);
  
    var runLater = run.later(myContext, function() {
      // will not be executed
    }, 500);
  
    run.cancel(runLater);
  
    var runScheduleOnce = run.scheduleOnce('afterRender', myContext, function() {
      // will not be executed
    });
  
    run.cancel(runScheduleOnce);
  
    var runOnce = run.once(myContext, function() {
      // will not be executed
    });
  
    run.cancel(runOnce);
  
    var throttle = run.throttle(myContext, function() {
      // will not be executed
    }, 1, false);
  
    run.cancel(throttle);
  
    var debounce = run.debounce(myContext, function() {
      // will not be executed
    }, 1);
  
    run.cancel(debounce);
  
    var debounceImmediate = run.debounce(myContext, function() {
      // will be executed since we passed in true (immediate)
    }, 100, true);
  
    // the 100ms delay until this method can be called again will be cancelled
    run.cancel(debounceImmediate);
    ```
  
    @method cancel
    @param {Object} timer Timer object to cancel
    @return {Boolean} true if cancelled or false/undefined if it wasn't found
    @public
  */
  run.cancel = function (timer) {
    return backburner.cancel(timer);
  };

  /**
    Delay calling the target method until the debounce period has elapsed
    with no additional debounce calls. If `debounce` is called again before
    the specified time has elapsed, the timer is reset and the entire period
    must pass again before the target method is called.
  
    This method should be used when an event may be called multiple times
    but the action should only be called once when the event is done firing.
    A common example is for scroll events where you only want updates to
    happen once scrolling has ceased.
  
    ```javascript
    function whoRan() {
      console.log(this.name + ' ran.');
    }
  
    var myContext = { name: 'debounce' };
  
    run.debounce(myContext, whoRan, 150);
  
    // less than 150ms passes
    run.debounce(myContext, whoRan, 150);
  
    // 150ms passes
    // whoRan is invoked with context myContext
    // console logs 'debounce ran.' one time.
    ```
  
    Immediate allows you to run the function immediately, but debounce
    other calls for this function until the wait time has elapsed. If
    `debounce` is called again before the specified time has elapsed,
    the timer is reset and the entire period must pass again before
    the method can be called again.
  
    ```javascript
    function whoRan() {
      console.log(this.name + ' ran.');
    }
  
    var myContext = { name: 'debounce' };
  
    run.debounce(myContext, whoRan, 150, true);
  
    // console logs 'debounce ran.' one time immediately.
    // 100ms passes
    run.debounce(myContext, whoRan, 150, true);
  
    // 150ms passes and nothing else is logged to the console and
    // the debouncee is no longer being watched
    run.debounce(myContext, whoRan, 150, true);
  
    // console logs 'debounce ran.' one time immediately.
    // 150ms passes and nothing else is logged to the console and
    // the debouncee is no longer being watched
  
    ```
  
    @method debounce
    @param {Object} [target] target of method to invoke
    @param {Function|String} method The method to invoke.
      May be a function or a string. If you pass a string
      then it will be looked up on the passed target.
    @param {Object} [args*] Optional arguments to pass to the timeout.
    @param {Number} wait Number of milliseconds to wait.
    @param {Boolean} immediate Trigger the function on the leading instead
      of the trailing edge of the wait interval. Defaults to false.
    @return {Array} Timer information for use in cancelling, see `run.cancel`.
    @public
  */
  run.debounce = function () {
    return backburner.debounce.apply(backburner, arguments);
  };

  /**
    Ensure that the target method is never called more frequently than
    the specified spacing period. The target method is called immediately.
  
    ```javascript
    function whoRan() {
      console.log(this.name + ' ran.');
    }
  
    var myContext = { name: 'throttle' };
  
    run.throttle(myContext, whoRan, 150);
    // whoRan is invoked with context myContext
    // console logs 'throttle ran.'
  
    // 50ms passes
    run.throttle(myContext, whoRan, 150);
  
    // 50ms passes
    run.throttle(myContext, whoRan, 150);
  
    // 150ms passes
    run.throttle(myContext, whoRan, 150);
    // whoRan is invoked with context myContext
    // console logs 'throttle ran.'
    ```
  
    @method throttle
    @param {Object} [target] target of method to invoke
    @param {Function|String} method The method to invoke.
      May be a function or a string. If you pass a string
      then it will be looked up on the passed target.
    @param {Object} [args*] Optional arguments to pass to the timeout.
    @param {Number} spacing Number of milliseconds to space out requests.
    @param {Boolean} immediate Trigger the function on the leading instead
      of the trailing edge of the wait interval. Defaults to true.
    @return {Array} Timer information for use in cancelling, see `run.cancel`.
    @public
  */
  run.throttle = function () {
    return backburner.throttle.apply(backburner, arguments);
  };

  // Make sure it's not an autorun during testing
  function checkAutoRun() {
    if (!run.currentRunLoop) {
      _emberMetalDebug.assert('You have turned on testing mode, which disabled the run-loop\'s autorun. ' + 'You will need to wrap any code with asynchronous side-effects in a run', !_emberMetalCore.default.testing);
    }
  }

  /**
    Add a new named queue after the specified queue.
  
    The queue to add will only be added once.
  
    @method _addQueue
    @param {String} name the name of the queue to add.
    @param {String} after the name of the queue to add after.
    @private
  */
  run._addQueue = function (name, after) {
    if (run.queues.indexOf(name) === -1) {
      run.queues.splice(run.queues.indexOf(after) + 1, 0, name);
    }
  };
});
enifed('ember-metal/set_properties', ['exports', 'ember-metal/property_events', 'ember-metal/property_set'], function (exports, _emberMetalProperty_events, _emberMetalProperty_set) {
  'use strict';

  exports.default = setProperties;

  /**
    Set a list of properties on an object. These properties are set inside
    a single `beginPropertyChanges` and `endPropertyChanges` batch, so
    observers will be buffered.
  
    ```javascript
    var anObject = Ember.Object.create();
  
    anObject.setProperties({
      firstName: 'Stanley',
      lastName: 'Stuart',
      age: 21
    });
    ```
  
    @method setProperties
    @param obj
    @param {Object} properties
    @return properties
    @public
  */

  function setProperties(obj, properties) {
    if (!properties || typeof properties !== 'object') {
      return properties;
    }
    _emberMetalProperty_events.changeProperties(function () {
      var props = Object.keys(properties);
      var propertyName;

      for (var i = 0, l = props.length; i < l; i++) {
        propertyName = props[i];

        _emberMetalProperty_set.set(obj, propertyName, properties[propertyName]);
      }
    });
    return properties;
  }
});
enifed('ember-metal/streams/dependency', ['exports', 'ember-metal/debug', 'ember-metal/assign', 'ember-metal/streams/utils'], function (exports, _emberMetalDebug, _emberMetalAssign, _emberMetalStreamsUtils) {
  'use strict';

  /**
    @module ember-metal
  */

  /**
    @private
    @class Dependency
    @namespace Ember.streams
    @constructor
  */
  function Dependency(depender, dependee) {
    _emberMetalDebug.assert('Dependency error: Depender must be a stream', _emberMetalStreamsUtils.isStream(depender));

    this.next = null;
    this.prev = null;
    this.depender = depender;
    this.dependee = dependee;
    this.unsubscription = null;
  }

  _emberMetalAssign.default(Dependency.prototype, {
    subscribe: function () {
      _emberMetalDebug.assert('Dependency error: Dependency tried to subscribe while already subscribed', !this.unsubscription);

      this.unsubscription = _emberMetalStreamsUtils.subscribe(this.dependee, this.depender.notify, this.depender);
    },

    unsubscribe: function () {
      if (this.unsubscription) {
        this.unsubscription();
        this.unsubscription = null;
      }
    },

    replace: function (dependee) {
      if (this.dependee !== dependee) {
        this.dependee = dependee;

        if (this.unsubscription) {
          this.unsubscribe();
          this.subscribe();
        }
        return true;
      }
      return false;
    },

    getValue: function () {
      return _emberMetalStreamsUtils.read(this.dependee);
    },

    setValue: function (value) {
      return _emberMetalStreamsUtils.setValue(this.dependee, value);
    }

    // destroy() {
    //   var next = this.next;
    //   var prev = this.prev;

    //   if (prev) {
    //     prev.next = next;
    //   } else {
    //     this.depender.dependencyHead = next;
    //   }

    //   if (next) {
    //     next.prev = prev;
    //   } else {
    //     this.depender.dependencyTail = prev;
    //   }

    //   this.unsubscribe();
    // }
  });

  exports.default = Dependency;
});
enifed('ember-metal/streams/key-stream', ['exports', 'ember-metal/debug', 'ember-metal/property_get', 'ember-metal/property_set', 'ember-metal/observer', 'ember-metal/streams/stream', 'ember-metal/streams/utils'], function (exports, _emberMetalDebug, _emberMetalProperty_get, _emberMetalProperty_set, _emberMetalObserver, _emberMetalStreamsStream, _emberMetalStreamsUtils) {
  'use strict';

  function labelFor(source, key) {
    return source.label ? source.label + '.' + key : key;
  }

  exports.default = _emberMetalStreamsStream.default.extend({
    init: function (source, key) {
      _emberMetalDebug.assert('KeyStream error: source must be a stream', _emberMetalStreamsUtils.isStream(source)); // TODO: This isn't necessary.
      _emberMetalDebug.assert('KeyStream error: key must be a non-empty string', typeof key === 'string' && key.length > 0);
      _emberMetalDebug.assert('KeyStream error: key must not have a \'.\'', key.indexOf('.') === -1);

      var label = labelFor(source, key);

      this.path = label;
      this.observedObject = null;
      this.key = key;
      this.sourceDep = this.addMutableDependency(source);
      this.label = label;
    },

    compute: function () {
      var object = this.sourceDep.getValue();
      var type = typeof object;

      if (!object || type === 'boolean') {
        return;
      }

      if (type === 'object') {
        return _emberMetalProperty_get.get(object, this.key);
      }

      return object[this.key];
    },

    setValue: function (value) {
      var object = this.sourceDep.getValue();
      if (object) {
        _emberMetalProperty_set.set(object, this.key, value);
      }
    },

    setSource: function (source) {
      this.sourceDep.replace(source);
      this.notify();
    },

    _super$revalidate: _emberMetalStreamsStream.default.prototype.revalidate,

    revalidate: function (value) {
      this._super$revalidate(value);

      var object = this.sourceDep.getValue();
      if (object !== this.observedObject) {
        this._clearObservedObject();

        if (object && typeof object === 'object') {
          _emberMetalObserver.addObserver(object, this.key, this, this.notify);
          this.observedObject = object;
        }
      }
    },

    _super$deactivate: _emberMetalStreamsStream.default.prototype.deactivate,

    _clearObservedObject: function () {
      if (this.observedObject) {
        _emberMetalObserver.removeObserver(this.observedObject, this.key, this, this.notify);
        this.observedObject = null;
      }
    },

    deactivate: function () {
      this._super$deactivate();
      this._clearObservedObject();
    }
  });
});
enifed('ember-metal/streams/proxy-stream', ['exports', 'ember-runtime/system/object', 'ember-metal/streams/stream'], function (exports, _emberRuntimeSystemObject, _emberMetalStreamsStream) {
  'use strict';

  var ProxyStream = _emberMetalStreamsStream.default.extend({
    init: function (source, label) {
      this.label = label;
      this.sourceDep = this.addMutableDependency(source);
    },

    compute: function () {
      return this.sourceDep.getValue();
    },

    setValue: function (value) {
      this.sourceDep.setValue(value);
    },

    setSource: function (source) {
      var didChange = this.sourceDep.replace(source);
      if (didChange || !(source instanceof _emberRuntimeSystemObject.default)) {
        // If the source changed, we must notify. If the source is not
        // an Ember.Object, we must also notify, because it could have
        // interior mutability that is otherwise not being observed.
        this.notify();
      }
    }
  });

  ProxyStream.extend = _emberMetalStreamsStream.default.extend;

  exports.default = ProxyStream;
});
enifed('ember-metal/streams/stream', ['exports', 'ember-metal/assign', 'ember-metal/debug', 'ember-metal/path_cache', 'ember-metal/observer', 'ember-metal/streams/utils', 'ember-metal/empty_object', 'ember-metal/streams/subscriber', 'ember-metal/streams/dependency', 'ember-metal/utils', 'require', 'ember-metal/symbol'], function (exports, _emberMetalAssign, _emberMetalDebug, _emberMetalPath_cache, _emberMetalObserver, _emberMetalStreamsUtils, _emberMetalEmpty_object, _emberMetalStreamsSubscriber, _emberMetalStreamsDependency, _emberMetalUtils, _require, _emberMetalSymbol) {
  'use strict';

  exports.wrap = wrap;
  var IS_STREAM = _emberMetalSymbol.default('IS_STREAM');

  exports.IS_STREAM = IS_STREAM;
  /**
    @module ember-metal
  */

  /**
    @private
    @class Stream
    @namespace Ember.stream
    @constructor
  */
  function BasicStream(label) {
    this._init(label);
  }

  var KeyStream;
  var ProxyMixin;

  BasicStream.prototype = {
    _init: function (label) {
      this[IS_STREAM] = true;
      this.label = makeLabel(label);
      this.isActive = false;
      this.isDirty = true;
      this.isDestroyed = false;
      this.cache = undefined;
      this.children = undefined;
      this.subscriberHead = null;
      this.subscriberTail = null;
      this.dependencyHead = null;
      this.dependencyTail = null;
      this.observedProxy = null;
      this.__ember_meta__ = null;
      this[_emberMetalUtils.GUID_KEY] = null;
    },

    _makeChildStream: function (key) {
      KeyStream = KeyStream || _require.default('ember-metal/streams/key-stream').default;
      return new KeyStream(this, key);
    },

    removeChild: function (key) {
      delete this.children[key];
    },

    getKey: function (key) {
      if (this.children === undefined) {
        this.children = new _emberMetalEmpty_object.default();
      }

      var keyStream = this.children[key];

      if (keyStream === undefined) {
        keyStream = this._makeChildStream(key);
        this.children[key] = keyStream;
      }

      return keyStream;
    },

    get: function (path) {
      var firstKey = _emberMetalPath_cache.getFirstKey(path);
      var tailPath = _emberMetalPath_cache.getTailPath(path);

      if (this.children === undefined) {
        this.children = new _emberMetalEmpty_object.default();
      }

      var keyStream = this.children[firstKey];

      if (keyStream === undefined) {
        keyStream = this._makeChildStream(firstKey, path);
        this.children[firstKey] = keyStream;
      }

      if (tailPath === undefined) {
        return keyStream;
      } else {
        return keyStream.get(tailPath);
      }
    },

    value: function () {
      // TODO: Ensure value is never called on a destroyed stream
      // so that we can uncomment this assertion.
      //
      // assert("Stream error: value was called after the stream was destroyed", !this.isDestroyed);

      // TODO: Remove this block. This will require ensuring we are
      // not treating streams as "volatile" anywhere.
      if (!this.isActive) {
        this.isDirty = true;
      }

      var willRevalidate = false;

      if (!this.isActive && this.subscriberHead) {
        this.activate();
        willRevalidate = true;
      }

      if (this.isDirty) {
        if (this.isActive) {
          willRevalidate = true;
        }

        this.cache = this.compute();
        this.isDirty = false;
      }

      if (willRevalidate) {
        this.revalidate(this.cache);
      }

      return this.cache;
    },

    addMutableDependency: function (object) {
      var dependency = new _emberMetalStreamsDependency.default(this, object);

      if (this.isActive) {
        dependency.subscribe();
      }

      if (this.dependencyHead === null) {
        this.dependencyHead = this.dependencyTail = dependency;
      } else {
        var tail = this.dependencyTail;
        tail.next = dependency;
        dependency.prev = tail;
        this.dependencyTail = dependency;
      }

      return dependency;
    },

    addDependency: function (object) {
      if (_emberMetalStreamsUtils.isStream(object)) {
        this.addMutableDependency(object);
      }
    },

    subscribeDependencies: function () {
      var dependency = this.dependencyHead;
      while (dependency) {
        var next = dependency.next;
        dependency.subscribe();
        dependency = next;
      }
    },

    unsubscribeDependencies: function () {
      var dependency = this.dependencyHead;
      while (dependency) {
        var next = dependency.next;
        dependency.unsubscribe();
        dependency = next;
      }
    },

    maybeDeactivate: function () {
      if (!this.subscriberHead && this.isActive) {
        this.isActive = false;
        this.unsubscribeDependencies();
        this.deactivate();
      }
    },

    activate: function () {
      this.isActive = true;
      this.subscribeDependencies();
    },

    revalidate: function (value) {
      if (value !== this.observedProxy) {
        this._clearObservedProxy();

        ProxyMixin = ProxyMixin || _require.default('ember-runtime/mixins/-proxy').default;

        if (ProxyMixin.detect(value)) {
          _emberMetalObserver.addObserver(value, 'content', this, this.notify);
          this.observedProxy = value;
        }
      }
    },

    _clearObservedProxy: function () {
      if (this.observedProxy) {
        _emberMetalObserver.removeObserver(this.observedProxy, 'content', this, this.notify);
        this.observedProxy = null;
      }
    },

    deactivate: function () {
      this._clearObservedProxy();
    },

    compute: function () {
      throw new Error('Stream error: compute not implemented');
    },

    setValue: function () {
      throw new Error('Stream error: setValue not implemented');
    },

    notify: function () {
      this.notifyExcept();
    },

    notifyExcept: function (callbackToSkip, contextToSkip) {
      if (!this.isDirty) {
        this.isDirty = true;
        this.notifySubscribers(callbackToSkip, contextToSkip);
      }
    },

    subscribe: function (callback, context) {
      _emberMetalDebug.assert('You tried to subscribe to a stream but the callback provided was not a function.', typeof callback === 'function');

      var subscriber = new _emberMetalStreamsSubscriber.default(callback, context, this);
      if (this.subscriberHead === null) {
        this.subscriberHead = this.subscriberTail = subscriber;
      } else {
        var tail = this.subscriberTail;
        tail.next = subscriber;
        subscriber.prev = tail;
        this.subscriberTail = subscriber;
      }

      var stream = this;
      return function (prune) {
        subscriber.removeFrom(stream);
        if (prune) {
          stream.prune();
        }
      };
    },

    prune: function () {
      if (this.subscriberHead === null) {
        this.destroy(true);
      }
    },

    unsubscribe: function (callback, context) {
      var subscriber = this.subscriberHead;

      while (subscriber) {
        var next = subscriber.next;
        if (subscriber.callback === callback && subscriber.context === context) {
          subscriber.removeFrom(this);
        }
        subscriber = next;
      }
    },

    notifySubscribers: function (callbackToSkip, contextToSkip) {
      var subscriber = this.subscriberHead;

      while (subscriber) {
        var next = subscriber.next;

        var callback = subscriber.callback;
        var context = subscriber.context;

        subscriber = next;

        if (callback === callbackToSkip && context === contextToSkip) {
          continue;
        }

        if (context === undefined) {
          callback(this);
        } else {
          callback.call(context, this);
        }
      }
    },

    destroy: function (prune) {
      if (!this.isDestroyed) {
        this.isDestroyed = true;

        this.subscriberHead = this.subscriberTail = null;
        this.maybeDeactivate();

        var dependencies = this.dependencies;

        if (dependencies) {
          for (var i = 0, l = dependencies.length; i < l; i++) {
            dependencies[i](prune);
          }
        }

        return true;
      }
    }
  };

  BasicStream.extend = function (object) {
    var Child = function () {
      this._init();
      this.init.apply(this, arguments);

      _emberMetalDebug.debugSeal(this);
    };

    Child.prototype = Object.create(this.prototype);

    _emberMetalAssign.default(Child.prototype, object);
    Child.extend = BasicStream.extend;
    return Child;
  };

  var Stream = BasicStream.extend({
    init: function (fn, label) {
      this._compute = fn;
      this.label = label;
    },

    compute: function () {
      return this._compute();
    }
  });

  function wrap(value, Kind, param) {
    if (_emberMetalStreamsUtils.isStream(value)) {
      return value;
    } else {
      return new Kind(value, param);
    }
  }

  function makeLabel(label) {
    if (label === undefined) {
      return '(no label)';
    } else {
      return label;
    }
  }

  exports.default = BasicStream;
  exports.Stream = Stream;
});
enifed('ember-metal/streams/subscriber', ['exports', 'ember-metal/assign'], function (exports, _emberMetalAssign) {
  'use strict';

  /**
    @module ember-metal
  */

  /**
    @private
    @class Subscriber
    @namespace Ember.streams
    @constructor
  */
  function Subscriber(callback, context) {
    this.next = null;
    this.prev = null;
    this.callback = callback;
    this.context = context;
  }

  _emberMetalAssign.default(Subscriber.prototype, {
    removeFrom: function (stream) {
      var next = this.next;
      var prev = this.prev;

      if (prev) {
        prev.next = next;
      } else {
        stream.subscriberHead = next;
      }

      if (next) {
        next.prev = prev;
      } else {
        stream.subscriberTail = prev;
      }

      stream.maybeDeactivate();
    }
  });

  exports.default = Subscriber;
});
enifed('ember-metal/streams/utils', ['exports', 'ember-metal/debug', 'ember-metal/streams/stream'], function (exports, _emberMetalDebug, _emberMetalStreamsStream) {
  'use strict';

  exports.isStream = isStream;
  exports.subscribe = subscribe;
  exports.unsubscribe = unsubscribe;
  exports.read = read;
  exports.readArray = readArray;
  exports.readHash = readHash;
  exports.scanArray = scanArray;
  exports.scanHash = scanHash;
  exports.concat = concat;
  exports.labelsFor = labelsFor;
  exports.labelsForObject = labelsForObject;
  exports.labelFor = labelFor;
  exports.or = or;
  exports.addDependency = addDependency;
  exports.zip = zip;
  exports.zipHash = zipHash;
  exports.chain = chain;
  exports.setValue = setValue;

  /*
   Check whether an object is a stream or not.
  
   @private
   @for Ember.stream
   @function isStream
   @param {Object|Stream} object Object to check whether it is a stream.
   @return {Boolean} `true` if the object is a stream, `false` otherwise.
  */

  function isStream(object) {
    return object && object[_emberMetalStreamsStream.IS_STREAM];
  }

  /*
   A method of subscribing to a stream which is safe for use with a non-stream
   object. If a non-stream object is passed, the function does nothing.
  
   @public
   @for Ember.stream
   @function subscribe
   @param {Object|Stream} object Object or stream to potentially subscribe to.
   @param {Function} callback Function to run when stream value changes.
   @param {Object} [context] the callback will be executed with this context if it
                             is provided.
   */

  function subscribe(object, callback, context) {
    if (object && object[_emberMetalStreamsStream.IS_STREAM]) {
      return object.subscribe(callback, context);
    }
  }

  /*
   A method of unsubscribing from a stream which is safe for use with a non-stream
   object. If a non-stream object is passed, the function does nothing.
  
   @private
   @for Ember.stream
   @function unsubscribe
   @param {Object|Stream} object Object or stream to potentially unsubscribe from.
   @param {Function} callback Function originally passed to `subscribe()`.
   @param {Object} [context] Object originally passed to `subscribe()`.
   */

  function unsubscribe(object, callback, context) {
    if (object && object[_emberMetalStreamsStream.IS_STREAM]) {
      object.unsubscribe(callback, context);
    }
  }

  /*
   Retrieve the value of a stream, or in the case where a non-stream object is passed,
   return the object itself.
  
   @private
   @for Ember.stream
   @function read
   @param {Object|Stream} object Object to return the value of.
   @return The stream's current value, or the non-stream object itself.
   */

  function read(object) {
    if (object && object[_emberMetalStreamsStream.IS_STREAM]) {
      return object.value();
    } else {
      return object;
    }
  }

  /*
   Map an array, replacing any streams with their values.
  
   @private
   @for Ember.stream
   @function readArray
   @param {Array} array The array to read values from
   @return {Array} A new array of the same length with the values of non-stream
                   objects mapped from their original positions untouched, and
                   the values of stream objects retaining their original position
                   and replaced with the stream's current value.
   */

  function readArray(array) {
    var length = array.length;
    var ret = new Array(length);
    for (var i = 0; i < length; i++) {
      ret[i] = read(array[i]);
    }
    return ret;
  }

  /*
   Map a hash, replacing any stream property values with the current value of that
   stream.
  
   @private
   @for Ember.stream
   @function readHash
   @param {Object} object The hash to read keys and values from.
   @return {Object} A new object with the same keys as the passed object. The
                    property values in the new object are the original values in
                    the case of non-stream objects, and the streams' current
                    values in the case of stream objects.
   */

  function readHash(object) {
    var ret = {};
    for (var key in object) {
      ret[key] = read(object[key]);
    }
    return ret;
  }

  /*
   Check whether an array contains any stream values.
  
   @private
   @for Ember.stream
   @function scanArray
   @param {Array} array Array given to a handlebars helper.
   @return {Boolean} `true` if the array contains a stream/bound value, `false`
                     otherwise.
  */

  function scanArray(array) {
    var length = array.length;
    var containsStream = false;

    for (var i = 0; i < length; i++) {
      if (isStream(array[i])) {
        containsStream = true;
        break;
      }
    }

    return containsStream;
  }

  /*
   Check whether a hash has any stream property values.
  
   @private
   @for Ember.stream
   @function scanHash
   @param {Object} hash "hash" argument given to a handlebars helper.
   @return {Boolean} `true` if the object contains a stream/bound value, `false`
                     otherwise.
   */

  function scanHash(hash) {
    var containsStream = false;

    for (var prop in hash) {
      if (isStream(hash[prop])) {
        containsStream = true;
        break;
      }
    }

    return containsStream;
  }

  var ConcatStream = _emberMetalStreamsStream.default.extend({
    init: function (array, separator) {
      this.array = array;
      this.separator = separator;

      // Used by angle bracket components to detect an attribute was provided
      // as a string literal.
      this.isConcat = true;
    },

    label: function () {
      var labels = labelsFor(this.array);
      return 'concat([' + labels.join(', ') + ']; separator=' + inspect(this.separator) + ')';
    },

    compute: function () {
      return concat(readArray(this.array), this.separator);
    }
  });

  /*
   Join an array, with any streams replaced by their current values.
  
   @private
   @for Ember.stream
   @function concat
   @param {Array} array An array containing zero or more stream objects and
                        zero or more non-stream objects.
   @param {String} separator String to be used to join array elements.
   @return {String} String with array elements concatenated and joined by the
                    provided separator, and any stream array members having been
                    replaced by the current value of the stream.
   */

  function concat(array, separator) {
    // TODO: Create subclass ConcatStream < Stream. Defer
    // subscribing to streams until the value() is called.
    var hasStream = scanArray(array);
    if (hasStream) {
      var stream = new ConcatStream(array, separator);

      for (var i = 0, l = array.length; i < l; i++) {
        addDependency(stream, array[i]);
      }

      return stream;
    } else {
      return array.join(separator);
    }
  }

  function labelsFor(streams) {
    var labels = [];

    for (var i = 0, l = streams.length; i < l; i++) {
      var stream = streams[i];
      labels.push(labelFor(stream));
    }

    return labels;
  }

  function labelsForObject(streams) {
    var labels = [];

    for (var prop in streams) {
      labels.push(prop + ': ' + inspect(streams[prop]));
    }

    return labels.length ? '{ ' + labels.join(', ') + ' }' : '{}';
  }

  function labelFor(maybeStream) {
    if (isStream(maybeStream)) {
      var stream = maybeStream;
      return typeof stream.label === 'function' ? stream.label() : stream.label;
    } else {
      return inspect(maybeStream);
    }
  }

  function inspect(value) {
    switch (typeof value) {
      case 'string':
        return '"' + value + '"';
      case 'object':
        return '{ ... }';
      case 'function':
        return 'function() { ... }';
      default:
        return String(value);
    }
  }

  function or(first, second) {
    var stream = new _emberMetalStreamsStream.Stream(function () {
      return first.value() || second.value();
    }, function () {
      return labelFor(first) + ' || ' + labelFor(second);
    });

    stream.addDependency(first);
    stream.addDependency(second);

    return stream;
  }

  function addDependency(stream, dependency) {
    _emberMetalDebug.assert('Cannot add a stream as a dependency to a non-stream', isStream(stream) || !isStream(dependency));
    if (isStream(stream)) {
      stream.addDependency(dependency);
    }
  }

  function zip(streams, callback, label) {
    _emberMetalDebug.assert('Must call zip with a label', !!label);

    var stream = new _emberMetalStreamsStream.Stream(function () {
      var array = readArray(streams);
      return callback ? callback(array) : array;
    }, function () {
      return label + '(' + labelsFor(streams) + ')';
    });

    for (var i = 0, l = streams.length; i < l; i++) {
      stream.addDependency(streams[i]);
    }

    return stream;
  }

  function zipHash(object, callback, label) {
    _emberMetalDebug.assert('Must call zipHash with a label', !!label);

    var stream = new _emberMetalStreamsStream.Stream(function () {
      var hash = readHash(object);
      return callback ? callback(hash) : hash;
    }, function () {
      return label + '(' + labelsForObject(object) + ')';
    });

    for (var prop in object) {
      stream.addDependency(object[prop]);
    }

    return stream;
  }

  /**
   Generate a new stream by providing a source stream and a function that can
   be used to transform the stream's value. In the case of a non-stream object,
   returns the result of the function.
  
   The value to transform would typically be available to the function you pass
   to `chain()` via scope. For example:
  
   ```javascript
       var source = ...;  // stream returning a number
                              // or a numeric (non-stream) object
       var result = chain(source, function() {
         var currentValue = read(source);
         return currentValue + 1;
       });
   ```
  
   In the example, result is a stream if source is a stream, or a number of
   source was numeric.
  
   @private
   @for Ember.stream
   @function chain
   @param {Object|Stream} value A stream or non-stream object.
   @param {Function} fn Function to be run when the stream value changes, or to
                        be run once in the case of a non-stream object.
   @return {Object|Stream} In the case of a stream `value` parameter, a new
                           stream that will be updated with the return value of
                           the provided function `fn`. In the case of a
                           non-stream object, the return value of the provided
                           function `fn`.
   */

  function chain(value, fn, label) {
    _emberMetalDebug.assert('Must call chain with a label', !!label);
    if (isStream(value)) {
      var stream = new _emberMetalStreamsStream.Stream(fn, function () {
        return label + '(' + labelFor(value) + ')';
      });
      stream.addDependency(value);
      return stream;
    } else {
      return fn();
    }
  }

  function setValue(object, value) {
    if (object && object[_emberMetalStreamsStream.IS_STREAM]) {
      object.setValue(value);
    }
  }
});
enifed('ember-metal/symbol', ['exports', 'ember-metal/utils'], function (exports, _emberMetalUtils) {
  'use strict';

  exports.default = symbol;

  function symbol(debugName) {
    // TODO: Investigate using platform symbols, but we do not
    // want to require non-enumerability for this API, which
    // would introduce a large cost.

    return _emberMetalUtils.intern(debugName + ' [id=' + _emberMetalUtils.GUID_KEY + Math.floor(Math.random() * new Date()) + ']');
  }
});
enifed('ember-metal/tags', ['exports', 'ember-metal/meta', 'require'], function (exports, _emberMetalMeta, _require2) {
  'use strict';

  exports.tagFor = tagFor;

  var hasGlimmer = _require2.has('glimmer-reference');
  var CONSTANT_TAG = undefined,
      CURRENT_TAG = undefined,
      DirtyableTag = undefined,
      makeTag = undefined;

  var markObjectAsDirty = undefined;

  exports.markObjectAsDirty = markObjectAsDirty;

  function tagFor(object, _meta) {
    if (!hasGlimmer) {
      throw new Error('Cannot call tagFor without Glimmer');
    }

    if (object && typeof object === 'object') {
      var meta = _meta || _emberMetalMeta.meta(object);
      return meta.writableTag(makeTag);
    } else {
      return CONSTANT_TAG;
    }
  }

  if (hasGlimmer) {
    var _require = _require2.default('glimmer-reference');

    DirtyableTag = _require.DirtyableTag;
    CONSTANT_TAG = _require.CONSTANT_TAG;
    CURRENT_TAG = _require.CURRENT_TAG;

    makeTag = function () {
      return new DirtyableTag();
    };

    exports.markObjectAsDirty = markObjectAsDirty = function (meta) {
      var tag = meta && meta.readableTag() || CURRENT_TAG;
      tag.dirty();
    };
  } else {
    exports.markObjectAsDirty = markObjectAsDirty = function () {};
  }
});
enifed('ember-metal/utils', ['exports'], function (exports) {
  'no use strict';
  // Remove "use strict"; from transpiled module until
  // https://bugs.webkit.org/show_bug.cgi?id=138038 is fixed

  /**
  @module ember-metal
  */

  /**
    Previously we used `Ember.$.uuid`, however `$.uuid` has been removed from
    jQuery master. We'll just bootstrap our own uuid now.
  
    @private
    @return {Number} the uuid
  */
  exports.uuid = uuid;
  exports.intern = intern;
  exports.generateGuid = generateGuid;
  exports.guidFor = guidFor;
  exports.wrap = wrap;
  exports.tryInvoke = tryInvoke;
  exports.makeArray = makeArray;
  exports.inspect = inspect;
  exports.applyStr = applyStr;
  exports.lookupDescriptor = lookupDescriptor;
  exports.toString = toString;
  var _uuid = 0;

  /**
    Generates a universally unique identifier. This method
    is used internally by Ember for assisting with
    the generation of GUID's and other unique identifiers.
  
    @public
    @return {Number} [description]
   */

  function uuid() {
    return ++_uuid;
  }

  /**
    Prefix used for guids through out Ember.
    @private
    @property GUID_PREFIX
    @for Ember
    @type String
    @final
  */
  var GUID_PREFIX = 'ember';

  // Used for guid generation...
  var numberCache = [];
  var stringCache = {};

  /**
    Strongly hint runtimes to intern the provided string.
  
    When do I need to use this function?
  
    For the most part, never. Pre-mature optimization is bad, and often the
    runtime does exactly what you need it to, and more often the trade-off isn't
    worth it.
  
    Why?
  
    Runtimes store strings in at least 2 different representations:
    Ropes and Symbols (interned strings). The Rope provides a memory efficient
    data-structure for strings created from concatenation or some other string
    manipulation like splitting.
  
    Unfortunately checking equality of different ropes can be quite costly as
    runtimes must resort to clever string comparison algorithms. These
    algorithms typically cost in proportion to the length of the string.
    Luckily, this is where the Symbols (interned strings) shine. As Symbols are
    unique by their string content, equality checks can be done by pointer
    comparison.
  
    How do I know if my string is a rope or symbol?
  
    Typically (warning general sweeping statement, but truthy in runtimes at
    present) static strings created as part of the JS source are interned.
    Strings often used for comparisons can be interned at runtime if some
    criteria are met.  One of these criteria can be the size of the entire rope.
    For example, in chrome 38 a rope longer then 12 characters will not
    intern, nor will segments of that rope.
  
    Some numbers: http://jsperf.com/eval-vs-keys/8
  
    Known Trick™
  
    @private
    @return {String} interned version of the provided string
  */

  function intern(str) {
    var obj = {};
    obj[str] = 1;
    for (var key in obj) {
      if (key === str) {
        return key;
      }
    }
    return str;
  }

  /**
    A unique key used to assign guids and other private metadata to objects.
    If you inspect an object in your browser debugger you will often see these.
    They can be safely ignored.
  
    On browsers that support it, these properties are added with enumeration
    disabled so they won't show up when you iterate over your properties.
  
    @private
    @property GUID_KEY
    @for Ember
    @type String
    @final
  */
  var GUID_KEY = intern('__ember' + +new Date());

  var GUID_DESC = {
    writable: true,
    configurable: true,
    enumerable: false,
    value: null
  };

  exports.GUID_DESC = GUID_DESC;
  var nullDescriptor = {
    configurable: true,
    writable: true,
    enumerable: false,
    value: null
  };

  var GUID_KEY_PROPERTY = {
    name: GUID_KEY,
    descriptor: nullDescriptor
  };

  exports.GUID_KEY_PROPERTY = GUID_KEY_PROPERTY;
  /**
    Generates a new guid, optionally saving the guid to the object that you
    pass in. You will rarely need to use this method. Instead you should
    call `Ember.guidFor(obj)`, which return an existing guid if available.
  
    @private
    @method generateGuid
    @for Ember
    @param {Object} [obj] Object the guid will be used for. If passed in, the guid will
      be saved on the object and reused whenever you pass the same object
      again.
  
      If no object is passed, just generate a new guid.
    @param {String} [prefix] Prefix to place in front of the guid. Useful when you want to
      separate the guid into separate namespaces.
    @return {String} the guid
  */

  function generateGuid(obj, prefix) {
    if (!prefix) {
      prefix = GUID_PREFIX;
    }

    var ret = prefix + uuid();
    if (obj) {
      if (obj[GUID_KEY] === null) {
        obj[GUID_KEY] = ret;
      } else {
        GUID_DESC.value = ret;
        if (obj.__defineNonEnumerable) {
          obj.__defineNonEnumerable(GUID_KEY_PROPERTY);
        } else {
          Object.defineProperty(obj, GUID_KEY, GUID_DESC);
        }
      }
    }
    return ret;
  }

  /**
    Returns a unique id for the object. If the object does not yet have a guid,
    one will be assigned to it. You can call this on any object,
    `Ember.Object`-based or not, but be aware that it will add a `_guid`
    property.
  
    You can also use this method on DOM Element objects.
  
    @public
    @method guidFor
    @for Ember
    @param {Object} obj any object, string, number, Element, or primitive
    @return {String} the unique guid for this instance.
  */

  function guidFor(obj) {
    if (obj && obj[GUID_KEY]) {
      return obj[GUID_KEY];
    }

    // special cases where we don't want to add a key to object
    if (obj === undefined) {
      return '(undefined)';
    }

    if (obj === null) {
      return '(null)';
    }

    var ret;
    var type = typeof obj;

    // Don't allow prototype changes to String etc. to change the guidFor
    switch (type) {
      case 'number':
        ret = numberCache[obj];

        if (!ret) {
          ret = numberCache[obj] = 'nu' + obj;
        }

        return ret;

      case 'string':
        ret = stringCache[obj];

        if (!ret) {
          ret = stringCache[obj] = 'st' + uuid();
        }

        return ret;

      case 'boolean':
        return obj ? '(true)' : '(false)';

      default:
        if (obj === Object) {
          return '(Object)';
        }

        if (obj === Array) {
          return '(Array)';
        }

        ret = GUID_PREFIX + uuid();

        if (obj[GUID_KEY] === null) {
          obj[GUID_KEY] = ret;
        } else {
          GUID_DESC.value = ret;

          if (obj.__defineNonEnumerable) {
            obj.__defineNonEnumerable(GUID_KEY_PROPERTY);
          } else {
            Object.defineProperty(obj, GUID_KEY, GUID_DESC);
          }
        }
        return ret;
    }
  }

  var HAS_SUPER_PATTERN = /\.(_super|call\(this|apply\(this)/;
  var fnToString = Function.prototype.toString;

  var checkHasSuper = (function () {
    var sourceAvailable = fnToString.call(function () {
      return this;
    }).indexOf('return this') > -1;

    if (sourceAvailable) {
      return function checkHasSuper(func) {
        return HAS_SUPER_PATTERN.test(fnToString.call(func));
      };
    }

    return function checkHasSuper() {
      return true;
    };
  })();

  exports.checkHasSuper = checkHasSuper;
  function ROOT() {}
  ROOT.__hasSuper = false;

  function hasSuper(func) {
    if (func.__hasSuper === undefined) {
      func.__hasSuper = checkHasSuper(func);
    }
    return func.__hasSuper;
  }

  /**
    Wraps the passed function so that `this._super` will point to the superFunc
    when the function is invoked. This is the primitive we use to implement
    calls to super.
  
    @private
    @method wrap
    @for Ember
    @param {Function} func The function to call
    @param {Function} superFunc The super function.
    @return {Function} wrapped function.
  */

  function wrap(func, superFunc) {
    if (!hasSuper(func)) {
      return func;
    }
    // ensure an unwrapped super that calls _super is wrapped with a terminal _super
    if (!superFunc.wrappedFunction && hasSuper(superFunc)) {
      return _wrap(func, _wrap(superFunc, ROOT));
    }
    return _wrap(func, superFunc);
  }

  function _wrap(func, superFunc) {
    function superWrapper() {
      var orig = this._super;
      this._super = superFunc;
      var ret = func.apply(this, arguments);
      this._super = orig;
      return ret;
    }

    superWrapper.wrappedFunction = func;
    superWrapper.__ember_observes__ = func.__ember_observes__;
    superWrapper.__ember_observesBefore__ = func.__ember_observesBefore__;
    superWrapper.__ember_listens__ = func.__ember_listens__;

    return superWrapper;
  }

  /**
    Checks to see if the `methodName` exists on the `obj`.
  
    ```javascript
    var foo = { bar: function() { return 'bar'; }, baz: null };
  
    Ember.canInvoke(foo, 'bar'); // true
    Ember.canInvoke(foo, 'baz'); // false
    Ember.canInvoke(foo, 'bat'); // false
    ```
  
    @method canInvoke
    @for Ember
    @param {Object} obj The object to check for the method
    @param {String} methodName The method name to check for
    @return {Boolean}
    @private
  */
  function canInvoke(obj, methodName) {
    return !!(obj && typeof obj[methodName] === 'function');
  }

  /**
    Checks to see if the `methodName` exists on the `obj`,
    and if it does, invokes it with the arguments passed.
  
    ```javascript
    var d = new Date('03/15/2013');
  
    Ember.tryInvoke(d, 'getTime');              // 1363320000000
    Ember.tryInvoke(d, 'setFullYear', [2014]);  // 1394856000000
    Ember.tryInvoke(d, 'noSuchMethod', [2014]); // undefined
    ```
  
    @method tryInvoke
    @for Ember
    @param {Object} obj The object to check for the method
    @param {String} methodName The method name to check for
    @param {Array} [args] The arguments to pass to the method
    @return {*} the return value of the invoked method or undefined if it cannot be invoked
    @public
  */

  function tryInvoke(obj, methodName, args) {
    if (canInvoke(obj, methodName)) {
      return args ? applyStr(obj, methodName, args) : applyStr(obj, methodName);
    }
  }

  // ........................................
  // TYPING & ARRAY MESSAGING
  //

  var objectToString = Object.prototype.toString;

  /**
    Forces the passed object to be part of an array. If the object is already
    an array, it will return the object. Otherwise, it will add the object to
    an array. If obj is `null` or `undefined`, it will return an empty array.
  
    ```javascript
    Ember.makeArray();            // []
    Ember.makeArray(null);        // []
    Ember.makeArray(undefined);   // []
    Ember.makeArray('lindsay');   // ['lindsay']
    Ember.makeArray([1, 2, 42]);  // [1, 2, 42]
  
    var controller = Ember.ArrayProxy.create({ content: [] });
  
    Ember.makeArray(controller) === controller;  // true
    ```
  
    @method makeArray
    @for Ember
    @param {Object} obj the object
    @return {Array}
    @private
  */

  function makeArray(obj) {
    if (obj === null || obj === undefined) {
      return [];
    }
    return Array.isArray(obj) ? obj : [obj];
  }

  /**
    Convenience method to inspect an object. This method will attempt to
    convert the object into a useful string description.
  
    It is a pretty simple implementation. If you want something more robust,
    use something like JSDump: https://github.com/NV/jsDump
  
    @method inspect
    @for Ember
    @param {Object} obj The object you want to inspect.
    @return {String} A description of the object
    @since 1.4.0
    @private
  */

  function inspect(obj) {
    if (obj === null) {
      return 'null';
    }
    if (obj === undefined) {
      return 'undefined';
    }
    if (Array.isArray(obj)) {
      return '[' + obj + ']';
    }
    // for non objects
    var type = typeof obj;
    if (type !== 'object' && type !== 'symbol') {
      return '' + obj;
    }
    // overridden toString
    if (typeof obj.toString === 'function' && obj.toString !== objectToString) {
      return obj.toString();
    }

    // Object.prototype.toString === {}.toString
    var v;
    var ret = [];
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        v = obj[key];
        if (v === 'toString') {
          continue;
        } // ignore useless items
        if (typeof v === 'function') {
          v = 'function() { ... }';
        }

        if (v && typeof v.toString !== 'function') {
          ret.push(key + ': ' + objectToString.call(v));
        } else {
          ret.push(key + ': ' + v);
        }
      }
    }
    return '{' + ret.join(', ') + '}';
  }

  /**
    @param {Object} t target
    @param {String} m method
    @param {Array} a args
    @private
  */

  function applyStr(t, m, a) {
    var l = a && a.length;
    if (!a || !l) {
      return t[m]();
    }
    switch (l) {
      case 1:
        return t[m](a[0]);
      case 2:
        return t[m](a[0], a[1]);
      case 3:
        return t[m](a[0], a[1], a[2]);
      case 4:
        return t[m](a[0], a[1], a[2], a[3]);
      case 5:
        return t[m](a[0], a[1], a[2], a[3], a[4]);
      default:
        return t[m].apply(t, a);
    }
  }

  function lookupDescriptor(obj, keyName) {
    var current = obj;
    while (current) {
      var descriptor = Object.getOwnPropertyDescriptor(current, keyName);

      if (descriptor) {
        return descriptor;
      }

      current = Object.getPrototypeOf(current);
    }

    return null;
  }

  // A `toString` util function that supports objects without a `toString`
  // method, e.g. an object created with `Object.create(null)`.

  function toString(obj) {
    if (obj && obj.toString) {
      return obj.toString();
    } else {
      return objectToString.call(obj);
    }
  }

  exports.GUID_KEY = GUID_KEY;
  exports.makeArray = makeArray;
  exports.canInvoke = canInvoke;
});
enifed('ember-metal/watch_key', ['exports', 'ember-metal/features', 'ember-metal/meta', 'ember-metal/properties', 'ember-metal/utils'], function (exports, _emberMetalFeatures, _emberMetalMeta, _emberMetalProperties, _emberMetalUtils) {
  'use strict';

  exports.watchKey = watchKey;
  exports.unwatchKey = unwatchKey;

  var handleMandatorySetter = undefined;

  function watchKey(obj, keyName, meta) {
    // can't watch length on Array - it is special...
    if (keyName === 'length' && Array.isArray(obj)) {
      return;
    }

    var m = meta || _emberMetalMeta.meta(obj);

    // activate watching first time
    if (!m.peekWatching(keyName)) {
      m.writeWatching(keyName, 1);

      var possibleDesc = obj[keyName];
      var desc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;
      if (desc && desc.willWatch) {
        desc.willWatch(obj, keyName);
      }

      if ('function' === typeof obj.willWatchProperty) {
        obj.willWatchProperty(keyName);
      }

      // NOTE: this is dropped for prod + minified builds
      handleMandatorySetter(m, obj, keyName);
    } else {
      m.writeWatching(keyName, (m.peekWatching(keyName) || 0) + 1);
    }
  }

  // Future traveler, although this code looks scary. It merely exists in
  // development to aid in development asertions. Production builds of
  // ember strip this entire block out
  handleMandatorySetter = function handleMandatorySetter(m, obj, keyName) {
    var descriptor = _emberMetalUtils.lookupDescriptor(obj, keyName);
    var configurable = descriptor ? descriptor.configurable : true;
    var isWritable = descriptor ? descriptor.writable : true;
    var hasValue = descriptor ? 'value' in descriptor : true;
    var possibleDesc = descriptor && descriptor.value;
    var isDescriptor = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor;

    if (isDescriptor) {
      return;
    }

    // this x in Y deopts, so keeping it in this function is better;
    if (configurable && isWritable && hasValue && keyName in obj) {
      var desc = {
        configurable: true,
        enumerable: Object.prototype.propertyIsEnumerable.call(obj, keyName),
        set: _emberMetalProperties.MANDATORY_SETTER_FUNCTION(keyName),
        get: undefined
      };

      if (Object.prototype.hasOwnProperty.call(obj, keyName)) {
        m.writeValues(keyName, obj[keyName]);
        desc.get = _emberMetalProperties.DEFAULT_GETTER_FUNCTION(keyName);
      } else {
        desc.get = _emberMetalProperties.INHERITING_GETTER_FUNCTION(keyName);
      }

      Object.defineProperty(obj, keyName, desc);
    }
  };

  function unwatchKey(obj, keyName, meta) {
    var m = meta || _emberMetalMeta.meta(obj);
    var count = m.peekWatching(keyName);
    if (count === 1) {
      m.writeWatching(keyName, 0);

      var possibleDesc = obj[keyName];
      var desc = possibleDesc !== null && typeof possibleDesc === 'object' && possibleDesc.isDescriptor ? possibleDesc : undefined;

      if (desc && desc.didUnwatch) {
        desc.didUnwatch(obj, keyName);
      }

      if ('function' === typeof obj.didUnwatchProperty) {
        obj.didUnwatchProperty(keyName);
      }

      // It is true, the following code looks quite WAT. But have no fear, It
      // exists purely to improve development ergonomics and is removed from
      // ember.min.js and ember.prod.js builds.
      //
      // Some further context: Once a property is watched by ember, bypassing `set`
      // for mutation, will bypass observation. This code exists to assert when
      // that occurs, and attempt to provide more helpful feedback. The alternative
      // is tricky to debug partially observable properties.
      if (!desc && keyName in obj) {
        var maybeMandatoryDescriptor = _emberMetalUtils.lookupDescriptor(obj, keyName);

        if (maybeMandatoryDescriptor.set && maybeMandatoryDescriptor.set.isMandatorySetter) {
          if (maybeMandatoryDescriptor.get && maybeMandatoryDescriptor.get.isInheritingGetter) {
            delete obj[keyName];
          } else {
            Object.defineProperty(obj, keyName, {
              configurable: true,
              enumerable: Object.prototype.propertyIsEnumerable.call(obj, keyName),
              writable: true,
              value: m.peekValues(keyName)
            });
            m.deleteFromValues(keyName);
          }
        }
      }
    } else if (count > 1) {
      m.writeWatching(keyName, count - 1);
    }
  }
});
enifed('ember-metal/watch_path', ['exports', 'ember-metal/meta', 'ember-metal/chains'], function (exports, _emberMetalMeta, _emberMetalChains) {
  'use strict';

  exports.watchPath = watchPath;
  exports.unwatchPath = unwatchPath;

  // get the chains for the current object. If the current object has
  // chains inherited from the proto they will be cloned and reconfigured for
  // the current object.
  function chainsFor(obj, meta) {
    return (meta || _emberMetalMeta.meta(obj)).writableChains(makeChainNode);
  }

  function makeChainNode(obj) {
    return new _emberMetalChains.ChainNode(null, null, obj);
  }

  function watchPath(obj, keyPath, meta) {
    // can't watch length on Array - it is special...
    if (keyPath === 'length' && Array.isArray(obj)) {
      return;
    }

    var m = meta || _emberMetalMeta.meta(obj);
    var counter = m.peekWatching(keyPath) || 0;
    if (!counter) {
      // activate watching first time
      m.writeWatching(keyPath, 1);
      chainsFor(obj, m).add(keyPath);
    } else {
      m.writeWatching(keyPath, counter + 1);
    }
  }

  function unwatchPath(obj, keyPath, meta) {
    var m = meta || _emberMetalMeta.meta(obj);
    var counter = m.peekWatching(keyPath) || 0;

    if (counter === 1) {
      m.writeWatching(keyPath, 0);
      chainsFor(obj, m).remove(keyPath);
    } else if (counter > 1) {
      m.writeWatching(keyPath, counter - 1);
    }
  }
});
enifed('ember-metal/watching', ['exports', 'ember-metal/chains', 'ember-metal/watch_key', 'ember-metal/watch_path', 'ember-metal/path_cache', 'ember-metal/meta'], function (exports, _emberMetalChains, _emberMetalWatch_key, _emberMetalWatch_path, _emberMetalPath_cache, _emberMetalMeta) {
  /**
  @module ember-metal
  */

  'use strict';

  exports.isWatching = isWatching;
  exports.watcherCount = watcherCount;
  exports.unwatch = unwatch;
  exports.destroy = destroy;

  /**
    Starts watching a property on an object. Whenever the property changes,
    invokes `Ember.propertyWillChange` and `Ember.propertyDidChange`. This is the
    primitive used by observers and dependent keys; usually you will never call
    this method directly but instead use higher level methods like
    `Ember.addObserver()`
  
    @private
    @method watch
    @for Ember
    @param obj
    @param {String} _keyPath
  */
  function watch(obj, _keyPath, m) {
    // can't watch length on Array - it is special...
    if (_keyPath === 'length' && Array.isArray(obj)) {
      return;
    }

    if (!_emberMetalPath_cache.isPath(_keyPath)) {
      _emberMetalWatch_key.watchKey(obj, _keyPath, m);
    } else {
      _emberMetalWatch_path.watchPath(obj, _keyPath, m);
    }
  }

  exports.watch = watch;

  function isWatching(obj, key) {
    var meta = _emberMetalMeta.peekMeta(obj);
    return (meta && meta.peekWatching(key)) > 0;
  }

  function watcherCount(obj, key) {
    var meta = _emberMetalMeta.peekMeta(obj);
    return meta && meta.peekWatching(key) || 0;
  }

  function unwatch(obj, _keyPath, m) {
    // can't watch length on Array - it is special...
    if (_keyPath === 'length' && Array.isArray(obj)) {
      return;
    }

    if (!_emberMetalPath_cache.isPath(_keyPath)) {
      _emberMetalWatch_key.unwatchKey(obj, _keyPath, m);
    } else {
      _emberMetalWatch_path.unwatchPath(obj, _keyPath, m);
    }
  }

  var NODE_STACK = [];

  /**
    Tears down the meta on an object so that it can be garbage collected.
    Multiple calls will have no effect.
  
    @method destroy
    @for Ember
    @param {Object} obj  the object to destroy
    @return {void}
    @private
  */

  function destroy(obj) {
    var meta = _emberMetalMeta.peekMeta(obj);
    var node, nodes, key, nodeObject;

    if (meta) {
      _emberMetalMeta.deleteMeta(obj);
      // remove chainWatchers to remove circular references that would prevent GC
      node = meta.readableChains();
      if (node) {
        NODE_STACK.push(node);
        // process tree
        while (NODE_STACK.length > 0) {
          node = NODE_STACK.pop();
          // push children
          nodes = node._chains;
          if (nodes) {
            for (key in nodes) {
              if (nodes[key] !== undefined) {
                NODE_STACK.push(nodes[key]);
              }
            }
          }
          // remove chainWatcher in node object
          if (node._watching) {
            nodeObject = node._object;
            if (nodeObject) {
              _emberMetalChains.removeChainWatcher(nodeObject, node._key, node);
            }
          }
        }
      }
    }
  }
});
enifed('ember-metal/weak_map', ['exports', 'ember-metal/debug', 'ember-metal/utils', 'ember-metal/meta'], function (exports, _emberMetalDebug, _emberMetalUtils, _emberMetalMeta) {
  'use strict';

  exports.default = WeakMap;

  var id = 0;
  function UNDEFINED() {}

  /*
   * @private
   * @class Ember.WeakMap
   *
   * A partial polyfill for [WeakMap](http://www.ecma-international.org/ecma-262/6.0/#sec-weakmap-objects).
   *
   * There is a small but important caveat. This implementation assumes that the
   * weak map will live longer (in the sense of garbage collection) than all of its
   * keys, otherwise it is possible to leak the values stored in the weak map. In
   * practice, most use cases satisfy this limitation which is why it is included
   * in ember-metal.
   */

  function WeakMap() {
    _emberMetalDebug.assert('Invoking the WeakMap constructor with arguments is not supported at this time', arguments.length === 0);

    this._id = _emberMetalUtils.GUID_KEY + id++;
  }

  /*
   * @method get
   * @param key {Object | Function}
   * @return {Any} stored value
   */
  WeakMap.prototype.get = function (obj) {
    var meta = _emberMetalMeta.peekMeta(obj);
    if (meta) {
      var map = meta.readableWeak();
      if (map) {
        if (map[this._id] === UNDEFINED) {
          return undefined;
        }

        return map[this._id];
      }
    }
  };

  /*
   * @method set
   * @param key {Object | Function}
   * @param value {Any}
   * @return {WeakMap} the weak map
   */
  WeakMap.prototype.set = function (obj, value) {
    _emberMetalDebug.assert('Uncaught TypeError: Invalid value used as weak map key', obj && (typeof obj === 'object' || typeof obj === 'function'));

    if (value === undefined) {
      value = UNDEFINED;
    }

    _emberMetalMeta.meta(obj).writableWeak()[this._id] = value;

    return this;
  };

  /*
   * @method has
   * @param key {Object | Function}
   * @return {boolean} if the key exists
   */
  WeakMap.prototype.has = function (obj) {
    var meta = _emberMetalMeta.peekMeta(obj);
    if (meta) {
      var map = meta.readableWeak();
      if (map) {
        return map[this._id] !== undefined;
      }
    }

    return false;
  };

  /*
   * @method delete
   * @param key {Object | Function}
   * @return {boolean} if the key was deleted
   */
  WeakMap.prototype.delete = function (obj) {
    if (this.has(obj)) {
      delete _emberMetalMeta.meta(obj).writableWeak()[this._id];
      return true;
    } else {
      return false;
    }
  };
});
enifed('ember-template-compiler/compat', ['exports', 'ember-metal/core', 'ember-template-compiler/compat/precompile', 'ember-template-compiler/system/compile', 'ember-template-compiler/system/template'], function (exports, _emberMetalCore, _emberTemplateCompilerCompatPrecompile, _emberTemplateCompilerSystemCompile, _emberTemplateCompilerSystemTemplate) {
  'use strict';

  var EmberHandlebars = _emberMetalCore.default.Handlebars = _emberMetalCore.default.Handlebars || {};

  EmberHandlebars.precompile = _emberTemplateCompilerCompatPrecompile.default;
  EmberHandlebars.compile = _emberTemplateCompilerSystemCompile.default;
  EmberHandlebars.template = _emberTemplateCompilerSystemTemplate.default;
});
enifed('ember-template-compiler/compat/precompile', ['exports', 'require', 'ember-template-compiler/system/compile_options'], function (exports, _require, _emberTemplateCompilerSystemCompile_options) {
  /**
  @module ember
  @submodule ember-template-compiler
  */
  'use strict';

  var compile, compileSpec;

  exports.default = function (string) {
    if ((!compile || !compileSpec) && _require.has('htmlbars-compiler/compiler')) {
      var Compiler = _require.default('htmlbars-compiler/compiler');

      compile = Compiler.compile;
      compileSpec = Compiler.compileSpec;
    }

    if (!compile || !compileSpec) {
      throw new Error('Cannot call `precompile` without the template compiler loaded. Please load `ember-template-compiler.js` prior to calling `precompile`.');
    }

    var asObject = arguments[1] === undefined ? true : arguments[1];
    var compileFunc = asObject ? compile : compileSpec;

    return compileFunc(string, _emberTemplateCompilerSystemCompile_options.default());
  };
});
enifed('ember-template-compiler/index', ['exports', 'ember-metal', 'ember-template-compiler/system/precompile', 'ember-template-compiler/system/compile', 'ember-template-compiler/system/template', 'ember-template-compiler/plugins', 'ember-template-compiler/plugins/transform-old-binding-syntax', 'ember-template-compiler/plugins/transform-old-class-binding-syntax', 'ember-template-compiler/plugins/transform-item-class', 'ember-template-compiler/plugins/transform-closure-component-attrs-into-mut', 'ember-template-compiler/plugins/transform-component-attrs-into-mut', 'ember-template-compiler/plugins/transform-component-curly-to-readonly', 'ember-template-compiler/plugins/transform-angle-bracket-components', 'ember-template-compiler/plugins/transform-input-on-to-onEvent', 'ember-template-compiler/plugins/transform-top-level-components', 'ember-template-compiler/plugins/deprecate-render-model', 'ember-template-compiler/plugins/prevent-render-block', 'ember-template-compiler/plugins/transform-inline-link-to', 'ember-template-compiler/plugins/assert-no-view-and-controller-paths', 'ember-template-compiler/plugins/assert-no-view-helper', 'ember-template-compiler/plugins/assert-no-each-in', 'ember-template-compiler/compat'], function (exports, _emberMetal, _emberTemplateCompilerSystemPrecompile, _emberTemplateCompilerSystemCompile, _emberTemplateCompilerSystemTemplate, _emberTemplateCompilerPlugins, _emberTemplateCompilerPluginsTransformOldBindingSyntax, _emberTemplateCompilerPluginsTransformOldClassBindingSyntax, _emberTemplateCompilerPluginsTransformItemClass, _emberTemplateCompilerPluginsTransformClosureComponentAttrsIntoMut, _emberTemplateCompilerPluginsTransformComponentAttrsIntoMut, _emberTemplateCompilerPluginsTransformComponentCurlyToReadonly, _emberTemplateCompilerPluginsTransformAngleBracketComponents, _emberTemplateCompilerPluginsTransformInputOnToOnEvent, _emberTemplateCompilerPluginsTransformTopLevelComponents, _emberTemplateCompilerPluginsDeprecateRenderModel, _emberTemplateCompilerPluginsPreventRenderBlock, _emberTemplateCompilerPluginsTransformInlineLinkTo, _emberTemplateCompilerPluginsAssertNoViewAndControllerPaths, _emberTemplateCompilerPluginsAssertNoViewHelper, _emberTemplateCompilerPluginsAssertNoEachIn, _emberTemplateCompilerCompat) {
  'use strict';

  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformOldBindingSyntax.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformOldClassBindingSyntax.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformItemClass.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformClosureComponentAttrsIntoMut.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformComponentAttrsIntoMut.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformComponentCurlyToReadonly.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformAngleBracketComponents.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformInputOnToOnEvent.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformTopLevelComponents.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsDeprecateRenderModel.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsPreventRenderBlock.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsAssertNoEachIn.default);
  _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsTransformInlineLinkTo.default);

  if (!_emberMetal.default.ENV._ENABLE_LEGACY_VIEW_SUPPORT) {
    _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsAssertNoViewAndControllerPaths.default);
    _emberTemplateCompilerPlugins.registerPlugin('ast', _emberTemplateCompilerPluginsAssertNoViewHelper.default);
  }

  exports._Ember = _emberMetal.default;
  exports.precompile = _emberTemplateCompilerSystemPrecompile.default;
  exports.compile = _emberTemplateCompilerSystemCompile.default;
  exports.template = _emberTemplateCompilerSystemTemplate.default;
  exports.registerPlugin = _emberTemplateCompilerPlugins.registerPlugin;
});

// used for adding Ember.Handlebars.compile for backwards compat
enifed('ember-template-compiler/plugins', ['exports'], function (exports) {
  /**
  @module ember
  @submodule ember-template-compiler
  */

  /**
   @private
   @property helpers
  */
  'use strict';

  exports.registerPlugin = registerPlugin;
  var plugins = {
    ast: []
  };

  /**
    Adds an AST plugin to be used by Ember.HTMLBars.compile.
  
    @private
    @method registerASTPlugin
  */

  function registerPlugin(type, Plugin) {
    if (!plugins[type]) {
      throw new Error('Attempting to register "' + Plugin + '" as "' + type + '" which is not a valid HTMLBars plugin type.');
    }

    plugins[type].push(Plugin);
  }

  exports.default = plugins;
});
enifed('ember-template-compiler/plugins/assert-no-each-in', ['exports', 'ember-metal/core', 'ember-metal/debug', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalCore, _emberMetalDebug, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  function AssertNoEachIn() {
    var options = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

    this.syntax = null;
    this.options = options;
  }

  AssertNoEachIn.prototype.transform = function AssertNoEachIn_transform(ast) {
    if (!!_emberMetalCore.default.ENV._ENABLE_LEGACY_VIEW_SUPPORT) {
      return ast;
    }
    var walker = new this.syntax.Walker();
    var moduleName = this.options && this.options.moduleName;

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }
      assertHelper(moduleName, node);
    });

    return ast;
  };

  function assertHelper(moduleName, node) {
    var moduleInfo = _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, node.loc);
    var singular = node.params[0].original;
    var plural = node.params[2].original;

    _emberMetalDebug.assert('Using {{#each ' + singular + ' in ' + plural + '}} ' + moduleInfo + 'is no longer supported in Ember 2.0+, please use {{#each ' + plural + ' as |' + singular + '|}}');
  }

  function validate(node) {
    return (node.type === 'BlockStatement' || node.type === 'MustacheStatement') && node.path.original === 'each' && node.params.length === 3 && node.params[1].type === 'PathExpression' && node.params[1].original === 'in';
  }

  exports.default = AssertNoEachIn;
});
enifed('ember-template-compiler/plugins/assert-no-view-and-controller-paths', ['exports', 'ember-metal/core', 'ember-metal/debug', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalCore, _emberMetalDebug, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  function AssertNoViewAndControllerPaths() {
    var options = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

    // set later within HTMLBars to the syntax package
    this.syntax = null;
    this.options = options;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  AssertNoViewAndControllerPaths.prototype.transform = function AssertNoViewAndControllerPaths_transform(ast) {
    var walker = new this.syntax.Walker();
    var moduleName = this.options && this.options.moduleName;

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      assertPath(moduleName, node, node.path);
      assertPaths(moduleName, node, node.params);
      assertHash(moduleName, node, node.hash);
    });

    return ast;
  };

  function assertHash(moduleName, node, hash) {
    if (!hash || !hash.pairs) {
      return;
    }
    var i, l, pair, paths;
    for (i = 0, l = hash.pairs.length; i < l; i++) {
      pair = hash.pairs[i];
      paths = pair.value.params;
      assertPaths(moduleName, pair, paths);
    }
  }

  function assertPaths(moduleName, node, paths) {
    if (!paths) {
      return;
    }
    var i, l, path;
    for (i = 0, l = paths.length; i < l; i++) {
      path = paths[i];
      assertPath(moduleName, node, path);
    }
  }

  function assertPath(moduleName, node, path) {
    _emberMetalDebug.assert('Using `{{' + (path && path.type === 'PathExpression' && path.parts[0]) + '}}` or any path based on it ' + _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, node.loc) + 'has been removed in Ember 2.0', (function () {
      var noAssertion = true;

      // allow opt-out of the assertion when legacy addons are present
      var viewKeyword = path && path.type === 'PathExpression' && path.parts && path.parts[0];
      if (viewKeyword === 'view') {
        noAssertion = _emberMetalCore.default.ENV._ENABLE_LEGACY_VIEW_SUPPORT;
      }

      return noAssertion;
    })(), {
      id: path.parts && path.parts[0] === 'view' ? 'view.keyword.view' : 'view.keyword.controller',
      until: '2.0.0'
    });
  }

  function validate(node) {
    return node.type === 'MustacheStatement' || node.type === 'BlockStatement';
  }

  exports.default = AssertNoViewAndControllerPaths;
});
enifed('ember-template-compiler/plugins/assert-no-view-helper', ['exports', 'ember-metal/core', 'ember-metal/debug', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalCore, _emberMetalDebug, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  function AssertNoViewHelper() {
    var options = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

    // set later within HTMLBars to the syntax package
    this.syntax = null;
    this.options = options;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  AssertNoViewHelper.prototype.transform = function AssertNoViewHelper_transform(ast) {
    if (!!_emberMetalCore.default.ENV._ENABLE_LEGACY_VIEW_SUPPORT) {
      return ast;
    }
    var walker = new this.syntax.Walker();
    var moduleName = this.options && this.options.moduleName;

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      assertHelper(moduleName, node);
    });

    return ast;
  };

  function assertHelper(moduleName, node) {
    var paramValue = node.params.length && node.params[0].value;

    if (!paramValue) {
      return;
    } else {
      _emberMetalDebug.assert('Using the `{{view "string"}}` helper is removed in 2.0. ' + _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, node.loc), _emberMetalCore.default.ENV._ENABLE_LEGACY_VIEW_SUPPORT, { id: 'view.helper', until: '2.0.0' });
    }
  }

  function validate(node) {
    return (node.type === 'MustacheStatement' || node.type === 'BlockStatement') && node.path.parts[0] === 'view';
  }

  exports.default = AssertNoViewHelper;
});
enifed('ember-template-compiler/plugins/deprecate-render-model', ['exports', 'ember-metal/debug', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalDebug, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  exports.default = DeprecateRenderModel;

  function DeprecateRenderModel(options) {
    this.syntax = null;
    this.options = options;
  }

  DeprecateRenderModel.prototype.transform = function DeprecateRenderModel_transform(ast) {
    var moduleName = this.options.moduleName;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      each(node.params, function (param) {
        if (param.type !== 'PathExpression') {
          return;
        }

        _emberMetalDebug.deprecate(deprecationMessage(moduleName, node, param), false, {
          id: 'ember-template-compiler.deprecate-render-model',
          until: '3.0.0',
          url: 'http://emberjs.com/deprecations/v2.x#toc_model-param-in-code-render-code-helper'
        });
      });
    });

    return ast;
  };

  function validate(node) {
    return node.type === 'MustacheStatement' && node.path.original === 'render' && node.params.length > 1;
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i]);
    }
  }

  function deprecationMessage(moduleName, node, param) {
    var sourceInformation = _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, node.loc);
    var componentName = node.params[0].original;
    var modelName = param.original;
    var original = '{{render "' + componentName + '" ' + modelName + '}}';
    var preferred = '{{' + componentName + ' model=' + modelName + '}}';

    return 'Please refactor `' + original + '` to a component and invoke via' + (' `' + preferred + '`. ' + sourceInformation);
  }
});
enifed('ember-template-compiler/plugins/prevent-render-block', ['exports', 'ember-metal/error', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalError, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  exports.default = PreventRenderBlock;

  function PreventRenderBlock(options) {
    this.syntax = null;
    this.options = options;
  }

  PreventRenderBlock.prototype.transform = function PreventRenderBlock_transform(ast) {
    var moduleName = this.options.moduleName;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      throw new _emberMetalError.default(assertionMessage(moduleName, node));
    });

    return ast;
  };

  function validate(node) {
    return node.type === 'BlockStatement' && node.path.original === 'render';
  }

  function assertionMessage(moduleName, node) {
    var sourceInformation = _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, node.loc);

    return 'Usage of `render` in block form is unsupported ' + sourceInformation + '.';
  }
});
enifed('ember-template-compiler/plugins/transform-angle-bracket-components', ['exports'], function (exports) {
  'use strict';

  function TransformAngleBracketComponents() {
    // set later within HTMLBars to the syntax package
    this.syntax = null;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  TransformAngleBracketComponents.prototype.transform = function TransformAngleBracketComponents_transform(ast) {
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      node.tag = '<' + node.tag + '>';
    });

    return ast;
  };

  function validate(node) {
    return node.type === 'ComponentNode';
  }

  exports.default = TransformAngleBracketComponents;
});
enifed('ember-template-compiler/plugins/transform-closure-component-attrs-into-mut', ['exports'], function (exports) {
  'use strict';

  function TransformClosureComponentAttrsIntoMut() {
    // set later within HTMLBars to the syntax package
    this.syntax = null;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  TransformClosureComponentAttrsIntoMut.prototype.transform = function TransformClosureComponentAttrsIntoMut_transform(ast) {
    var b = this.syntax.builders;

    this.syntax.traverse(ast, {
      SubExpression: function (node) {
        if (isComponentClosure(node)) {
          mutParameters(b, node);
        }
      }
    });

    return ast;
  };

  function isComponentClosure(node) {
    return node.type === 'SubExpression' && node.path.original === 'component';
  }

  function mutParameters(builder, node) {
    for (var i = 1; i < node.params.length; i++) {
      if (node.params[i].type === 'PathExpression') {
        node.params[i] = builder.sexpr(builder.path('@mut'), [node.params[i]]);
      }
    }

    each(node.hash.pairs, function (pair) {
      var value = pair.value;

      if (value.type === 'PathExpression') {
        pair.value = builder.sexpr(builder.path('@mut'), [pair.value]);
      }
    });
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i]);
    }
  }

  exports.default = TransformClosureComponentAttrsIntoMut;
});
enifed('ember-template-compiler/plugins/transform-component-attrs-into-mut', ['exports'], function (exports) {
  'use strict';

  function TransformComponentAttrsIntoMut() {
    // set later within HTMLBars to the syntax package
    this.syntax = null;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  TransformComponentAttrsIntoMut.prototype.transform = function TransformComponentAttrsIntoMut_transform(ast) {
    var b = this.syntax.builders;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      each(node.hash.pairs, function (pair) {
        var value = pair.value;

        if (value.type === 'PathExpression') {
          pair.value = b.sexpr(b.path('@mut'), [pair.value]);
        }
      });
    });

    return ast;
  };

  function validate(node) {
    return node.type === 'BlockStatement' || node.type === 'MustacheStatement';
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i]);
    }
  }

  exports.default = TransformComponentAttrsIntoMut;
});
enifed('ember-template-compiler/plugins/transform-component-curly-to-readonly', ['exports'], function (exports) {
  'use strict';

  function TransformComponentCurlyToReadonly() {
    // set later within HTMLBars to the syntax package
    this.syntax = null;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  TransformComponentCurlyToReadonly.prototype.transform = function TransformComponetnCurlyToReadonly_transform(ast) {
    var b = this.syntax.builders;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      each(node.attributes, function (attr) {
        if (attr.value.type !== 'MustacheStatement') {
          return;
        }
        if (attr.value.params.length || attr.value.hash.pairs.length) {
          return;
        }

        attr.value = b.mustache(b.path('readonly'), [attr.value.path], null, !attr.value.escape);
      });
    });

    return ast;
  };

  function validate(node) {
    return node.type === 'ComponentNode';
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i]);
    }
  }

  exports.default = TransformComponentCurlyToReadonly;
});
enifed('ember-template-compiler/plugins/transform-inline-link-to', ['exports'], function (exports) {
  'use strict';

  exports.default = TransformInlineLinkTo;

  function TransformInlineLinkTo(options) {
    this.options = options;
    this.syntax = null;
  }

  TransformInlineLinkTo.prototype.transform = function TransformInlineLinkTo_transform(ast) {
    var _syntax = this.syntax;
    var traverse = _syntax.traverse;
    var b = _syntax.builders;

    function buildProgram(content, loc) {
      return b.program([buildStatement(content, loc)], null, loc);
    }

    function buildStatement(content, loc) {
      switch (content.type) {
        case 'PathExpression':
          return b.mustache(content, null, null, null, loc);

        case 'SubExpression':
          return b.mustache(content.path, content.params, content.hash, null, loc);

        // The default case handles literals.
        default:
          return b.text('' + content.value, loc);
      }
    }

    function unsafeHtml(expr) {
      return b.sexpr('-html-safe', [expr]);
    }

    traverse(ast, {
      MustacheStatement: function (node) {
        if (node.path.original === 'link-to') {
          var content = node.escaped ? node.params[0] : unsafeHtml(node.params[0]);
          return b.block('link-to', node.params.slice(1), node.hash, buildProgram(content, node.loc), null, node.loc);
        }
      }
    });

    return ast;
  };
});
enifed('ember-template-compiler/plugins/transform-input-on-to-onEvent', ['exports', 'ember-metal/debug', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalDebug, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  /**
   @module ember
   @submodule ember-htmlbars
  */

  /**
    An HTMLBars AST transformation that replaces all instances of
  
    ```handlebars
   {{input on="enter" action="doStuff"}}
   {{input on="key-press" action="doStuff"}}
    ```
  
    with
  
    ```handlebars
   {{input enter="doStuff"}}
   {{input key-press="doStuff"}}
    ```
  
    @private
    @class TransformInputOnToOnEvent
  */
  function TransformInputOnToOnEvent() {
    var options = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

    // set later within HTMLBars to the syntax package
    this.syntax = null;
    this.options = options;
  }

  /**
    @private
    @method transform
    @param {AST} ast The AST to be transformed.
  */
  TransformInputOnToOnEvent.prototype.transform = function TransformInputOnToOnEvent_transform(ast) {
    var pluginContext = this;
    var b = pluginContext.syntax.builders;
    var walker = new pluginContext.syntax.Walker();
    var moduleName = pluginContext.options.moduleName;

    walker.visit(ast, function (node) {
      if (pluginContext.validate(node)) {
        var action = hashPairForKey(node.hash, 'action');
        var on = hashPairForKey(node.hash, 'on');
        var onEvent = hashPairForKey(node.hash, 'onEvent');
        var normalizedOn = on || onEvent;
        var moduleInfo = _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, node.loc);

        if (normalizedOn && normalizedOn.value.type !== 'StringLiteral') {
          _emberMetalDebug.deprecate('Using a dynamic value for \'#{normalizedOn.key}=\' with the \'{{input}}\' helper ' + moduleInfo + 'is deprecated.', false, { id: 'ember-template-compiler.transform-input-on-to-onEvent.dynamic-value', until: '3.0.0' });

          normalizedOn.key = 'onEvent';
          return; // exit early, as we cannot transform further
        }

        removeFromHash(node.hash, normalizedOn);
        removeFromHash(node.hash, action);

        if (!action) {
          _emberMetalDebug.deprecate('Using \'{{input ' + normalizedOn.key + '="' + normalizedOn.value.value + '" ...}}\' without specifying an action ' + moduleInfo + 'will do nothing.', false, { id: 'ember-template-compiler.transform-input-on-to-onEvent.no-action', until: '3.0.0' });

          return; // exit early, if no action was available there is nothing to do
        }

        var specifiedOn = normalizedOn ? normalizedOn.key + '="' + normalizedOn.value.value + '" ' : '';
        if (normalizedOn && normalizedOn.value.value === 'keyPress') {
          // using `keyPress` in the root of the component will
          // clobber the keyPress event handler
          normalizedOn.value.value = 'key-press';
        }

        var expected = (normalizedOn ? normalizedOn.value.value : 'enter') + '="' + action.value.original + '"';

        _emberMetalDebug.deprecate('Using \'{{input ' + specifiedOn + 'action="' + action.value.original + '"}}\' ' + moduleInfo + 'is deprecated. Please use \'{{input ' + expected + '}}\' instead.', false, { id: 'ember-template-compiler.transform-input-on-to-onEvent.normalized-on', until: '3.0.0' });
        if (!normalizedOn) {
          normalizedOn = b.pair('onEvent', b.string('enter'));
        }

        node.hash.pairs.push(b.pair(normalizedOn.value.value, action.value));
      }
    });

    return ast;
  };

  TransformInputOnToOnEvent.prototype.validate = function TransformWithAsToHash_validate(node) {
    return node.type === 'MustacheStatement' && node.path.original === 'input' && (hashPairForKey(node.hash, 'action') || hashPairForKey(node.hash, 'on') || hashPairForKey(node.hash, 'onEvent'));
  };

  function hashPairForKey(hash, key) {
    for (var i = 0, l = hash.pairs.length; i < l; i++) {
      var pair = hash.pairs[i];
      if (pair.key === key) {
        return pair;
      }
    }

    return false;
  }

  function removeFromHash(hash, pairToRemove) {
    var newPairs = [];
    for (var i = 0, l = hash.pairs.length; i < l; i++) {
      var pair = hash.pairs[i];

      if (pair !== pairToRemove) {
        newPairs.push(pair);
      }
    }

    hash.pairs = newPairs;
  }

  exports.default = TransformInputOnToOnEvent;
});
enifed('ember-template-compiler/plugins/transform-item-class', ['exports'], function (exports) {
  'use strict';

  exports.default = TransformItemClass;

  function TransformItemClass() {
    this.syntax = null;
  }

  TransformItemClass.prototype.transform = function TransformItemClass_transform(ast) {
    var b = this.syntax.builders;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      each(node.hash.pairs, function (pair) {
        var key = pair.key;
        var value = pair.value;

        if (key !== 'itemClass') {
          return;
        }
        if (value.type === 'StringLiteral') {
          return;
        }

        var propName = value.original;
        var params = [value];
        var sexprParams = [b.string(propName), b.path(propName)];

        params.push(b.sexpr(b.string('-normalize-class'), sexprParams));
        var sexpr = b.sexpr(b.string('if'), params);

        pair.value = sexpr;
      });
    });

    return ast;
  };

  function validate(node) {
    return (node.type === 'BlockStatement' || node.type === 'MustacheStatement') && node.path.original === 'collection';
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i]);
    }
  }
});
enifed('ember-template-compiler/plugins/transform-old-binding-syntax', ['exports', 'ember-metal/debug', 'ember-template-compiler/system/calculate-location-display'], function (exports, _emberMetalDebug, _emberTemplateCompilerSystemCalculateLocationDisplay) {
  'use strict';

  exports.default = TransformOldBindingSyntax;

  function TransformOldBindingSyntax(options) {
    this.syntax = null;
    this.options = options;
  }

  TransformOldBindingSyntax.prototype.transform = function TransformOldBindingSyntax_transform(ast) {
    var moduleName = this.options.moduleName;
    var b = this.syntax.builders;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      each(node.hash.pairs, function (pair) {
        var key = pair.key;
        var value = pair.value;

        var sourceInformation = _emberTemplateCompilerSystemCalculateLocationDisplay.default(moduleName, pair.loc);

        if (key === 'classBinding') {
          return;
        }

        _emberMetalDebug.assert('Setting \'attributeBindings\' via template helpers is not allowed ' + sourceInformation, key !== 'attributeBindings');

        if (key.substr(-7) === 'Binding') {
          var newKey = key.slice(0, -7);

          _emberMetalDebug.deprecate('You\'re using legacy binding syntax: ' + key + '=' + exprToString(value) + ' ' + sourceInformation + '. Please replace with ' + newKey + '=' + value.original, false, { id: 'ember-template-compiler.transform-old-binding-syntax', until: '3.0.0' });

          pair.key = newKey;
          if (value.type === 'StringLiteral') {
            pair.value = b.path(value.original);
          }
        }
      });
    });

    return ast;
  };

  function validate(node) {
    return node.type === 'BlockStatement' || node.type === 'MustacheStatement';
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i]);
    }
  }

  function exprToString(expr) {
    switch (expr.type) {
      case 'StringLiteral':
        return '"' + expr.original + '"';
      case 'PathExpression':
        return expr.original;
    }
  }
});
enifed('ember-template-compiler/plugins/transform-old-class-binding-syntax', ['exports'], function (exports) {
  'use strict';

  exports.default = TransformOldClassBindingSyntax;

  function TransformOldClassBindingSyntax(options) {
    this.syntax = null;
    this.options = options;
  }

  TransformOldClassBindingSyntax.prototype.transform = function TransformOldClassBindingSyntax_transform(ast) {
    var b = this.syntax.builders;
    var walker = new this.syntax.Walker();

    walker.visit(ast, function (node) {
      if (!validate(node)) {
        return;
      }

      var allOfTheMicrosyntaxes = [];
      var allOfTheMicrosyntaxIndexes = [];
      var classPair = undefined;

      each(node.hash.pairs, function (pair, index) {
        var key = pair.key;

        if (key === 'classBinding' || key === 'classNameBindings') {
          allOfTheMicrosyntaxIndexes.push(index);
          allOfTheMicrosyntaxes.push(pair);
        } else if (key === 'class') {
          classPair = pair;
        }
      });

      if (allOfTheMicrosyntaxes.length === 0) {
        return;
      }

      var classValue = [];

      if (classPair) {
        classValue.push(classPair.value);
        classValue.push(b.string(' '));
      } else {
        classPair = b.pair('class', null);
        node.hash.pairs.push(classPair);
      }

      each(allOfTheMicrosyntaxIndexes, function (index) {
        node.hash.pairs.splice(index, 1);
      });

      each(allOfTheMicrosyntaxes, function (_ref) {
        var value = _ref.value;
        var loc = _ref.loc;

        var sexprs = [];
        // TODO: add helpful deprecation when both `classNames` and `classNameBindings` can
        // be removed.

        if (value.type === 'StringLiteral') {
          var microsyntax = parseMicrosyntax(value.original);

          buildSexprs(microsyntax, sexprs, b);

          classValue.push.apply(classValue, sexprs);
        }
      });

      var hash = b.hash();
      classPair.value = b.sexpr(b.string('concat'), classValue, hash);
    });

    return ast;
  };

  function buildSexprs(microsyntax, sexprs, b) {
    for (var i = 0, l = microsyntax.length; i < l; i++) {
      var _microsyntax$i = microsyntax[i];
      var propName = _microsyntax$i[0];
      var activeClass = _microsyntax$i[1];
      var inactiveClass = _microsyntax$i[2];

      var sexpr = undefined;

      // :my-class-name microsyntax for static values
      if (propName === '') {
        sexpr = b.string(activeClass);
      } else {
        var params = [b.path(propName)];

        if (activeClass) {
          params.push(b.string(activeClass));
        } else {
          var sexprParams = [b.string(propName), b.path(propName)];

          var hash = b.hash();
          if (activeClass !== undefined) {
            hash.pairs.push(b.pair('activeClass', b.string(activeClass)));
          }

          if (inactiveClass !== undefined) {
            hash.pairs.push(b.pair('inactiveClass', b.string(inactiveClass)));
          }

          params.push(b.sexpr(b.string('-normalize-class'), sexprParams, hash));
        }

        if (inactiveClass) {
          params.push(b.string(inactiveClass));
        }

        sexpr = b.sexpr(b.string('if'), params);
      }

      sexprs.push(sexpr);
      sexprs.push(b.string(' '));
    }
  }

  function validate(node) {
    return node.type === 'BlockStatement' || node.type === 'MustacheStatement';
  }

  function each(list, callback) {
    for (var i = 0, l = list.length; i < l; i++) {
      callback(list[i], i);
    }
  }

  function parseMicrosyntax(string) {
    var segments = string.split(' ');

    for (var i = 0, l = segments.length; i < l; i++) {
      segments[i] = segments[i].split(':');
    }

    return segments;
  }
});
enifed('ember-template-compiler/plugins/transform-top-level-components', ['exports'], function (exports) {
  'use strict';

  function TransformTopLevelComponents() {
    // set later within HTMLBars to the syntax package
    this.syntax = null;
  }

  /**
    @private
    @method transform
    @param {AST} The AST to be transformed.
  */
  TransformTopLevelComponents.prototype.transform = function TransformTopLevelComponents_transform(ast) {
    var b = this.syntax.builders;

    hasSingleComponentNode(ast, function (component) {
      if (component.type === 'ComponentNode') {
        component.tag = '@' + component.tag;
        component.isStatic = true;
      }
    }, function (element) {
      var hasTripleCurlies = element.attributes.some(function (attr) {
        return attr.value.escaped === false;
      });

      if (element.modifiers.length || hasTripleCurlies) {
        return element;
      } else {
        // TODO: Properly copy loc from children
        var program = b.program(element.children);
        var component = b.component('@<' + element.tag + '>', element.attributes, program, element.loc);
        component.isStatic = true;
        return component;
      }
    });

    return ast;
  };

  function hasSingleComponentNode(program, componentCallback, elementCallback) {
    var loc = program.loc;
    var body = program.body;

    if (!loc || loc.start.line !== 1 || loc.start.column !== 0) {
      return;
    }

    var lastComponentNode = undefined;
    var lastIndex = undefined;
    var nodeCount = 0;

    for (var i = 0, l = body.length; i < l; i++) {
      var curr = body[i];

      // text node with whitespace only
      if (curr.type === 'TextNode' && /^[\s]*$/.test(curr.chars)) {
        continue;
      }

      // has multiple root elements if we've been here before
      if (nodeCount++ > 0) {
        return false;
      }

      if (curr.type === 'ComponentNode' || curr.type === 'ElementNode') {
        lastComponentNode = curr;
        lastIndex = i;
      }
    }

    if (!lastComponentNode) {
      return;
    }

    if (lastComponentNode.type === 'ComponentNode') {
      componentCallback(lastComponentNode);
    }
  }

  exports.default = TransformTopLevelComponents;
});
enifed('ember-template-compiler/system/calculate-location-display', ['exports'], function (exports) {
  'use strict';

  exports.default = calculateLocationDisplay;

  function calculateLocationDisplay(moduleName, _loc) {
    var loc = _loc || {};

    var _ref = loc.start || {};

    var column = _ref.column;
    var line = _ref.line;

    var moduleInfo = '';
    if (moduleName) {
      moduleInfo += '\'' + moduleName + '\' ';
    }

    if (line !== undefined && column !== undefined) {
      if (moduleName) {
        // only prepend @ if the moduleName was present
        moduleInfo += '@ ';
      }
      moduleInfo += 'L' + line + ':C' + column;
    }

    if (moduleInfo) {
      moduleInfo = '(' + moduleInfo + ') ';
    }

    return moduleInfo;
  }
});
enifed('ember-template-compiler/system/compile', ['exports', 'ember-metal/features', 'require', 'ember-template-compiler/system/compile_options', 'ember-template-compiler/system/template'], function (exports, _emberMetalFeatures, _require, _emberTemplateCompilerSystemCompile_options, _emberTemplateCompilerSystemTemplate) {
  /**
  @module ember
  @submodule ember-template-compiler
  */

  'use strict';

  var compile;

  /**
    Uses HTMLBars `compile` function to process a string into a compiled template.
  
    This is not present in production builds.
  
    @private
    @method compile
    @param {String} templateString This is the string to be compiled by HTMLBars.
    @param {Object} options This is an options hash to augment the compiler options.
  */

  exports.default = function (templateString, options) {
    if (!compile && _require.has('htmlbars-compiler/compiler')) {
      compile = _require.default('htmlbars-compiler/compiler').compile;
    }

    if (!compile) {
      throw new Error('Cannot call `compile` without the template compiler loaded. Please load `ember-template-compiler.js` prior to calling `compile`.');
    }

    var templateSpec = compile(templateString, _emberTemplateCompilerSystemCompile_options.default(options));

    return _emberTemplateCompilerSystemTemplate.default(templateSpec);
  };
});
enifed('ember-template-compiler/system/compile_options', ['exports', 'ember-metal/assign', 'ember-template-compiler/plugins'], function (exports, _emberMetalAssign, _emberTemplateCompilerPlugins) {
  /**
  @module ember
  @submodule ember-template-compiler
  */

  'use strict';

  var compileOptions = undefined;
  var fragmentReason = undefined;

  /**
    @private
    @property compileOptions
  */
  compileOptions = function (_options) {
    var disableComponentGeneration = true;

    var options = undefined;
    // When calling `Ember.Handlebars.compile()` a second argument of `true`
    // had a special meaning (long since lost), this just gaurds against
    // `options` being true, and causing an error during compilation.
    if (_options === true) {
      options = {};
    } else {
      options = _emberMetalAssign.default({}, _options);
    }

    options.disableComponentGeneration = disableComponentGeneration;

    var plugins = {
      ast: _emberTemplateCompilerPlugins.default.ast.slice()
    };

    if (options.plugins && options.plugins.ast) {
      plugins.ast = plugins.ast.concat(options.plugins.ast);
    }
    options.plugins = plugins;

    options.buildMeta = function buildMeta(program) {
      return {
        fragmentReason: fragmentReason(program),
        revision: 'Ember@2.6.2',
        loc: program.loc,
        moduleName: options.moduleName
      };
    };

    return options;
  };

  fragmentReason = function (program) {
    var loc = program.loc;
    var body = program.body;

    if (!loc || loc.start.line !== 1 || loc.start.column !== 0) {
      return false;
    }

    var candidate = undefined;
    var nodeCount = 0;

    var problems = {};

    for (var i = 0, l = body.length; i < l; i++) {
      var curr = body[i];

      // text node with whitespace only
      if (curr.type === 'TextNode' && /^[\s]*$/.test(curr.chars)) {
        continue;
      }

      // has multiple root elements if we've been here before
      if (nodeCount++ > 0) {
        problems['multiple-nodes'] = true;
      }

      if (curr.type === 'ComponentNode' || curr.type === 'ElementNode') {
        candidate = curr;
      } else {
        problems['wrong-type'] = true;
      }
    }

    if (nodeCount === 0) {
      return { name: 'missing-wrapper', problems: ['empty-body'] };
    }

    var problemList = Object.keys(problems);
    if (problemList.length) {
      return { name: 'missing-wrapper', problems: problemList };
    }

    if (candidate.type === 'ComponentNode') {
      return false;
    } else if (candidate.modifiers.length) {
      return { name: 'modifiers', modifiers: candidate.modifiers.map(function (m) {
          return m.path.original;
        }) };
    } else if (candidate.attributes.some(function (attr) {
      return !attr.value.escaped;
    })) {
      return { name: 'triple-curlies' };
    } else {
      return false;
    }
  };

  exports.default = compileOptions;
});
enifed('ember-template-compiler/system/precompile', ['exports', 'ember-metal/features', 'require', 'ember-template-compiler/system/compile_options'], function (exports, _emberMetalFeatures, _require, _emberTemplateCompilerSystemCompile_options) {
  /**
  @module ember
  @submodule ember-template-compiler
  */

  'use strict';

  var compileSpec;

  /**
    Uses HTMLBars `compile` function to process a string into a compiled template string.
    The returned string must be passed through `Ember.HTMLBars.template`.
  
    This is not present in production builds.
  
    @private
    @method precompile
    @param {String} templateString This is the string to be compiled by HTMLBars.
  */

  exports.default = function (templateString, options) {
    if (!compileSpec && _require.has('htmlbars-compiler/compiler')) {
      compileSpec = _require.default('htmlbars-compiler/compiler').compileSpec;
    }

    if (!compileSpec) {
      throw new Error('Cannot call `compileSpec` without the template compiler loaded. Please load `ember-template-compiler.js` prior to calling `compileSpec`.');
    }

    return compileSpec(templateString, _emberTemplateCompilerSystemCompile_options.default(options));
  };
});
enifed('ember-template-compiler/system/template', ['exports', 'ember-metal/features', 'require'], function (exports, _emberMetalFeatures, _require2) {
  'use strict';

  /**
  @module ember
  @submodule ember-template-compiler
  */

  /**
    Augments the default precompiled output of an HTMLBars template with
    additional information needed by Ember.
  
    @private
    @method template
    @param {Function} templateSpec This is the compiled HTMLBars template spec.
  */

  var template = undefined;

  var _require = _require2.default('htmlbars-runtime/hooks');

  var wrap = _require.wrap;

  template = function (templateSpec) {
    if (!templateSpec.render) {
      templateSpec = wrap(templateSpec);
    }

    templateSpec.isTop = true;
    templateSpec.isMethod = false;

    return templateSpec;
  };
  exports.default = template;
});
enifed("htmlbars-compiler", ["exports", "htmlbars-compiler/compiler"], function (exports, _htmlbarsCompilerCompiler) {
  "use strict";

  exports.compile = _htmlbarsCompilerCompiler.compile;
  exports.compileSpec = _htmlbarsCompilerCompiler.compileSpec;
  exports.template = _htmlbarsCompilerCompiler.template;
});
enifed("htmlbars-compiler/compiler", ["exports", "htmlbars-syntax/parser", "htmlbars-compiler/template-compiler", "htmlbars-runtime/hooks", "htmlbars-runtime/render"], function (exports, _htmlbarsSyntaxParser, _htmlbarsCompilerTemplateCompiler, _htmlbarsRuntimeHooks, _htmlbarsRuntimeRender) {
  /*jshint evil:true*/
  "use strict";

  exports.compileSpec = compileSpec;
  exports.template = template;
  exports.compile = compile;

  /*
   * Compile a string into a template spec string. The template spec is a string
   * representation of a template. Usually, you would use compileSpec for
   * pre-compilation of a template on the server.
   *
   * Example usage:
   *
   *     var templateSpec = compileSpec("Howdy {{name}}");
   *     // This next step is basically what plain compile does
   *     var template = new Function("return " + templateSpec)();
   *
   * @method compileSpec
   * @param {String} string An HTMLBars template string
   * @return {TemplateSpec} A template spec string
   */

  function compileSpec(string, options) {
    var ast = _htmlbarsSyntaxParser.preprocess(string, options);
    var compiler = new _htmlbarsCompilerTemplateCompiler.default(options);
    var program = compiler.compile(ast);
    return program;
  }

  /*
   * @method template
   * @param {TemplateSpec} templateSpec A precompiled template
   * @return {Template} A template spec string
   */

  function template(templateSpec) {
    return new Function("return " + templateSpec)();
  }

  /*
   * Compile a string into a template rendering function
   *
   * Example usage:
   *
   *     // Template is the hydration portion of the compiled template
   *     var template = compile("Howdy {{name}}");
   *
   *     // Template accepts three arguments:
   *     //
   *     //   1. A context object
   *     //   2. An env object
   *     //   3. A contextualElement (optional, document.body is the default)
   *     //
   *     // The env object *must* have at least these two properties:
   *     //
   *     //   1. `hooks` - Basic hooks for rendering a template
   *     //   2. `dom` - An instance of DOMHelper
   *     //
   *     import {hooks} from 'htmlbars-runtime';
   *     import {DOMHelper} from 'morph';
   *     var context = {name: 'whatever'},
   *         env = {hooks: hooks, dom: new DOMHelper()},
   *         contextualElement = document.body;
   *     var domFragment = template(context, env, contextualElement);
   *
   * @method compile
   * @param {String} string An HTMLBars template string
   * @param {Object} options A set of options to provide to the compiler
   * @return {Template} A function for rendering the template
   */

  function compile(string, options) {
    return _htmlbarsRuntimeHooks.wrap(template(compileSpec(string, options)), _htmlbarsRuntimeRender.default);
  }
});
enifed("htmlbars-compiler/fragment-javascript-compiler", ["exports", "htmlbars-compiler/utils", "htmlbars-util/quoting"], function (exports, _htmlbarsCompilerUtils, _htmlbarsUtilQuoting) {
  "use strict";

  var svgNamespace = "http://www.w3.org/2000/svg",

  // http://www.w3.org/html/wg/drafts/html/master/syntax.html#html-integration-point
  svgHTMLIntegrationPoints = { 'foreignObject': true, 'desc': true, 'title': true };

  function FragmentJavaScriptCompiler() {
    this.source = [];
    this.depth = -1;
  }

  exports.default = FragmentJavaScriptCompiler;

  FragmentJavaScriptCompiler.prototype.compile = function (opcodes, options) {
    this.source.length = 0;
    this.depth = -1;
    this.indent = options && options.indent || "";
    this.namespaceFrameStack = [{ namespace: null, depth: null }];
    this.domNamespace = null;

    this.source.push('function buildFragment(dom) {\n');
    _htmlbarsCompilerUtils.processOpcodes(this, opcodes);
    this.source.push(this.indent + '}');

    return this.source.join('');
  };

  FragmentJavaScriptCompiler.prototype.createFragment = function () {
    var el = 'el' + ++this.depth;
    this.source.push(this.indent + '  var ' + el + ' = dom.createDocumentFragment();\n');
  };

  FragmentJavaScriptCompiler.prototype.createElement = function (tagName) {
    var el = 'el' + ++this.depth;
    if (tagName === 'svg') {
      this.pushNamespaceFrame({ namespace: svgNamespace, depth: this.depth });
    }
    this.ensureNamespace();
    this.source.push(this.indent + '  var ' + el + ' = dom.createElement(' + _htmlbarsUtilQuoting.string(tagName) + ');\n');
    if (svgHTMLIntegrationPoints[tagName]) {
      this.pushNamespaceFrame({ namespace: null, depth: this.depth });
    }
  };

  FragmentJavaScriptCompiler.prototype.createText = function (str) {
    var el = 'el' + ++this.depth;
    this.source.push(this.indent + '  var ' + el + ' = dom.createTextNode(' + _htmlbarsUtilQuoting.string(str) + ');\n');
  };

  FragmentJavaScriptCompiler.prototype.createComment = function (str) {
    var el = 'el' + ++this.depth;
    this.source.push(this.indent + '  var ' + el + ' = dom.createComment(' + _htmlbarsUtilQuoting.string(str) + ');\n');
  };

  FragmentJavaScriptCompiler.prototype.returnNode = function () {
    var el = 'el' + this.depth;
    this.source.push(this.indent + '  return ' + el + ';\n');
  };

  FragmentJavaScriptCompiler.prototype.setAttribute = function (name, value, namespace) {
    var el = 'el' + this.depth;
    if (namespace) {
      this.source.push(this.indent + '  dom.setAttributeNS(' + el + ',' + _htmlbarsUtilQuoting.string(namespace) + ',' + _htmlbarsUtilQuoting.string(name) + ',' + _htmlbarsUtilQuoting.string(value) + ');\n');
    } else {
      this.source.push(this.indent + '  dom.setAttribute(' + el + ',' + _htmlbarsUtilQuoting.string(name) + ',' + _htmlbarsUtilQuoting.string(value) + ');\n');
    }
  };

  FragmentJavaScriptCompiler.prototype.appendChild = function () {
    if (this.depth === this.getCurrentNamespaceFrame().depth) {
      this.popNamespaceFrame();
    }
    var child = 'el' + this.depth--;
    var el = 'el' + this.depth;
    this.source.push(this.indent + '  dom.appendChild(' + el + ', ' + child + ');\n');
  };

  FragmentJavaScriptCompiler.prototype.getCurrentNamespaceFrame = function () {
    return this.namespaceFrameStack[this.namespaceFrameStack.length - 1];
  };

  FragmentJavaScriptCompiler.prototype.pushNamespaceFrame = function (frame) {
    this.namespaceFrameStack.push(frame);
  };

  FragmentJavaScriptCompiler.prototype.popNamespaceFrame = function () {
    return this.namespaceFrameStack.pop();
  };

  FragmentJavaScriptCompiler.prototype.ensureNamespace = function () {
    var correctNamespace = this.getCurrentNamespaceFrame().namespace;
    if (this.domNamespace !== correctNamespace) {
      this.source.push(this.indent + '  dom.setNamespace(' + (correctNamespace ? _htmlbarsUtilQuoting.string(correctNamespace) : 'null') + ');\n');
      this.domNamespace = correctNamespace;
    }
  };
});
enifed("htmlbars-compiler/fragment-opcode-compiler", ["exports", "htmlbars-compiler/template-visitor", "htmlbars-compiler/utils", "htmlbars-util", "htmlbars-util/array-utils"], function (exports, _htmlbarsCompilerTemplateVisitor, _htmlbarsCompilerUtils, _htmlbarsUtil, _htmlbarsUtilArrayUtils) {
  "use strict";

  function FragmentOpcodeCompiler() {
    this.opcodes = [];
  }

  exports.default = FragmentOpcodeCompiler;

  FragmentOpcodeCompiler.prototype.compile = function (ast) {
    var templateVisitor = new _htmlbarsCompilerTemplateVisitor.default();
    templateVisitor.visit(ast);

    _htmlbarsCompilerUtils.processOpcodes(this, templateVisitor.actions);

    return this.opcodes;
  };

  FragmentOpcodeCompiler.prototype.opcode = function (type, params) {
    this.opcodes.push([type, params]);
  };

  FragmentOpcodeCompiler.prototype.text = function (text) {
    this.opcode('createText', [text.chars]);
    this.opcode('appendChild');
  };

  FragmentOpcodeCompiler.prototype.comment = function (comment) {
    this.opcode('createComment', [comment.value]);
    this.opcode('appendChild');
  };

  FragmentOpcodeCompiler.prototype.openElement = function (element) {
    this.opcode('createElement', [element.tag]);
    _htmlbarsUtilArrayUtils.forEach(element.attributes, this.attribute, this);
  };

  FragmentOpcodeCompiler.prototype.closeElement = function () {
    this.opcode('appendChild');
  };

  FragmentOpcodeCompiler.prototype.startProgram = function () {
    this.opcodes.length = 0;
    this.opcode('createFragment');
  };

  FragmentOpcodeCompiler.prototype.endProgram = function () {
    this.opcode('returnNode');
  };

  FragmentOpcodeCompiler.prototype.mustache = function () {
    this.pushMorphPlaceholderNode();
  };

  FragmentOpcodeCompiler.prototype.component = function () {
    this.pushMorphPlaceholderNode();
  };

  FragmentOpcodeCompiler.prototype.block = function () {
    this.pushMorphPlaceholderNode();
  };

  FragmentOpcodeCompiler.prototype.pushMorphPlaceholderNode = function () {
    this.opcode('createComment', [""]);
    this.opcode('appendChild');
  };

  FragmentOpcodeCompiler.prototype.attribute = function (attr) {
    if (attr.value.type === 'TextNode') {
      var namespace = _htmlbarsUtil.getAttrNamespace(attr.name);
      this.opcode('setAttribute', [attr.name, attr.value.chars, namespace]);
    }
  };

  FragmentOpcodeCompiler.prototype.setNamespace = function (namespace) {
    this.opcode('setNamespace', [namespace]);
  };
});
enifed("htmlbars-compiler/hydration-javascript-compiler", ["exports", "htmlbars-compiler/utils", "htmlbars-util/quoting"], function (exports, _htmlbarsCompilerUtils, _htmlbarsUtilQuoting) {
  "use strict";

  function HydrationJavaScriptCompiler() {
    this.stack = [];
    this.source = [];
    this.mustaches = [];
    this.parents = [['fragment']];
    this.parentCount = 0;
    this.morphs = [];
    this.fragmentProcessing = [];
    this.hooks = undefined;
  }

  exports.default = HydrationJavaScriptCompiler;

  var prototype = HydrationJavaScriptCompiler.prototype;

  prototype.compile = function (opcodes, options) {
    this.stack.length = 0;
    this.mustaches.length = 0;
    this.source.length = 0;
    this.parents.length = 1;
    this.parents[0] = ['fragment'];
    this.morphs.length = 0;
    this.fragmentProcessing.length = 0;
    this.parentCount = 0;
    this.indent = options && options.indent || "";
    this.hooks = {};
    this.hasOpenBoundary = false;
    this.hasCloseBoundary = false;
    this.statements = [];
    this.expressionStack = [];
    this.locals = [];
    this.hasOpenBoundary = false;
    this.hasCloseBoundary = false;

    _htmlbarsCompilerUtils.processOpcodes(this, opcodes);

    if (this.hasOpenBoundary) {
      this.source.unshift(this.indent + "  dom.insertBoundary(fragment, 0);\n");
    }

    if (this.hasCloseBoundary) {
      this.source.unshift(this.indent + "  dom.insertBoundary(fragment, null);\n");
    }

    var i, l;

    var indent = this.indent;

    var morphs;

    var result = {
      createMorphsProgram: '',
      hydrateMorphsProgram: '',
      fragmentProcessingProgram: '',
      statements: this.statements,
      locals: this.locals,
      hasMorphs: false
    };

    result.hydrateMorphsProgram = this.source.join('');

    if (this.morphs.length) {
      result.hasMorphs = true;
      morphs = indent + '  var morphs = new Array(' + this.morphs.length + ');\n';

      for (i = 0, l = this.morphs.length; i < l; ++i) {
        var morph = this.morphs[i];
        morphs += indent + '  morphs[' + i + '] = ' + morph + ';\n';
      }
    }

    if (this.fragmentProcessing.length) {
      var processing = "";
      for (i = 0, l = this.fragmentProcessing.length; i < l; ++i) {
        processing += this.indent + '  ' + this.fragmentProcessing[i] + '\n';
      }
      result.fragmentProcessingProgram = processing;
    }

    var createMorphsProgram;
    if (result.hasMorphs) {
      createMorphsProgram = 'function buildRenderNodes(dom, fragment, contextualElement) {\n' + result.fragmentProcessingProgram + morphs;

      if (this.hasOpenBoundary) {
        createMorphsProgram += indent + "  dom.insertBoundary(fragment, 0);\n";
      }

      if (this.hasCloseBoundary) {
        createMorphsProgram += indent + "  dom.insertBoundary(fragment, null);\n";
      }

      createMorphsProgram += indent + '  return morphs;\n' + indent + '}';
    } else {
      createMorphsProgram = 'function buildRenderNodes() { return []; }';
    }

    result.createMorphsProgram = createMorphsProgram;

    return result;
  };

  prototype.prepareArray = function (length) {
    var values = [];

    for (var i = 0; i < length; i++) {
      values.push(this.expressionStack.pop());
    }

    this.expressionStack.push(values);
  };

  prototype.prepareObject = function (size) {
    var pairs = [];

    for (var i = 0; i < size; i++) {
      pairs.push(this.expressionStack.pop(), this.expressionStack.pop());
    }

    this.expressionStack.push(pairs);
  };

  prototype.openBoundary = function () {
    this.hasOpenBoundary = true;
  };

  prototype.closeBoundary = function () {
    this.hasCloseBoundary = true;
  };

  prototype.pushLiteral = function (value) {
    this.expressionStack.push(value);
  };

  prototype.pushGetHook = function (path, meta) {
    this.expressionStack.push(['get', path, meta]);
  };

  prototype.pushSexprHook = function (meta) {
    this.expressionStack.push(['subexpr', this.expressionStack.pop(), this.expressionStack.pop(), this.expressionStack.pop(), meta]);
  };

  prototype.pushConcatHook = function () {
    this.expressionStack.push(['concat', this.expressionStack.pop()]);
  };

  prototype.printSetHook = function (name) {
    this.locals.push(name);
  };

  prototype.printBlockHook = function (templateId, inverseId, meta) {
    this.statements.push(['block', this.expressionStack.pop(), // path
    this.expressionStack.pop(), // params
    this.expressionStack.pop(), // hash
    templateId, inverseId, meta]);
  };

  prototype.printInlineHook = function (meta) {
    var path = this.expressionStack.pop();
    var params = this.expressionStack.pop();
    var hash = this.expressionStack.pop();

    this.statements.push(['inline', path, params, hash, meta]);
  };

  prototype.printContentHook = function (meta) {
    this.statements.push(['content', this.expressionStack.pop(), meta]);
  };

  prototype.printComponentHook = function (templateId) {
    this.statements.push(['component', this.expressionStack.pop(), // path
    this.expressionStack.pop(), // attrs
    templateId]);
  };

  prototype.printAttributeHook = function () {
    this.statements.push(['attribute', this.expressionStack.pop(), // name
    this.expressionStack.pop() // value;
    ]);
  };

  prototype.printElementHook = function (meta) {
    this.statements.push(['element', this.expressionStack.pop(), // path
    this.expressionStack.pop(), // params
    this.expressionStack.pop(), // hash
    meta]);
  };

  prototype.createMorph = function (morphNum, parentPath, startIndex, endIndex, escaped) {
    var isRoot = parentPath.length === 0;
    var parent = this.getParent();

    var morphMethod = escaped ? 'createMorphAt' : 'createUnsafeMorphAt';
    var morph = "dom." + morphMethod + "(" + parent + "," + (startIndex === null ? "-1" : startIndex) + "," + (endIndex === null ? "-1" : endIndex) + (isRoot ? ",contextualElement)" : ")");

    this.morphs[morphNum] = morph;
  };

  prototype.createAttrMorph = function (attrMorphNum, elementNum, name, escaped, namespace) {
    var morphMethod = escaped ? 'createAttrMorph' : 'createUnsafeAttrMorph';
    var morph = "dom." + morphMethod + "(element" + elementNum + ", '" + name + (namespace ? "', '" + namespace : '') + "')";
    this.morphs[attrMorphNum] = morph;
  };

  prototype.createElementMorph = function (morphNum, elementNum) {
    var morphMethod = 'createElementMorph';
    var morph = "dom." + morphMethod + "(element" + elementNum + ")";
    this.morphs[morphNum] = morph;
  };

  prototype.repairClonedNode = function (blankChildTextNodes, isElementChecked) {
    var parent = this.getParent(),
        processing = 'if (this.cachedFragment) { dom.repairClonedNode(' + parent + ',' + _htmlbarsUtilQuoting.array(blankChildTextNodes) + (isElementChecked ? ',true' : '') + '); }';
    this.fragmentProcessing.push(processing);
  };

  prototype.shareElement = function (elementNum) {
    var elementNodesName = "element" + elementNum;
    this.fragmentProcessing.push('var ' + elementNodesName + ' = ' + this.getParent() + ';');
    this.parents[this.parents.length - 1] = [elementNodesName];
  };

  prototype.consumeParent = function (i) {
    var newParent = this.lastParent().slice();
    newParent.push(i);

    this.parents.push(newParent);
  };

  prototype.popParent = function () {
    this.parents.pop();
  };

  prototype.getParent = function () {
    var last = this.lastParent().slice();
    var frag = last.shift();

    if (!last.length) {
      return frag;
    }

    return 'dom.childAt(' + frag + ', [' + last.join(', ') + '])';
  };

  prototype.lastParent = function () {
    return this.parents[this.parents.length - 1];
  };
});
enifed("htmlbars-compiler/hydration-opcode-compiler", ["exports", "htmlbars-compiler/template-visitor", "htmlbars-compiler/utils", "htmlbars-util", "htmlbars-util/array-utils", "htmlbars-syntax/utils"], function (exports, _htmlbarsCompilerTemplateVisitor, _htmlbarsCompilerUtils, _htmlbarsUtil, _htmlbarsUtilArrayUtils, _htmlbarsSyntaxUtils) {
  "use strict";

  function detectIsElementChecked(element) {
    for (var i = 0, len = element.attributes.length; i < len; i++) {
      if (element.attributes[i].name === 'checked') {
        return true;
      }
    }
    return false;
  }

  function HydrationOpcodeCompiler() {
    this.opcodes = [];
    this.paths = [];
    this.templateId = 0;
    this.currentDOMChildIndex = 0;
    this.morphs = [];
    this.morphNum = 0;
    this.element = null;
    this.elementNum = -1;
  }

  exports.default = HydrationOpcodeCompiler;

  HydrationOpcodeCompiler.prototype.compile = function (ast) {
    var templateVisitor = new _htmlbarsCompilerTemplateVisitor.default();
    templateVisitor.visit(ast);

    _htmlbarsCompilerUtils.processOpcodes(this, templateVisitor.actions);

    return this.opcodes;
  };

  HydrationOpcodeCompiler.prototype.accept = function (node) {
    this[node.type](node);
  };

  HydrationOpcodeCompiler.prototype.opcode = function (type) {
    var params = [].slice.call(arguments, 1);
    this.opcodes.push([type, params]);
  };

  HydrationOpcodeCompiler.prototype.startProgram = function (program, c, blankChildTextNodes) {
    this.opcodes.length = 0;
    this.paths.length = 0;
    this.morphs.length = 0;
    this.templateId = 0;
    this.currentDOMChildIndex = -1;
    this.morphNum = 0;

    var blockParams = program.blockParams || [];

    for (var i = 0; i < blockParams.length; i++) {
      this.opcode('printSetHook', blockParams[i], i);
    }

    if (blankChildTextNodes.length > 0) {
      this.opcode('repairClonedNode', blankChildTextNodes);
    }
  };

  HydrationOpcodeCompiler.prototype.insertBoundary = function (first) {
    this.opcode(first ? 'openBoundary' : 'closeBoundary');
  };

  HydrationOpcodeCompiler.prototype.endProgram = function () {
    distributeMorphs(this.morphs, this.opcodes);
  };

  HydrationOpcodeCompiler.prototype.text = function () {
    ++this.currentDOMChildIndex;
  };

  HydrationOpcodeCompiler.prototype.comment = function () {
    ++this.currentDOMChildIndex;
  };

  HydrationOpcodeCompiler.prototype.openElement = function (element, pos, len, mustacheCount, blankChildTextNodes) {
    distributeMorphs(this.morphs, this.opcodes);
    ++this.currentDOMChildIndex;

    this.element = this.currentDOMChildIndex;

    this.opcode('consumeParent', this.currentDOMChildIndex);

    // If our parent reference will be used more than once, cache its reference.
    if (mustacheCount > 1) {
      shareElement(this);
    }

    var isElementChecked = detectIsElementChecked(element);
    if (blankChildTextNodes.length > 0 || isElementChecked) {
      this.opcode('repairClonedNode', blankChildTextNodes, isElementChecked);
    }

    this.paths.push(this.currentDOMChildIndex);
    this.currentDOMChildIndex = -1;

    _htmlbarsUtilArrayUtils.forEach(element.attributes, this.attribute, this);
    _htmlbarsUtilArrayUtils.forEach(element.modifiers, this.elementModifier, this);
  };

  HydrationOpcodeCompiler.prototype.closeElement = function () {
    distributeMorphs(this.morphs, this.opcodes);
    this.opcode('popParent');
    this.currentDOMChildIndex = this.paths.pop();
  };

  HydrationOpcodeCompiler.prototype.mustache = function (mustache, childIndex, childCount) {
    this.pushMorphPlaceholderNode(childIndex, childCount);

    var opcode;

    if (_htmlbarsSyntaxUtils.isHelper(mustache)) {
      prepareHash(this, mustache.hash);
      prepareParams(this, mustache.params);
      preparePath(this, mustache.path);
      opcode = 'printInlineHook';
    } else {
      preparePath(this, mustache.path);
      opcode = 'printContentHook';
    }

    var morphNum = this.morphNum++;
    var start = this.currentDOMChildIndex;
    var end = this.currentDOMChildIndex;
    this.morphs.push([morphNum, this.paths.slice(), start, end, mustache.escaped]);

    this.opcode(opcode, meta(mustache));
  };

  function meta(node) {
    var loc = node.loc;
    if (!loc) {
      return [];
    }

    var source = loc.source;
    var start = loc.start;
    var end = loc.end;

    return ['loc', [source || null, [start.line, start.column], [end.line, end.column]]];
  }

  HydrationOpcodeCompiler.prototype.block = function (block, childIndex, childCount) {
    this.pushMorphPlaceholderNode(childIndex, childCount);

    prepareHash(this, block.hash);
    prepareParams(this, block.params);
    preparePath(this, block.path);

    var morphNum = this.morphNum++;
    var start = this.currentDOMChildIndex;
    var end = this.currentDOMChildIndex;
    this.morphs.push([morphNum, this.paths.slice(), start, end, true]);

    var templateId = this.templateId++;
    var inverseId = block.inverse === null ? null : this.templateId++;

    this.opcode('printBlockHook', templateId, inverseId, meta(block));
  };

  HydrationOpcodeCompiler.prototype.component = function (component, childIndex, childCount) {
    this.pushMorphPlaceholderNode(childIndex, childCount, component.isStatic);

    var program = component.program || {};
    var blockParams = program.blockParams || [];

    var attrs = component.attributes;
    for (var i = attrs.length - 1; i >= 0; i--) {
      var name = attrs[i].name;
      var value = attrs[i].value;

      // TODO: Introduce context specific AST nodes to avoid switching here.
      if (value.type === 'TextNode') {
        this.opcode('pushLiteral', value.chars);
      } else if (value.type === 'MustacheStatement') {
        this.accept(_htmlbarsSyntaxUtils.unwrapMustache(value));
      } else if (value.type === 'ConcatStatement') {
        prepareParams(this, value.parts);
        this.opcode('pushConcatHook', this.morphNum);
      }

      this.opcode('pushLiteral', name);
    }

    var morphNum = this.morphNum++;
    var start = this.currentDOMChildIndex;
    var end = this.currentDOMChildIndex;
    this.morphs.push([morphNum, this.paths.slice(), start, end, true]);

    this.opcode('prepareObject', attrs.length);
    this.opcode('pushLiteral', component.tag);
    this.opcode('printComponentHook', this.templateId++, blockParams.length, meta(component));
  };

  HydrationOpcodeCompiler.prototype.attribute = function (attr) {
    var value = attr.value;
    var escaped = true;
    var namespace = _htmlbarsUtil.getAttrNamespace(attr.name);

    // TODO: Introduce context specific AST nodes to avoid switching here.
    if (value.type === 'TextNode') {
      return;
    } else if (value.type === 'MustacheStatement') {
      escaped = value.escaped;
      this.accept(_htmlbarsSyntaxUtils.unwrapMustache(value));
    } else if (value.type === 'ConcatStatement') {
      prepareParams(this, value.parts);
      this.opcode('pushConcatHook', this.morphNum);
    }

    this.opcode('pushLiteral', attr.name);

    var attrMorphNum = this.morphNum++;

    if (this.element !== null) {
      shareElement(this);
    }

    this.opcode('createAttrMorph', attrMorphNum, this.elementNum, attr.name, escaped, namespace);
    this.opcode('printAttributeHook');
  };

  HydrationOpcodeCompiler.prototype.elementModifier = function (modifier) {
    prepareHash(this, modifier.hash);
    prepareParams(this, modifier.params);
    preparePath(this, modifier.path);

    // If we have a helper in a node, and this element has not been cached, cache it
    if (this.element !== null) {
      shareElement(this);
    }

    publishElementMorph(this);
    this.opcode('printElementHook', meta(modifier));
  };

  HydrationOpcodeCompiler.prototype.pushMorphPlaceholderNode = function (childIndex, childCount, skipBoundaryNodes) {
    if (!skipBoundaryNodes) {
      if (this.paths.length === 0) {
        if (childIndex === 0) {
          this.opcode('openBoundary');
        }
        if (childIndex === childCount - 1) {
          this.opcode('closeBoundary');
        }
      }
    }

    this.comment();
  };

  HydrationOpcodeCompiler.prototype.MustacheStatement = function (mustache) {
    prepareHash(this, mustache.hash);
    prepareParams(this, mustache.params);
    preparePath(this, mustache.path);
    this.opcode('pushSexprHook', meta(mustache));
  };

  HydrationOpcodeCompiler.prototype.SubExpression = function (sexpr) {
    prepareHash(this, sexpr.hash);
    prepareParams(this, sexpr.params);
    preparePath(this, sexpr.path);
    this.opcode('pushSexprHook', meta(sexpr));
  };

  HydrationOpcodeCompiler.prototype.PathExpression = function (path) {
    this.opcode('pushGetHook', path.original, meta(path));
  };

  HydrationOpcodeCompiler.prototype.StringLiteral = function (node) {
    this.opcode('pushLiteral', node.value);
  };

  HydrationOpcodeCompiler.prototype.BooleanLiteral = function (node) {
    this.opcode('pushLiteral', node.value);
  };

  HydrationOpcodeCompiler.prototype.NumberLiteral = function (node) {
    this.opcode('pushLiteral', node.value);
  };

  HydrationOpcodeCompiler.prototype.UndefinedLiteral = function (node) {
    this.opcode('pushLiteral', node.value);
  };

  HydrationOpcodeCompiler.prototype.NullLiteral = function (node) {
    this.opcode('pushLiteral', node.value);
  };

  function preparePath(compiler, path) {
    compiler.opcode('pushLiteral', path.original);
  }

  function prepareParams(compiler, params) {
    for (var i = params.length - 1; i >= 0; i--) {
      var param = params[i];
      compiler[param.type](param);
    }

    compiler.opcode('prepareArray', params.length);
  }

  function prepareHash(compiler, hash) {
    var pairs = hash.pairs;

    for (var i = pairs.length - 1; i >= 0; i--) {
      var key = pairs[i].key;
      var value = pairs[i].value;

      compiler[value.type](value);
      compiler.opcode('pushLiteral', key);
    }

    compiler.opcode('prepareObject', pairs.length);
  }

  function shareElement(compiler) {
    compiler.opcode('shareElement', ++compiler.elementNum);
    compiler.element = null; // Set element to null so we don't cache it twice
  }

  function publishElementMorph(compiler) {
    var morphNum = compiler.morphNum++;
    compiler.opcode('createElementMorph', morphNum, compiler.elementNum);
  }

  function distributeMorphs(morphs, opcodes) {
    if (morphs.length === 0) {
      return;
    }

    // Splice morphs after the most recent shareParent/consumeParent.
    var o;
    for (o = opcodes.length - 1; o >= 0; --o) {
      var opcode = opcodes[o][0];
      if (opcode === 'shareElement' || opcode === 'consumeParent' || opcode === 'popParent') {
        break;
      }
    }

    var spliceArgs = [o + 1, 0];
    for (var i = 0; i < morphs.length; ++i) {
      spliceArgs.push(['createMorph', morphs[i].slice()]);
    }
    opcodes.splice.apply(opcodes, spliceArgs);
    morphs.length = 0;
  }
});
enifed('htmlbars-compiler/template-compiler', ['exports', 'htmlbars-compiler/fragment-opcode-compiler', 'htmlbars-compiler/fragment-javascript-compiler', 'htmlbars-compiler/hydration-opcode-compiler', 'htmlbars-compiler/hydration-javascript-compiler', 'htmlbars-compiler/template-visitor', 'htmlbars-compiler/utils', 'htmlbars-util/quoting', 'htmlbars-util/array-utils'], function (exports, _htmlbarsCompilerFragmentOpcodeCompiler, _htmlbarsCompilerFragmentJavascriptCompiler, _htmlbarsCompilerHydrationOpcodeCompiler, _htmlbarsCompilerHydrationJavascriptCompiler, _htmlbarsCompilerTemplateVisitor, _htmlbarsCompilerUtils, _htmlbarsUtilQuoting, _htmlbarsUtilArrayUtils) {
  'use strict';

  function TemplateCompiler(options) {
    this.options = options || {};
    this.consumerBuildMeta = this.options.buildMeta || function () {};
    this.fragmentOpcodeCompiler = new _htmlbarsCompilerFragmentOpcodeCompiler.default();
    this.fragmentCompiler = new _htmlbarsCompilerFragmentJavascriptCompiler.default();
    this.hydrationOpcodeCompiler = new _htmlbarsCompilerHydrationOpcodeCompiler.default();
    this.hydrationCompiler = new _htmlbarsCompilerHydrationJavascriptCompiler.default();
    this.templates = [];
    this.childTemplates = [];
  }

  exports.default = TemplateCompiler;

  TemplateCompiler.prototype.compile = function (ast) {
    var templateVisitor = new _htmlbarsCompilerTemplateVisitor.default();
    templateVisitor.visit(ast);

    _htmlbarsCompilerUtils.processOpcodes(this, templateVisitor.actions);

    return this.templates.pop();
  };

  TemplateCompiler.prototype.startProgram = function (program, childTemplateCount, blankChildTextNodes) {
    this.fragmentOpcodeCompiler.startProgram(program, childTemplateCount, blankChildTextNodes);
    this.hydrationOpcodeCompiler.startProgram(program, childTemplateCount, blankChildTextNodes);

    this.childTemplates.length = 0;
    while (childTemplateCount--) {
      this.childTemplates.push(this.templates.pop());
    }
  };

  TemplateCompiler.prototype.insertBoundary = function (first) {
    this.hydrationOpcodeCompiler.insertBoundary(first);
  };

  TemplateCompiler.prototype.getChildTemplateVars = function (indent) {
    var vars = '';
    if (this.childTemplates) {
      for (var i = 0; i < this.childTemplates.length; i++) {
        vars += indent + 'var child' + i + ' = ' + this.childTemplates[i] + ';\n';
      }
    }
    return vars;
  };

  TemplateCompiler.prototype.getHydrationHooks = function (indent, hooks) {
    var hookVars = [];
    for (var hook in hooks) {
      hookVars.push(hook + ' = hooks.' + hook);
    }

    if (hookVars.length > 0) {
      return indent + 'var hooks = env.hooks, ' + hookVars.join(', ') + ';\n';
    } else {
      return '';
    }
  };

  TemplateCompiler.prototype.endProgram = function (program, programDepth) {
    this.fragmentOpcodeCompiler.endProgram(program);
    this.hydrationOpcodeCompiler.endProgram(program);

    var indent = _htmlbarsUtilQuoting.repeat("  ", programDepth);
    var options = {
      indent: indent + "    "
    };

    // function build(dom) { return fragment; }
    var fragmentProgram = this.fragmentCompiler.compile(this.fragmentOpcodeCompiler.opcodes, options);

    // function hydrate(fragment) { return mustaches; }
    var hydrationPrograms = this.hydrationCompiler.compile(this.hydrationOpcodeCompiler.opcodes, options);

    var blockParams = program.blockParams || [];

    var templateSignature = 'context, rootNode, env, options';
    if (blockParams.length > 0) {
      templateSignature += ', blockArguments';
    }

    var statements = _htmlbarsUtilArrayUtils.map(hydrationPrograms.statements, function (s) {
      return indent + '      ' + JSON.stringify(s);
    }).join(",\n");

    var locals = JSON.stringify(hydrationPrograms.locals);

    var templates = _htmlbarsUtilArrayUtils.map(this.childTemplates, function (_, index) {
      return 'child' + index;
    }).join(', ');

    var template = '(function() {\n' + this.getChildTemplateVars(indent + '  ') + indent + '  return {\n' + this.buildMeta(indent + '    ', program) + indent + '    isEmpty: ' + (program.body.length ? 'false' : 'true') + ',\n' + indent + '    arity: ' + blockParams.length + ',\n' + indent + '    cachedFragment: null,\n' + indent + '    hasRendered: false,\n' + indent + '    buildFragment: ' + fragmentProgram + ',\n' + indent + '    buildRenderNodes: ' + hydrationPrograms.createMorphsProgram + ',\n' + indent + '    statements: [\n' + statements + '\n' + indent + '    ],\n' + indent + '    locals: ' + locals + ',\n' + indent + '    templates: [' + templates + ']\n' + indent + '  };\n' + indent + '}())';

    this.templates.push(template);
  };

  TemplateCompiler.prototype.buildMeta = function (indent, program) {
    var meta = this.consumerBuildMeta(program) || {};

    var head = indent + 'meta: ';
    var stringMeta = JSON.stringify(meta, null, 2).replace(/\n/g, '\n' + indent);
    var tail = ',\n';

    return head + stringMeta + tail;
  };

  TemplateCompiler.prototype.openElement = function (element, i, l, r, c, b) {
    this.fragmentOpcodeCompiler.openElement(element, i, l, r, c, b);
    this.hydrationOpcodeCompiler.openElement(element, i, l, r, c, b);
  };

  TemplateCompiler.prototype.closeElement = function (element, i, l, r) {
    this.fragmentOpcodeCompiler.closeElement(element, i, l, r);
    this.hydrationOpcodeCompiler.closeElement(element, i, l, r);
  };

  TemplateCompiler.prototype.component = function (component, i, l, s) {
    this.fragmentOpcodeCompiler.component(component, i, l, s);
    this.hydrationOpcodeCompiler.component(component, i, l, s);
  };

  TemplateCompiler.prototype.block = function (block, i, l, s) {
    this.fragmentOpcodeCompiler.block(block, i, l, s);
    this.hydrationOpcodeCompiler.block(block, i, l, s);
  };

  TemplateCompiler.prototype.text = function (string, i, l, r) {
    this.fragmentOpcodeCompiler.text(string, i, l, r);
    this.hydrationOpcodeCompiler.text(string, i, l, r);
  };

  TemplateCompiler.prototype.comment = function (string, i, l, r) {
    this.fragmentOpcodeCompiler.comment(string, i, l, r);
    this.hydrationOpcodeCompiler.comment(string, i, l, r);
  };

  TemplateCompiler.prototype.mustache = function (mustache, i, l, s) {
    this.fragmentOpcodeCompiler.mustache(mustache, i, l, s);
    this.hydrationOpcodeCompiler.mustache(mustache, i, l, s);
  };

  TemplateCompiler.prototype.setNamespace = function (namespace) {
    this.fragmentOpcodeCompiler.setNamespace(namespace);
  };
});
enifed('htmlbars-compiler/template-visitor', ['exports'], function (exports) {
  'use strict';

  var push = Array.prototype.push;

  function Frame() {
    this.parentNode = null;
    this.children = null;
    this.childIndex = null;
    this.childCount = null;
    this.childTemplateCount = 0;
    this.mustacheCount = 0;
    this.actions = [];
  }

  /**
   * Takes in an AST and outputs a list of actions to be consumed
   * by a compiler. For example, the template
   *
   *     foo{{bar}}<div>baz</div>
   *
   * produces the actions
   *
   *     [['startProgram', [programNode, 0]],
   *      ['text', [textNode, 0, 3]],
   *      ['mustache', [mustacheNode, 1, 3]],
   *      ['openElement', [elementNode, 2, 3, 0]],
   *      ['text', [textNode, 0, 1]],
   *      ['closeElement', [elementNode, 2, 3],
   *      ['endProgram', [programNode]]]
   *
   * This visitor walks the AST depth first and backwards. As
   * a result the bottom-most child template will appear at the
   * top of the actions list whereas the root template will appear
   * at the bottom of the list. For example,
   *
   *     <div>{{#if}}foo{{else}}bar<b></b>{{/if}}</div>
   *
   * produces the actions
   *
   *     [['startProgram', [programNode, 0]],
   *      ['text', [textNode, 0, 2, 0]],
   *      ['openElement', [elementNode, 1, 2, 0]],
   *      ['closeElement', [elementNode, 1, 2]],
   *      ['endProgram', [programNode]],
   *      ['startProgram', [programNode, 0]],
   *      ['text', [textNode, 0, 1]],
   *      ['endProgram', [programNode]],
   *      ['startProgram', [programNode, 2]],
   *      ['openElement', [elementNode, 0, 1, 1]],
   *      ['block', [blockNode, 0, 1]],
   *      ['closeElement', [elementNode, 0, 1]],
   *      ['endProgram', [programNode]]]
   *
   * The state of the traversal is maintained by a stack of frames.
   * Whenever a node with children is entered (either a ProgramNode
   * or an ElementNode) a frame is pushed onto the stack. The frame
   * contains information about the state of the traversal of that
   * node. For example,
   *
   *   - index of the current child node being visited
   *   - the number of mustaches contained within its child nodes
   *   - the list of actions generated by its child nodes
   */

  function TemplateVisitor() {
    this.frameStack = [];
    this.actions = [];
    this.programDepth = -1;
  }

  // Traversal methods

  TemplateVisitor.prototype.visit = function (node) {
    this[node.type](node);
  };

  TemplateVisitor.prototype.Program = function (program) {
    this.programDepth++;

    var parentFrame = this.getCurrentFrame();
    var programFrame = this.pushFrame();

    programFrame.parentNode = program;
    programFrame.children = program.body;
    programFrame.childCount = program.body.length;
    programFrame.blankChildTextNodes = [];
    programFrame.actions.push(['endProgram', [program, this.programDepth]]);

    for (var i = program.body.length - 1; i >= 0; i--) {
      programFrame.childIndex = i;
      this.visit(program.body[i]);
    }

    programFrame.actions.push(['startProgram', [program, programFrame.childTemplateCount, programFrame.blankChildTextNodes.reverse()]]);
    this.popFrame();

    this.programDepth--;

    // Push the completed template into the global actions list
    if (parentFrame) {
      parentFrame.childTemplateCount++;
    }
    push.apply(this.actions, programFrame.actions.reverse());
  };

  TemplateVisitor.prototype.ElementNode = function (element) {
    var parentFrame = this.getCurrentFrame();
    var elementFrame = this.pushFrame();

    elementFrame.parentNode = element;
    elementFrame.children = element.children;
    elementFrame.childCount = element.children.length;
    elementFrame.mustacheCount += element.modifiers.length;
    elementFrame.blankChildTextNodes = [];

    var actionArgs = [element, parentFrame.childIndex, parentFrame.childCount];

    elementFrame.actions.push(['closeElement', actionArgs]);

    for (var i = element.attributes.length - 1; i >= 0; i--) {
      this.visit(element.attributes[i]);
    }

    for (i = element.children.length - 1; i >= 0; i--) {
      elementFrame.childIndex = i;
      this.visit(element.children[i]);
    }

    elementFrame.actions.push(['openElement', actionArgs.concat([elementFrame.mustacheCount, elementFrame.blankChildTextNodes.reverse()])]);
    this.popFrame();

    // Propagate the element's frame state to the parent frame
    if (elementFrame.mustacheCount > 0) {
      parentFrame.mustacheCount++;
    }
    parentFrame.childTemplateCount += elementFrame.childTemplateCount;
    push.apply(parentFrame.actions, elementFrame.actions);
  };

  TemplateVisitor.prototype.AttrNode = function (attr) {
    if (attr.value.type !== 'TextNode') {
      this.getCurrentFrame().mustacheCount++;
    }
  };

  TemplateVisitor.prototype.TextNode = function (text) {
    var frame = this.getCurrentFrame();
    if (text.chars === '') {
      frame.blankChildTextNodes.push(domIndexOf(frame.children, text));
    }
    frame.actions.push(['text', [text, frame.childIndex, frame.childCount]]);
  };

  TemplateVisitor.prototype.BlockStatement = function (node) {
    var frame = this.getCurrentFrame();

    frame.mustacheCount++;
    frame.actions.push(['block', [node, frame.childIndex, frame.childCount]]);

    if (node.inverse) {
      this.visit(node.inverse);
    }
    if (node.program) {
      this.visit(node.program);
    }
  };

  TemplateVisitor.prototype.ComponentNode = function (node) {
    var frame = this.getCurrentFrame();

    frame.mustacheCount++;
    frame.actions.push(['component', [node, frame.childIndex, frame.childCount]]);

    if (node.program) {
      this.visit(node.program);
    }
  };

  TemplateVisitor.prototype.PartialStatement = function (node) {
    var frame = this.getCurrentFrame();
    frame.mustacheCount++;
    frame.actions.push(['mustache', [node, frame.childIndex, frame.childCount]]);
  };

  TemplateVisitor.prototype.CommentStatement = function (text) {
    var frame = this.getCurrentFrame();
    frame.actions.push(['comment', [text, frame.childIndex, frame.childCount]]);
  };

  TemplateVisitor.prototype.MustacheStatement = function (mustache) {
    var frame = this.getCurrentFrame();
    frame.mustacheCount++;
    frame.actions.push(['mustache', [mustache, frame.childIndex, frame.childCount]]);
  };

  // Frame helpers

  TemplateVisitor.prototype.getCurrentFrame = function () {
    return this.frameStack[this.frameStack.length - 1];
  };

  TemplateVisitor.prototype.pushFrame = function () {
    var frame = new Frame();
    this.frameStack.push(frame);
    return frame;
  };

  TemplateVisitor.prototype.popFrame = function () {
    return this.frameStack.pop();
  };

  exports.default = TemplateVisitor;

  // Returns the index of `domNode` in the `nodes` array, skipping
  // over any nodes which do not represent DOM nodes.
  function domIndexOf(nodes, domNode) {
    var index = -1;

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];

      if (node.type !== 'TextNode' && node.type !== 'ElementNode') {
        continue;
      } else {
        index++;
      }

      if (node === domNode) {
        return index;
      }
    }

    return -1;
  }
});
enifed("htmlbars-compiler/utils", ["exports"], function (exports) {
  "use strict";

  exports.processOpcodes = processOpcodes;

  function processOpcodes(compiler, opcodes) {
    for (var i = 0, l = opcodes.length; i < l; i++) {
      var method = opcodes[i][0];
      var params = opcodes[i][1];
      if (params) {
        compiler[method].apply(compiler, params);
      } else {
        compiler[method].call(compiler);
      }
    }
  }
});
enifed('htmlbars-runtime', ['exports', 'htmlbars-runtime/hooks', 'htmlbars-runtime/render', 'htmlbars-util/morph-utils', 'htmlbars-util/template-utils'], function (exports, _htmlbarsRuntimeHooks, _htmlbarsRuntimeRender, _htmlbarsUtilMorphUtils, _htmlbarsUtilTemplateUtils) {
  'use strict';

  var internal = {
    blockFor: _htmlbarsUtilTemplateUtils.blockFor,
    manualElement: _htmlbarsRuntimeRender.manualElement,
    hostBlock: _htmlbarsRuntimeHooks.hostBlock,
    continueBlock: _htmlbarsRuntimeHooks.continueBlock,
    hostYieldWithShadowTemplate: _htmlbarsRuntimeHooks.hostYieldWithShadowTemplate,
    visitChildren: _htmlbarsUtilMorphUtils.visitChildren,
    validateChildMorphs: _htmlbarsUtilMorphUtils.validateChildMorphs,
    clearMorph: _htmlbarsUtilTemplateUtils.clearMorph
  };

  exports.hooks = _htmlbarsRuntimeHooks.default;
  exports.render = _htmlbarsRuntimeRender.default;
  exports.internal = internal;
});
enifed('htmlbars-runtime/expression-visitor', ['exports'], function (exports) {
  /**
    # Expression Nodes:
  
    These nodes are not directly responsible for any part of the DOM, but are
    eventually passed to a Statement Node.
  
    * get
    * subexpr
    * concat
  */

  'use strict';

  exports.acceptParams = acceptParams;
  exports.acceptHash = acceptHash;

  function acceptParams(nodes, env, scope) {
    var array = [];

    for (var i = 0, l = nodes.length; i < l; i++) {
      array.push(acceptExpression(nodes[i], env, scope).value);
    }

    return array;
  }

  function acceptHash(pairs, env, scope) {
    var object = {};

    for (var i = 0, l = pairs.length; i < l; i += 2) {
      var key = pairs[i];
      var value = pairs[i + 1];
      object[key] = acceptExpression(value, env, scope).value;
    }

    return object;
  }

  function acceptExpression(node, env, scope) {
    var ret = { value: null };

    // Primitive literals are unambiguously non-array representations of
    // themselves.
    if (typeof node !== 'object' || node === null) {
      ret.value = node;
    } else {
      ret.value = evaluateNode(node, env, scope);
    }

    return ret;
  }

  function evaluateNode(node, env, scope) {
    switch (node[0]) {
      // can be used by manualElement
      case 'value':
        return node[1];
      case 'get':
        return evaluateGet(node, env, scope);
      case 'subexpr':
        return evaluateSubexpr(node, env, scope);
      case 'concat':
        return evaluateConcat(node, env, scope);
    }
  }

  function evaluateGet(node, env, scope) {
    var path = node[1];

    return env.hooks.get(env, scope, path);
  }

  function evaluateSubexpr(node, env, scope) {
    var path = node[1];
    var rawParams = node[2];
    var rawHash = node[3];

    var params = acceptParams(rawParams, env, scope);
    var hash = acceptHash(rawHash, env, scope);

    return env.hooks.subexpr(env, scope, path, params, hash);
  }

  function evaluateConcat(node, env, scope) {
    var rawParts = node[1];

    var parts = acceptParams(rawParts, env, scope);

    return env.hooks.concat(env, parts);
  }
});
enifed("htmlbars-runtime/hooks", ["exports", "htmlbars-runtime/render", "morph-range/morph-list", "htmlbars-util/object-utils", "htmlbars-util/morph-utils", "htmlbars-util/template-utils"], function (exports, _htmlbarsRuntimeRender, _morphRangeMorphList, _htmlbarsUtilObjectUtils, _htmlbarsUtilMorphUtils, _htmlbarsUtilTemplateUtils) {
  "use strict";

  exports.wrap = wrap;
  exports.wrapForHelper = wrapForHelper;
  exports.createScope = createScope;
  exports.createFreshScope = createFreshScope;
  exports.bindShadowScope = bindShadowScope;
  exports.createChildScope = createChildScope;
  exports.bindSelf = bindSelf;
  exports.updateSelf = updateSelf;
  exports.bindLocal = bindLocal;
  exports.updateLocal = updateLocal;
  exports.bindBlock = bindBlock;
  exports.block = block;
  exports.continueBlock = continueBlock;
  exports.hostBlock = hostBlock;
  exports.handleRedirect = handleRedirect;
  exports.handleKeyword = handleKeyword;
  exports.linkRenderNode = linkRenderNode;
  exports.inline = inline;
  exports.keyword = keyword;
  exports.invokeHelper = invokeHelper;
  exports.classify = classify;
  exports.partial = partial;
  exports.range = range;
  exports.element = element;
  exports.attribute = attribute;
  exports.subexpr = subexpr;
  exports.get = get;
  exports.getRoot = getRoot;
  exports.getBlock = getBlock;
  exports.getChild = getChild;
  exports.getValue = getValue;
  exports.getCellOrValue = getCellOrValue;
  exports.component = component;
  exports.concat = concat;
  exports.hasHelper = hasHelper;
  exports.lookupHelper = lookupHelper;
  exports.bindScope = bindScope;
  exports.updateScope = updateScope;

  /**
    HTMLBars delegates the runtime behavior of a template to
    hooks provided by the host environment. These hooks explain
    the lexical environment of a Handlebars template, the internal
    representation of references, and the interaction between an
    HTMLBars template and the DOM it is managing.
  
    While HTMLBars host hooks have access to all of this internal
    machinery, templates and helpers have access to the abstraction
    provided by the host hooks.
  
    ## The Lexical Environment
  
    The default lexical environment of an HTMLBars template includes:
  
    * Any local variables, provided by *block arguments*
    * The current value of `self`
  
    ## Simple Nesting
  
    Let's look at a simple template with a nested block:
  
    ```hbs
    <h1>{{title}}</h1>
  
    {{#if author}}
      <p class="byline">{{author}}</p>
    {{/if}}
    ```
  
    In this case, the lexical environment at the top-level of the
    template does not change inside of the `if` block. This is
    achieved via an implementation of `if` that looks like this:
  
    ```js
    registerHelper('if', function(params) {
      if (!!params[0]) {
        return this.yield();
      }
    });
    ```
  
    A call to `this.yield` invokes the child template using the
    current lexical environment.
  
    ## Block Arguments
  
    It is possible for nested blocks to introduce new local
    variables:
  
    ```hbs
    {{#count-calls as |i|}}
    <h1>{{title}}</h1>
    <p>Called {{i}} times</p>
    {{/count}}
    ```
  
    In this example, the child block inherits its surrounding
    lexical environment, but augments it with a single new
    variable binding.
  
    The implementation of `count-calls` supplies the value of
    `i`, but does not otherwise alter the environment:
  
    ```js
    var count = 0;
    registerHelper('count-calls', function() {
      return this.yield([ ++count ]);
    });
    ```
  */

  function wrap(template) {
    if (template === null) {
      return null;
    }

    return {
      meta: template.meta,
      arity: template.arity,
      raw: template,
      render: function (self, env, options, blockArguments) {
        var scope = env.hooks.createFreshScope();

        var contextualElement = options && options.contextualElement;
        var renderOptions = new _htmlbarsRuntimeRender.RenderOptions(null, self, blockArguments, contextualElement);

        return _htmlbarsRuntimeRender.default(template, env, scope, renderOptions);
      }
    };
  }

  function wrapForHelper(template, env, scope, morph, renderState, visitor) {
    if (!template) {
      return {};
    }

    var yieldArgs = yieldTemplate(template, env, scope, morph, renderState, visitor);

    return {
      meta: template.meta,
      arity: template.arity,
      'yield': yieldArgs, // quoted since it's a reserved word, see issue #420
      yieldItem: yieldItem(template, env, scope, morph, renderState, visitor),
      raw: template,

      render: function (self, blockArguments) {
        yieldArgs(blockArguments, self);
      }
    };
  }

  // Called by a user-land helper to render a template.
  function yieldTemplate(template, env, parentScope, morph, renderState, visitor) {
    return function (blockArguments, self) {
      // Render state is used to track the progress of the helper (since it
      // may call into us multiple times). As the user-land helper calls
      // into library code, we track what needs to be cleaned up after the
      // helper has returned.
      //
      // Here, we remember that a template has been yielded and so we do not
      // need to remove the previous template. (If no template is yielded
      // this render by the helper, we assume nothing should be shown and
      // remove any previous rendered templates.)
      renderState.morphToClear = null;

      // In this conditional is true, it means that on the previous rendering pass
      // the helper yielded multiple items via `yieldItem()`, but this time they
      // are yielding a single template. In that case, we mark the morph list for
      // cleanup so it is removed from the DOM.
      if (morph.morphList) {
        _htmlbarsUtilTemplateUtils.clearMorphList(morph.morphList, morph, env);
        renderState.morphListToClear = null;
      }

      var scope = parentScope;

      if (morph.lastYielded && isStableTemplate(template, morph.lastYielded)) {
        return morph.lastResult.revalidateWith(env, undefined, self, blockArguments, visitor);
      }

      // Check to make sure that we actually **need** a new scope, and can't
      // share the parent scope. Note that we need to move this check into
      // a host hook, because the host's notion of scope may require a new
      // scope in more cases than the ones we can determine statically.
      if (self !== undefined || parentScope === null || template.arity) {
        scope = env.hooks.createChildScope(parentScope);
      }

      morph.lastYielded = { self: self, template: template, shadowTemplate: null };

      // Render the template that was selected by the helper
      var renderOptions = new _htmlbarsRuntimeRender.RenderOptions(morph, self, blockArguments);
      _htmlbarsRuntimeRender.default(template, env, scope, renderOptions);
    };
  }

  function yieldItem(template, env, parentScope, morph, renderState, visitor) {
    // Initialize state that tracks multiple items being
    // yielded in.
    var currentMorph = null;

    // Candidate morphs for deletion.
    var candidates = {};

    // Reuse existing MorphList if this is not a first-time
    // render.
    var morphList = morph.morphList;
    if (morphList) {
      currentMorph = morphList.firstChildMorph;
    }

    // Advances the currentMorph pointer to the morph in the previously-rendered
    // list that matches the yielded key. While doing so, it marks any morphs
    // that it advances past as candidates for deletion. Assuming those morphs
    // are not yielded in later, they will be removed in the prune step during
    // cleanup.
    // Note that this helper function assumes that the morph being seeked to is
    // guaranteed to exist in the previous MorphList; if this is called and the
    // morph does not exist, it will result in an infinite loop
    function advanceToKey(key) {
      var seek = currentMorph;

      while (seek.key !== key) {
        candidates[seek.key] = seek;
        seek = seek.nextMorph;
      }

      currentMorph = seek.nextMorph;
      return seek;
    }

    return function (_key, blockArguments, self) {
      if (typeof _key !== 'string') {
        throw new Error("You must provide a string key when calling `yieldItem`; you provided " + _key);
      }

      // At least one item has been yielded, so we do not wholesale
      // clear the last MorphList but instead apply a prune operation.
      renderState.morphListToClear = null;
      morph.lastYielded = null;

      var morphList, morphMap;

      if (!morph.morphList) {
        morph.morphList = new _morphRangeMorphList.default();
        morph.morphMap = {};
        morph.setMorphList(morph.morphList);
      }

      morphList = morph.morphList;
      morphMap = morph.morphMap;

      // A map of morphs that have been yielded in on this
      // rendering pass. Any morphs that do not make it into
      // this list will be pruned from the MorphList during the cleanup
      // process.
      var handledMorphs = renderState.handledMorphs;
      var key = undefined;

      if (_key in handledMorphs) {
        // In this branch we are dealing with a duplicate key. The strategy
        // is to take the original key and append a counter to it that is
        // incremented every time the key is reused. In order to greatly
        // reduce the chance of colliding with another valid key we also add
        // an extra string "--z8mS2hvDW0A--" to the new key.
        var collisions = renderState.collisions;
        if (collisions === undefined) {
          collisions = renderState.collisions = {};
        }
        var count = collisions[_key] | 0;
        collisions[_key] = ++count;

        key = _key + '--z8mS2hvDW0A--' + count;
      } else {
        key = _key;
      }

      if (currentMorph && currentMorph.key === key) {
        yieldTemplate(template, env, parentScope, currentMorph, renderState, visitor)(blockArguments, self);
        currentMorph = currentMorph.nextMorph;
        handledMorphs[key] = currentMorph;
      } else if (morphMap[key] !== undefined) {
        var foundMorph = morphMap[key];

        if (key in candidates) {
          // If we already saw this morph, move it forward to this position
          morphList.insertBeforeMorph(foundMorph, currentMorph);
        } else {
          // Otherwise, move the pointer forward to the existing morph for this key
          advanceToKey(key);
        }

        handledMorphs[foundMorph.key] = foundMorph;
        yieldTemplate(template, env, parentScope, foundMorph, renderState, visitor)(blockArguments, self);
      } else {
        var childMorph = _htmlbarsRuntimeRender.createChildMorph(env.dom, morph);
        childMorph.key = key;
        morphMap[key] = handledMorphs[key] = childMorph;
        morphList.insertBeforeMorph(childMorph, currentMorph);
        yieldTemplate(template, env, parentScope, childMorph, renderState, visitor)(blockArguments, self);
      }

      renderState.morphListToPrune = morphList;
      morph.childNodes = null;
    };
  }

  function isStableTemplate(template, lastYielded) {
    return !lastYielded.shadowTemplate && template === lastYielded.template;
  }
  function optionsFor(template, inverse, env, scope, morph, visitor) {
    // If there was a template yielded last time, set morphToClear so it will be cleared
    // if no template is yielded on this render.
    var morphToClear = morph.lastResult ? morph : null;
    var renderState = new _htmlbarsUtilTemplateUtils.RenderState(morphToClear, morph.morphList || null);

    return {
      templates: {
        template: wrapForHelper(template, env, scope, morph, renderState, visitor),
        inverse: wrapForHelper(inverse, env, scope, morph, renderState, visitor)
      },
      renderState: renderState
    };
  }

  function thisFor(options) {
    return {
      arity: options.template.arity,
      'yield': options.template.yield, // quoted since it's a reserved word, see issue #420
      yieldItem: options.template.yieldItem,
      yieldIn: options.template.yieldIn
    };
  }

  /**
    Host Hook: createScope
  
    @param {Scope?} parentScope
    @return Scope
  
    Corresponds to entering a new HTMLBars block.
  
    This hook is invoked when a block is entered with
    a new `self` or additional local variables.
  
    When invoked for a top-level template, the
    `parentScope` is `null`, and this hook should return
    a fresh Scope.
  
    When invoked for a child template, the `parentScope`
    is the scope for the parent environment.
  
    Note that the `Scope` is an opaque value that is
    passed to other host hooks. For example, the `get`
    hook uses the scope to retrieve a value for a given
    scope and variable name.
  */

  function createScope(env, parentScope) {
    if (parentScope) {
      return env.hooks.createChildScope(parentScope);
    } else {
      return env.hooks.createFreshScope();
    }
  }

  function createFreshScope() {
    // because `in` checks have unpredictable performance, keep a
    // separate dictionary to track whether a local was bound.
    // See `bindLocal` for more information.
    return { self: null, blocks: {}, locals: {}, localPresent: {} };
  }

  /**
    Host Hook: bindShadowScope
  
    @param {Scope?} parentScope
    @return Scope
  
    Corresponds to rendering a new template into an existing
    render tree, but with a new top-level lexical scope. This
    template is called the "shadow root".
  
    If a shadow template invokes `{{yield}}`, it will render
    the block provided to the shadow root in the original
    lexical scope.
  
    ```hbs
    {{!-- post template --}}
    <p>{{props.title}}</p>
    {{yield}}
  
    {{!-- blog template --}}
    {{#post title="Hello world"}}
      <p>by {{byline}}</p>
      <article>This is my first post</article>
    {{/post}}
  
    {{#post title="Goodbye world"}}
      <p>by {{byline}}</p>
      <article>This is my last post</article>
    {{/post}}
    ```
  
    ```js
    helpers.post = function(params, hash, options) {
      options.template.yieldIn(postTemplate, { props: hash });
    };
  
    blog.render({ byline: "Yehuda Katz" });
    ```
  
    Produces:
  
    ```html
    <p>Hello world</p>
    <p>by Yehuda Katz</p>
    <article>This is my first post</article>
  
    <p>Goodbye world</p>
    <p>by Yehuda Katz</p>
    <article>This is my last post</article>
    ```
  
    In short, `yieldIn` creates a new top-level scope for the
    provided template and renders it, making the original block
    available to `{{yield}}` in that template.
  */

  function bindShadowScope(env /*, parentScope, shadowScope */) {
    return env.hooks.createFreshScope();
  }

  function createChildScope(parent) {
    var scope = Object.create(parent);
    scope.locals = Object.create(parent.locals);
    scope.localPresent = Object.create(parent.localPresent);
    scope.blocks = Object.create(parent.blocks);
    return scope;
  }

  /**
    Host Hook: bindSelf
  
    @param {Scope} scope
    @param {any} self
  
    Corresponds to entering a template.
  
    This hook is invoked when the `self` value for a scope is ready to be bound.
  
    The host must ensure that child scopes reflect the change to the `self` in
    future calls to the `get` hook.
  */

  function bindSelf(env, scope, self) {
    scope.self = self;
  }

  function updateSelf(env, scope, self) {
    env.hooks.bindSelf(env, scope, self);
  }

  /**
    Host Hook: bindLocal
  
    @param {Environment} env
    @param {Scope} scope
    @param {String} name
    @param {any} value
  
    Corresponds to entering a template with block arguments.
  
    This hook is invoked when a local variable for a scope has been provided.
  
    The host must ensure that child scopes reflect the change in future calls
    to the `get` hook.
  */

  function bindLocal(env, scope, name, value) {
    scope.localPresent[name] = true;
    scope.locals[name] = value;
  }

  function updateLocal(env, scope, name, value) {
    env.hooks.bindLocal(env, scope, name, value);
  }

  /**
    Host Hook: bindBlock
  
    @param {Environment} env
    @param {Scope} scope
    @param {Function} block
  
    Corresponds to entering a shadow template that was invoked by a block helper with
    `yieldIn`.
  
    This hook is invoked with an opaque block that will be passed along
    to the shadow template, and inserted into the shadow template when
    `{{yield}}` is used. Optionally provide a non-default block name
    that can be targeted by `{{yield to=blockName}}`.
  */

  function bindBlock(env, scope, block) {
    var name = arguments.length <= 3 || arguments[3] === undefined ? 'default' : arguments[3];

    scope.blocks[name] = block;
  }

  /**
    Host Hook: block
  
    @param {RenderNode} renderNode
    @param {Environment} env
    @param {Scope} scope
    @param {String} path
    @param {Array} params
    @param {Object} hash
    @param {Block} block
    @param {Block} elseBlock
  
    Corresponds to:
  
    ```hbs
    {{#helper param1 param2 key1=val1 key2=val2}}
      {{!-- child template --}}
    {{/helper}}
    ```
  
    This host hook is a workhorse of the system. It is invoked
    whenever a block is encountered, and is responsible for
    resolving the helper to call, and then invoke it.
  
    The helper should be invoked with:
  
    - `{Array} params`: the parameters passed to the helper
      in the template.
    - `{Object} hash`: an object containing the keys and values passed
      in the hash position in the template.
  
    The values in `params` and `hash` will already be resolved
    through a previous call to the `get` host hook.
  
    The helper should be invoked with a `this` value that is
    an object with one field:
  
    `{Function} yield`: when invoked, this function executes the
    block with the current scope. It takes an optional array of
    block parameters. If block parameters are supplied, HTMLBars
    will invoke the `bindLocal` host hook to bind the supplied
    values to the block arguments provided by the template.
  
    In general, the default implementation of `block` should work
    for most host environments. It delegates to other host hooks
    where appropriate, and properly invokes the helper with the
    appropriate arguments.
  */

  function block(morph, env, scope, path, params, hash, template, inverse, visitor) {
    if (handleRedirect(morph, env, scope, path, params, hash, template, inverse, visitor)) {
      return;
    }

    continueBlock(morph, env, scope, path, params, hash, template, inverse, visitor);
  }

  function continueBlock(morph, env, scope, path, params, hash, template, inverse, visitor) {
    hostBlock(morph, env, scope, template, inverse, null, visitor, function (options) {
      var helper = env.hooks.lookupHelper(env, scope, path);
      return env.hooks.invokeHelper(morph, env, scope, visitor, params, hash, helper, options.templates, thisFor(options.templates));
    });
  }

  function hostBlock(morph, env, scope, template, inverse, shadowOptions, visitor, callback) {
    var options = optionsFor(template, inverse, env, scope, morph, visitor);
    _htmlbarsUtilTemplateUtils.renderAndCleanup(morph, env, options, shadowOptions, callback);
  }

  function handleRedirect(morph, env, scope, path, params, hash, template, inverse, visitor) {
    if (!path) {
      return false;
    }

    var redirect = env.hooks.classify(env, scope, path);
    if (redirect) {
      switch (redirect) {
        case 'component':
          env.hooks.component(morph, env, scope, path, params, hash, { default: template, inverse: inverse }, visitor);break;
        case 'inline':
          env.hooks.inline(morph, env, scope, path, params, hash, visitor);break;
        case 'block':
          env.hooks.block(morph, env, scope, path, params, hash, template, inverse, visitor);break;
        default:
          throw new Error("Internal HTMLBars redirection to " + redirect + " not supported");
      }
      return true;
    }

    if (handleKeyword(path, morph, env, scope, params, hash, template, inverse, visitor)) {
      return true;
    }

    return false;
  }

  function handleKeyword(path, morph, env, scope, params, hash, template, inverse, visitor) {
    var keyword = env.hooks.keywords[path];
    if (!keyword) {
      return false;
    }

    if (typeof keyword === 'function') {
      return keyword(morph, env, scope, params, hash, template, inverse, visitor);
    }

    if (keyword.willRender) {
      keyword.willRender(morph, env);
    }

    var lastState, newState;
    if (keyword.setupState) {
      lastState = _htmlbarsUtilObjectUtils.shallowCopy(morph.getState());
      newState = morph.setState(keyword.setupState(lastState, env, scope, params, hash));
    }

    if (keyword.childEnv) {
      // Build the child environment...
      env = keyword.childEnv(morph.getState(), env);

      // ..then save off the child env builder on the render node. If the render
      // node tree is re-rendered and this node is not dirty, the child env
      // builder will still be invoked so that child dirty render nodes still get
      // the correct child env.
      morph.buildChildEnv = keyword.childEnv;
    }

    var firstTime = !morph.rendered;

    if (keyword.isEmpty) {
      var isEmpty = keyword.isEmpty(morph.getState(), env, scope, params, hash);

      if (isEmpty) {
        if (!firstTime) {
          _htmlbarsUtilTemplateUtils.clearMorph(morph, env, false);
        }
        return true;
      }
    }

    if (firstTime) {
      if (keyword.render) {
        keyword.render(morph, env, scope, params, hash, template, inverse, visitor);
      }
      morph.rendered = true;
      return true;
    }

    var isStable;
    if (keyword.isStable) {
      isStable = keyword.isStable(lastState, newState);
    } else {
      isStable = stableState(lastState, newState);
    }

    if (isStable) {
      if (keyword.rerender) {
        var newEnv = keyword.rerender(morph, env, scope, params, hash, template, inverse, visitor);
        env = newEnv || env;
      }
      _htmlbarsUtilMorphUtils.validateChildMorphs(env, morph, visitor);
      return true;
    } else {
      _htmlbarsUtilTemplateUtils.clearMorph(morph, env, false);
    }

    // If the node is unstable, re-render from scratch
    if (keyword.render) {
      keyword.render(morph, env, scope, params, hash, template, inverse, visitor);
      morph.rendered = true;
      return true;
    }
  }

  function stableState(oldState, newState) {
    if (_htmlbarsUtilObjectUtils.keyLength(oldState) !== _htmlbarsUtilObjectUtils.keyLength(newState)) {
      return false;
    }

    for (var prop in oldState) {
      if (oldState[prop] !== newState[prop]) {
        return false;
      }
    }

    return true;
  }

  function linkRenderNode() /* morph, env, scope, params, hash */{
    return;
  }

  /**
    Host Hook: inline
  
    @param {RenderNode} renderNode
    @param {Environment} env
    @param {Scope} scope
    @param {String} path
    @param {Array} params
    @param {Hash} hash
  
    Corresponds to:
  
    ```hbs
    {{helper param1 param2 key1=val1 key2=val2}}
    ```
  
    This host hook is similar to the `block` host hook, but it
    invokes helpers that do not supply an attached block.
  
    Like the `block` hook, the helper should be invoked with:
  
    - `{Array} params`: the parameters passed to the helper
      in the template.
    - `{Object} hash`: an object containing the keys and values passed
      in the hash position in the template.
  
    The values in `params` and `hash` will already be resolved
    through a previous call to the `get` host hook.
  
    In general, the default implementation of `inline` should work
    for most host environments. It delegates to other host hooks
    where appropriate, and properly invokes the helper with the
    appropriate arguments.
  
    The default implementation of `inline` also makes `partial`
    a keyword. Instead of invoking a helper named `partial`,
    it invokes the `partial` host hook.
  */

  function inline(morph, env, scope, path, params, hash, visitor) {
    if (handleRedirect(morph, env, scope, path, params, hash, null, null, visitor)) {
      return;
    }

    var value = undefined,
        hasValue = undefined;
    if (morph.linkedResult) {
      value = env.hooks.getValue(morph.linkedResult);
      hasValue = true;
    } else {
      var options = optionsFor(null, null, env, scope, morph);

      var helper = env.hooks.lookupHelper(env, scope, path);
      var result = env.hooks.invokeHelper(morph, env, scope, visitor, params, hash, helper, options.templates, thisFor(options.templates));

      if (result && result.link) {
        morph.linkedResult = result.value;
        _htmlbarsUtilMorphUtils.linkParams(env, scope, morph, '@content-helper', [morph.linkedResult], null);
      }

      if (result && 'value' in result) {
        value = env.hooks.getValue(result.value);
        hasValue = true;
      }
    }

    if (hasValue) {
      if (morph.lastValue !== value) {
        morph.setContent(value);
      }
      morph.lastValue = value;
    }
  }

  function keyword(path, morph, env, scope, params, hash, template, inverse, visitor) {
    handleKeyword(path, morph, env, scope, params, hash, template, inverse, visitor);
  }

  function invokeHelper(morph, env, scope, visitor, _params, _hash, helper, templates, context) {
    var params = normalizeArray(env, _params);
    var hash = normalizeObject(env, _hash);
    return { value: helper.call(context, params, hash, templates) };
  }

  function normalizeArray(env, array) {
    var out = new Array(array.length);

    for (var i = 0, l = array.length; i < l; i++) {
      out[i] = env.hooks.getCellOrValue(array[i]);
    }

    return out;
  }

  function normalizeObject(env, object) {
    var out = {};

    for (var prop in object) {
      out[prop] = env.hooks.getCellOrValue(object[prop]);
    }

    return out;
  }

  function classify() /* env, scope, path */{
    return null;
  }

  var keywords = {
    partial: function (morph, env, scope, params) {
      var value = env.hooks.partial(morph, env, scope, params[0]);
      morph.setContent(value);
      return true;
    },

    // quoted since it's a reserved word, see issue #420
    'yield': function (morph, env, scope, params, hash, template, inverse, visitor) {
      // the current scope is provided purely for the creation of shadow
      // scopes; it should not be provided to user code.

      var to = env.hooks.getValue(hash.to) || 'default';
      var block = env.hooks.getBlock(scope, to);

      if (block) {
        block.invoke(env, params, hash.self, morph, scope, visitor);
      }
      return true;
    },

    hasBlock: function (morph, env, scope, params) {
      var name = env.hooks.getValue(params[0]) || 'default';
      return !!env.hooks.getBlock(scope, name);
    },

    hasBlockParams: function (morph, env, scope, params) {
      var name = env.hooks.getValue(params[0]) || 'default';
      var block = env.hooks.getBlock(scope, name);
      return !!(block && block.arity);
    }

  };

  exports.keywords = keywords;
  /**
    Host Hook: partial
  
    @param {RenderNode} renderNode
    @param {Environment} env
    @param {Scope} scope
    @param {String} path
  
    Corresponds to:
  
    ```hbs
    {{partial "location"}}
    ```
  
    This host hook is invoked by the default implementation of
    the `inline` hook. This makes `partial` a keyword in an
    HTMLBars environment using the default `inline` host hook.
  
    It is implemented as a host hook so that it can retrieve
    the named partial out of the `Environment`. Helpers, in
    contrast, only have access to the values passed in to them,
    and not to the ambient lexical environment.
  
    The host hook should invoke the referenced partial with
    the ambient `self`.
  */

  function partial(renderNode, env, scope, path) {
    var template = env.partials[path];
    return template.render(scope.self, env, {}).fragment;
  }

  /**
    Host hook: range
  
    @param {RenderNode} renderNode
    @param {Environment} env
    @param {Scope} scope
    @param {any} value
  
    Corresponds to:
  
    ```hbs
    {{content}}
    {{{unescaped}}}
    ```
  
    This hook is responsible for updating a render node
    that represents a range of content with a value.
  */

  function range(morph, env, scope, path, value, visitor) {
    if (handleRedirect(morph, env, scope, path, [], {}, null, null, visitor)) {
      return;
    }

    value = env.hooks.getValue(value);

    if (morph.lastValue !== value) {
      morph.setContent(value);
    }

    morph.lastValue = value;
  }

  /**
    Host hook: element
  
    @param {RenderNode} renderNode
    @param {Environment} env
    @param {Scope} scope
    @param {String} path
    @param {Array} params
    @param {Hash} hash
  
    Corresponds to:
  
    ```hbs
    <div {{bind-attr foo=bar}}></div>
    ```
  
    This hook is responsible for invoking a helper that
    modifies an element.
  
    Its purpose is largely legacy support for awkward
    idioms that became common when using the string-based
    Handlebars engine.
  
    Most of the uses of the `element` hook are expected
    to be superseded by component syntax and the
    `attribute` hook.
  */

  function element(morph, env, scope, path, params, hash, visitor) {
    if (handleRedirect(morph, env, scope, path, params, hash, null, null, visitor)) {
      return;
    }

    var helper = env.hooks.lookupHelper(env, scope, path);
    if (helper) {
      env.hooks.invokeHelper(null, env, scope, null, params, hash, helper, { element: morph.element });
    }
  }

  /**
    Host hook: attribute
  
    @param {RenderNode} renderNode
    @param {Environment} env
    @param {String} name
    @param {any} value
  
    Corresponds to:
  
    ```hbs
    <div foo={{bar}}></div>
    ```
  
    This hook is responsible for updating a render node
    that represents an element's attribute with a value.
  
    It receives the name of the attribute as well as an
    already-resolved value, and should update the render
    node with the value if appropriate.
  */

  function attribute(morph, env, scope, name, value) {
    value = env.hooks.getValue(value);

    if (morph.lastValue !== value) {
      morph.setContent(value);
    }

    morph.lastValue = value;
  }

  function subexpr(env, scope, helperName, params, hash) {
    var helper = env.hooks.lookupHelper(env, scope, helperName);
    var result = env.hooks.invokeHelper(null, env, scope, null, params, hash, helper, {});
    if (result && 'value' in result) {
      return env.hooks.getValue(result.value);
    }
  }

  /**
    Host Hook: get
  
    @param {Environment} env
    @param {Scope} scope
    @param {String} path
  
    Corresponds to:
  
    ```hbs
    {{foo.bar}}
      ^
  
    {{helper foo.bar key=value}}
             ^           ^
    ```
  
    This hook is the "leaf" hook of the system. It is used to
    resolve a path relative to the current scope.
  */

  function get(env, scope, path) {
    if (path === '') {
      return scope.self;
    }

    var keys = path.split('.');
    var value = env.hooks.getRoot(scope, keys[0])[0];

    for (var i = 1; i < keys.length; i++) {
      if (value) {
        value = env.hooks.getChild(value, keys[i]);
      } else {
        break;
      }
    }

    return value;
  }

  function getRoot(scope, key) {
    if (scope.localPresent[key]) {
      return [scope.locals[key]];
    } else if (scope.self) {
      return [scope.self[key]];
    } else {
      return [undefined];
    }
  }

  function getBlock(scope, key) {
    return scope.blocks[key];
  }

  function getChild(value, key) {
    return value[key];
  }

  function getValue(reference) {
    return reference;
  }

  function getCellOrValue(reference) {
    return reference;
  }

  function component(morph, env, scope, tagName, params, attrs, templates, visitor) {
    if (env.hooks.hasHelper(env, scope, tagName)) {
      return env.hooks.block(morph, env, scope, tagName, params, attrs, templates.default, templates.inverse, visitor);
    }

    componentFallback(morph, env, scope, tagName, attrs, templates.default);
  }

  function concat(env, params) {
    var value = "";
    for (var i = 0, l = params.length; i < l; i++) {
      value += env.hooks.getValue(params[i]);
    }
    return value;
  }

  function componentFallback(morph, env, scope, tagName, attrs, template) {
    var element = env.dom.createElement(tagName);
    for (var name in attrs) {
      element.setAttribute(name, env.hooks.getValue(attrs[name]));
    }
    var fragment = _htmlbarsRuntimeRender.default(template, env, scope, {}).fragment;
    element.appendChild(fragment);
    morph.setNode(element);
  }

  function hasHelper(env, scope, helperName) {
    return env.helpers[helperName] !== undefined;
  }

  function lookupHelper(env, scope, helperName) {
    return env.helpers[helperName];
  }

  function bindScope() /* env, scope */{
    // this function is used to handle host-specified extensions to scope
    // other than `self`, `locals` and `block`.
  }

  function updateScope(env, scope) {
    env.hooks.bindScope(env, scope);
  }

  exports.default = {
    // fundamental hooks that you will likely want to override
    bindLocal: bindLocal,
    bindSelf: bindSelf,
    bindScope: bindScope,
    classify: classify,
    component: component,
    concat: concat,
    createFreshScope: createFreshScope,
    getChild: getChild,
    getRoot: getRoot,
    getBlock: getBlock,
    getValue: getValue,
    getCellOrValue: getCellOrValue,
    keywords: keywords,
    linkRenderNode: linkRenderNode,
    partial: partial,
    subexpr: subexpr,

    // fundamental hooks with good default behavior
    bindBlock: bindBlock,
    bindShadowScope: bindShadowScope,
    updateLocal: updateLocal,
    updateSelf: updateSelf,
    updateScope: updateScope,
    createChildScope: createChildScope,
    hasHelper: hasHelper,
    lookupHelper: lookupHelper,
    invokeHelper: invokeHelper,
    cleanupRenderNode: null,
    destroyRenderNode: null,
    willCleanupTree: null,
    didCleanupTree: null,
    willRenderNode: null,
    didRenderNode: null,

    // derived hooks
    attribute: attribute,
    block: block,
    createScope: createScope,
    element: element,
    get: get,
    inline: inline,
    range: range,
    keyword: keyword
  };
});
enifed("htmlbars-runtime/morph", ["exports", "morph-range"], function (exports, _morphRange) {
  "use strict";

  var guid = 1;

  function HTMLBarsMorph(domHelper, contextualElement) {
    this.super$constructor(domHelper, contextualElement);

    this._state = undefined;
    this.ownerNode = null;
    this.isDirty = false;
    this.isSubtreeDirty = false;
    this.lastYielded = null;
    this.lastResult = null;
    this.lastValue = null;
    this.buildChildEnv = null;
    this.morphList = null;
    this.morphMap = null;
    this.key = null;
    this.linkedParams = null;
    this.linkedResult = null;
    this.childNodes = null;
    this.rendered = false;
    this.guid = "range" + guid++;
    this.seen = false;
  }

  HTMLBarsMorph.empty = function (domHelper, contextualElement) {
    var morph = new HTMLBarsMorph(domHelper, contextualElement);
    morph.clear();
    return morph;
  };

  HTMLBarsMorph.create = function (domHelper, contextualElement, node) {
    var morph = new HTMLBarsMorph(domHelper, contextualElement);
    morph.setNode(node);
    return morph;
  };

  HTMLBarsMorph.attach = function (domHelper, contextualElement, firstNode, lastNode) {
    var morph = new HTMLBarsMorph(domHelper, contextualElement);
    morph.setRange(firstNode, lastNode);
    return morph;
  };

  var prototype = HTMLBarsMorph.prototype = Object.create(_morphRange.default.prototype);
  prototype.constructor = HTMLBarsMorph;
  prototype.super$constructor = _morphRange.default;

  prototype.getState = function () {
    if (!this._state) {
      this._state = {};
    }

    return this._state;
  };

  prototype.setState = function (newState) {
    /*jshint -W093 */

    return this._state = newState;
  };

  exports.default = HTMLBarsMorph;
});
enifed("htmlbars-runtime/node-visitor", ["exports", "htmlbars-util/morph-utils", "htmlbars-runtime/expression-visitor"], function (exports, _htmlbarsUtilMorphUtils, _htmlbarsRuntimeExpressionVisitor) {
  "use strict";

  /**
    Node classification:
  
    # Primary Statement Nodes:
  
    These nodes are responsible for a render node that represents a morph-range.
  
    * block
    * inline
    * content
    * element
    * component
  
    # Leaf Statement Nodes:
  
    This node is responsible for a render node that represents a morph-attr.
  
    * attribute
  */

  function linkParamsAndHash(env, scope, morph, path, params, hash) {
    if (morph.linkedParams) {
      params = morph.linkedParams.params;
      hash = morph.linkedParams.hash;
    } else {
      params = params && _htmlbarsRuntimeExpressionVisitor.acceptParams(params, env, scope);
      hash = hash && _htmlbarsRuntimeExpressionVisitor.acceptHash(hash, env, scope);
    }

    _htmlbarsUtilMorphUtils.linkParams(env, scope, morph, path, params, hash);
    return [params, hash];
  }

  var AlwaysDirtyVisitor = {

    block: function (node, morph, env, scope, template, visitor) {
      var path = node[1];
      var params = node[2];
      var hash = node[3];
      var templateId = node[4];
      var inverseId = node[5];

      var paramsAndHash = linkParamsAndHash(env, scope, morph, path, params, hash);

      morph.isDirty = morph.isSubtreeDirty = false;
      env.hooks.block(morph, env, scope, path, paramsAndHash[0], paramsAndHash[1], templateId === null ? null : template.templates[templateId], inverseId === null ? null : template.templates[inverseId], visitor);
    },

    inline: function (node, morph, env, scope, visitor) {
      var path = node[1];
      var params = node[2];
      var hash = node[3];

      var paramsAndHash = linkParamsAndHash(env, scope, morph, path, params, hash);

      morph.isDirty = morph.isSubtreeDirty = false;
      env.hooks.inline(morph, env, scope, path, paramsAndHash[0], paramsAndHash[1], visitor);
    },

    content: function (node, morph, env, scope, visitor) {
      var path = node[1];

      morph.isDirty = morph.isSubtreeDirty = false;

      if (isHelper(env, scope, path)) {
        env.hooks.inline(morph, env, scope, path, [], {}, visitor);
        if (morph.linkedResult) {
          _htmlbarsUtilMorphUtils.linkParams(env, scope, morph, '@content-helper', [morph.linkedResult], null);
        }
        return;
      }

      var params = undefined;
      if (morph.linkedParams) {
        params = morph.linkedParams.params;
      } else {
        params = [env.hooks.get(env, scope, path)];
      }

      _htmlbarsUtilMorphUtils.linkParams(env, scope, morph, '@range', params, null);
      env.hooks.range(morph, env, scope, path, params[0], visitor);
    },

    element: function (node, morph, env, scope, visitor) {
      var path = node[1];
      var params = node[2];
      var hash = node[3];

      var paramsAndHash = linkParamsAndHash(env, scope, morph, path, params, hash);

      morph.isDirty = morph.isSubtreeDirty = false;
      env.hooks.element(morph, env, scope, path, paramsAndHash[0], paramsAndHash[1], visitor);
    },

    attribute: function (node, morph, env, scope) {
      var name = node[1];
      var value = node[2];

      var paramsAndHash = linkParamsAndHash(env, scope, morph, '@attribute', [value], null);

      morph.isDirty = morph.isSubtreeDirty = false;
      env.hooks.attribute(morph, env, scope, name, paramsAndHash[0][0]);
    },

    component: function (node, morph, env, scope, template, visitor) {
      var path = node[1];
      var attrs = node[2];
      var templateId = node[3];
      var inverseId = node[4];

      var paramsAndHash = linkParamsAndHash(env, scope, morph, path, [], attrs);
      var templates = {
        default: template.templates[templateId],
        inverse: template.templates[inverseId]
      };

      morph.isDirty = morph.isSubtreeDirty = false;
      env.hooks.component(morph, env, scope, path, paramsAndHash[0], paramsAndHash[1], templates, visitor);
    },

    attributes: function (node, morph, env, scope, parentMorph, visitor) {
      var template = node[1];

      env.hooks.attributes(morph, env, scope, template, parentMorph, visitor);
    }

  };

  exports.AlwaysDirtyVisitor = AlwaysDirtyVisitor;
  exports.default = {
    block: function (node, morph, env, scope, template, visitor) {
      dirtyCheck(env, morph, visitor, function (visitor) {
        AlwaysDirtyVisitor.block(node, morph, env, scope, template, visitor);
      });
    },

    inline: function (node, morph, env, scope, visitor) {
      dirtyCheck(env, morph, visitor, function (visitor) {
        AlwaysDirtyVisitor.inline(node, morph, env, scope, visitor);
      });
    },

    content: function (node, morph, env, scope, visitor) {
      dirtyCheck(env, morph, visitor, function (visitor) {
        AlwaysDirtyVisitor.content(node, morph, env, scope, visitor);
      });
    },

    element: function (node, morph, env, scope, template, visitor) {
      dirtyCheck(env, morph, visitor, function (visitor) {
        AlwaysDirtyVisitor.element(node, morph, env, scope, template, visitor);
      });
    },

    attribute: function (node, morph, env, scope, template) {
      dirtyCheck(env, morph, null, function () {
        AlwaysDirtyVisitor.attribute(node, morph, env, scope, template);
      });
    },

    component: function (node, morph, env, scope, template, visitor) {
      dirtyCheck(env, morph, visitor, function (visitor) {
        AlwaysDirtyVisitor.component(node, morph, env, scope, template, visitor);
      });
    },

    attributes: function (node, morph, env, scope, parentMorph, visitor) {
      AlwaysDirtyVisitor.attributes(node, morph, env, scope, parentMorph, visitor);
    }
  };

  function dirtyCheck(_env, morph, visitor, callback) {
    var isDirty = morph.isDirty;
    var isSubtreeDirty = morph.isSubtreeDirty;
    var env = _env;

    if (isSubtreeDirty) {
      visitor = AlwaysDirtyVisitor;
    }

    if (isDirty || isSubtreeDirty) {
      callback(visitor);
    } else {
      if (morph.buildChildEnv) {
        env = morph.buildChildEnv(morph.getState(), env);
      }
      _htmlbarsUtilMorphUtils.validateChildMorphs(env, morph, visitor);
    }
  }

  function isHelper(env, scope, path) {
    return env.hooks.keywords[path] !== undefined || env.hooks.hasHelper(env, scope, path);
  }
});
enifed("htmlbars-runtime/render", ["exports", "htmlbars-util/morph-utils", "htmlbars-runtime/node-visitor", "htmlbars-runtime/morph", "htmlbars-util/template-utils", "htmlbars-util/void-tag-names"], function (exports, _htmlbarsUtilMorphUtils, _htmlbarsRuntimeNodeVisitor, _htmlbarsRuntimeMorph, _htmlbarsUtilTemplateUtils, _htmlbarsUtilVoidTagNames) {
  "use strict";

  exports.default = render;
  exports.RenderOptions = RenderOptions;
  exports.manualElement = manualElement;
  exports.attachAttributes = attachAttributes;
  exports.createChildMorph = createChildMorph;
  exports.getCachedFragment = getCachedFragment;

  var svgNamespace = "http://www.w3.org/2000/svg";

  function render(template, env, scope, options) {
    var dom = env.dom;
    var contextualElement;

    if (options) {
      if (options.renderNode) {
        contextualElement = options.renderNode.contextualElement;
      } else if (options.contextualElement) {
        contextualElement = options.contextualElement;
      }
    }

    dom.detectNamespace(contextualElement);

    var renderResult = RenderResult.build(env, scope, template, options, contextualElement);
    renderResult.render();

    return renderResult;
  }

  function RenderOptions(renderNode, self, blockArguments, contextualElement) {
    this.renderNode = renderNode || null;
    this.self = self;
    this.blockArguments = blockArguments || null;
    this.contextualElement = contextualElement || null;
  }

  function RenderResult(env, scope, options, rootNode, ownerNode, nodes, fragment, template, shouldSetContent) {
    this.root = rootNode;
    this.fragment = fragment;

    this.nodes = nodes;
    this.template = template;
    this.statements = template.statements.slice();
    this.env = env;
    this.scope = scope;
    this.shouldSetContent = shouldSetContent;

    if (options.self !== undefined) {
      this.bindSelf(options.self);
    }
    if (options.blockArguments !== undefined) {
      this.bindLocals(options.blockArguments);
    }

    this.initializeNodes(ownerNode);
  }

  RenderResult.build = function (env, scope, template, options, contextualElement) {
    var dom = env.dom;
    var fragment = getCachedFragment(template, env);
    var nodes = template.buildRenderNodes(dom, fragment, contextualElement);

    var rootNode, ownerNode, shouldSetContent;

    if (options && options.renderNode) {
      rootNode = options.renderNode;
      ownerNode = rootNode.ownerNode;
      shouldSetContent = true;
    } else {
      rootNode = dom.createMorph(null, fragment.firstChild, fragment.lastChild, contextualElement);
      ownerNode = rootNode;
      rootNode.ownerNode = ownerNode;
      shouldSetContent = false;
    }

    if (rootNode.childNodes) {
      _htmlbarsUtilMorphUtils.visitChildren(rootNode.childNodes, function (node) {
        _htmlbarsUtilTemplateUtils.clearMorph(node, env, true);
      });
    }

    rootNode.childNodes = nodes;
    return new RenderResult(env, scope, options, rootNode, ownerNode, nodes, fragment, template, shouldSetContent);
  };

  function manualElement(tagName, attributes, _isEmpty) {
    var statements = [];

    for (var key in attributes) {
      if (typeof attributes[key] === 'string') {
        continue;
      }
      statements.push(["attribute", key, attributes[key]]);
    }

    var isEmpty = _isEmpty || _htmlbarsUtilVoidTagNames.default[tagName];

    if (!isEmpty) {
      statements.push(['content', 'yield']);
    }

    var template = {
      arity: 0,
      cachedFragment: null,
      hasRendered: false,
      buildFragment: function buildFragment(dom) {
        var el0 = dom.createDocumentFragment();
        if (tagName === 'svg') {
          dom.setNamespace(svgNamespace);
        }
        var el1 = dom.createElement(tagName);

        for (var key in attributes) {
          if (typeof attributes[key] !== 'string') {
            continue;
          }
          dom.setAttribute(el1, key, attributes[key]);
        }

        if (!isEmpty) {
          var el2 = dom.createComment("");
          dom.appendChild(el1, el2);
        }

        dom.appendChild(el0, el1);

        return el0;
      },
      buildRenderNodes: function buildRenderNodes(dom, fragment) {
        var element = dom.childAt(fragment, [0]);
        var morphs = [];

        for (var key in attributes) {
          if (typeof attributes[key] === 'string') {
            continue;
          }
          morphs.push(dom.createAttrMorph(element, key));
        }

        if (!isEmpty) {
          morphs.push(dom.createMorphAt(element, 0, 0));
        }

        return morphs;
      },
      statements: statements,
      locals: [],
      templates: []
    };

    return template;
  }

  function attachAttributes(attributes) {
    var statements = [];

    for (var key in attributes) {
      if (typeof attributes[key] === 'string') {
        continue;
      }
      statements.push(["attribute", key, attributes[key]]);
    }

    var template = {
      arity: 0,
      cachedFragment: null,
      hasRendered: false,
      buildFragment: function buildFragment(dom) {
        var el0 = this.element;
        if (el0.namespaceURI === "http://www.w3.org/2000/svg") {
          dom.setNamespace(svgNamespace);
        }
        for (var key in attributes) {
          if (typeof attributes[key] !== 'string') {
            continue;
          }
          dom.setAttribute(el0, key, attributes[key]);
        }

        return el0;
      },
      buildRenderNodes: function buildRenderNodes(dom) {
        var element = this.element;
        var morphs = [];

        for (var key in attributes) {
          if (typeof attributes[key] === 'string') {
            continue;
          }
          morphs.push(dom.createAttrMorph(element, key));
        }

        return morphs;
      },
      statements: statements,
      locals: [],
      templates: [],
      element: null
    };

    return template;
  }

  RenderResult.prototype.initializeNodes = function (ownerNode) {
    var childNodes = this.root.childNodes;

    for (var i = 0, l = childNodes.length; i < l; i++) {
      childNodes[i].ownerNode = ownerNode;
    }
  };

  RenderResult.prototype.render = function () {
    this.root.lastResult = this;
    this.root.rendered = true;
    this.populateNodes(_htmlbarsRuntimeNodeVisitor.AlwaysDirtyVisitor);

    if (this.shouldSetContent && this.root.setContent) {
      this.root.setContent(this.fragment);
    }
  };

  RenderResult.prototype.dirty = function () {
    _htmlbarsUtilMorphUtils.visitChildren([this.root], function (node) {
      node.isDirty = true;
    });
  };

  RenderResult.prototype.revalidate = function (env, self, blockArguments, scope) {
    this.revalidateWith(env, scope, self, blockArguments, _htmlbarsRuntimeNodeVisitor.default);
  };

  RenderResult.prototype.rerender = function (env, self, blockArguments, scope) {
    this.revalidateWith(env, scope, self, blockArguments, _htmlbarsRuntimeNodeVisitor.AlwaysDirtyVisitor);
  };

  RenderResult.prototype.revalidateWith = function (env, scope, self, blockArguments, visitor) {
    if (env !== undefined) {
      this.env = env;
    }
    if (scope !== undefined) {
      this.scope = scope;
    }
    this.updateScope();

    if (self !== undefined) {
      this.updateSelf(self);
    }
    if (blockArguments !== undefined) {
      this.updateLocals(blockArguments);
    }

    this.populateNodes(visitor);
  };

  RenderResult.prototype.destroy = function () {
    var rootNode = this.root;
    _htmlbarsUtilTemplateUtils.clearMorph(rootNode, this.env, true);
  };

  RenderResult.prototype.populateNodes = function (visitor) {
    var env = this.env;
    var scope = this.scope;
    var template = this.template;
    var nodes = this.nodes;
    var statements = this.statements;
    var i, l;

    for (i = 0, l = statements.length; i < l; i++) {
      var statement = statements[i];
      var morph = nodes[i];

      if (env.hooks.willRenderNode) {
        env.hooks.willRenderNode(morph, env, scope);
      }

      switch (statement[0]) {
        case 'block':
          visitor.block(statement, morph, env, scope, template, visitor);break;
        case 'inline':
          visitor.inline(statement, morph, env, scope, visitor);break;
        case 'content':
          visitor.content(statement, morph, env, scope, visitor);break;
        case 'element':
          visitor.element(statement, morph, env, scope, template, visitor);break;
        case 'attribute':
          visitor.attribute(statement, morph, env, scope);break;
        case 'component':
          visitor.component(statement, morph, env, scope, template, visitor);break;
      }

      if (env.hooks.didRenderNode) {
        env.hooks.didRenderNode(morph, env, scope);
      }
    }
  };

  RenderResult.prototype.bindScope = function () {
    this.env.hooks.bindScope(this.env, this.scope);
  };

  RenderResult.prototype.updateScope = function () {
    this.env.hooks.updateScope(this.env, this.scope);
  };

  RenderResult.prototype.bindSelf = function (self) {
    this.env.hooks.bindSelf(this.env, this.scope, self);
  };

  RenderResult.prototype.updateSelf = function (self) {
    this.env.hooks.updateSelf(this.env, this.scope, self);
  };

  RenderResult.prototype.bindLocals = function (blockArguments) {
    var localNames = this.template.locals;

    for (var i = 0, l = localNames.length; i < l; i++) {
      this.env.hooks.bindLocal(this.env, this.scope, localNames[i], blockArguments[i]);
    }
  };

  RenderResult.prototype.updateLocals = function (blockArguments) {
    var localNames = this.template.locals;

    for (var i = 0, l = localNames.length; i < l; i++) {
      this.env.hooks.updateLocal(this.env, this.scope, localNames[i], blockArguments[i]);
    }
  };

  function initializeNode(node, owner) {
    node.ownerNode = owner;
  }

  function createChildMorph(dom, parentMorph, contextualElement) {
    var morph = _htmlbarsRuntimeMorph.default.empty(dom, contextualElement || parentMorph.contextualElement);
    initializeNode(morph, parentMorph.ownerNode);
    return morph;
  }

  function getCachedFragment(template, env) {
    var dom = env.dom,
        fragment;
    if (env.useFragmentCache && dom.canClone) {
      if (template.cachedFragment === null) {
        fragment = template.buildFragment(dom);
        if (template.hasRendered) {
          template.cachedFragment = fragment;
        } else {
          template.hasRendered = true;
        }
      }
      if (template.cachedFragment) {
        fragment = dom.cloneNode(template.cachedFragment, true);
      }
    } else if (!fragment) {
      fragment = template.buildFragment(dom);
    }

    return fragment;
  }
});
enifed("htmlbars-syntax", ["exports", "htmlbars-syntax/builders", "htmlbars-syntax/parser", "htmlbars-syntax/generation/print", "htmlbars-syntax/traversal/traverse", "htmlbars-syntax/traversal/walker"], function (exports, _htmlbarsSyntaxBuilders, _htmlbarsSyntaxParser, _htmlbarsSyntaxGenerationPrint, _htmlbarsSyntaxTraversalTraverse, _htmlbarsSyntaxTraversalWalker) {
  "use strict";

  exports.builders = _htmlbarsSyntaxBuilders.default;
  exports.parse = _htmlbarsSyntaxParser.default;
  exports.print = _htmlbarsSyntaxGenerationPrint.default;
  exports.traverse = _htmlbarsSyntaxTraversalTraverse.default;
  exports.Walker = _htmlbarsSyntaxTraversalWalker.default;
});
enifed("htmlbars-syntax/builders", ["exports"], function (exports) {
  // Statements

  "use strict";

  exports.buildMustache = buildMustache;
  exports.buildBlock = buildBlock;
  exports.buildElementModifier = buildElementModifier;
  exports.buildPartial = buildPartial;
  exports.buildComment = buildComment;
  exports.buildConcat = buildConcat;
  exports.buildElement = buildElement;
  exports.buildComponent = buildComponent;
  exports.buildAttr = buildAttr;
  exports.buildText = buildText;
  exports.buildSexpr = buildSexpr;
  exports.buildPath = buildPath;
  exports.buildString = buildString;
  exports.buildBoolean = buildBoolean;
  exports.buildNumber = buildNumber;
  exports.buildNull = buildNull;
  exports.buildUndefined = buildUndefined;
  exports.buildHash = buildHash;
  exports.buildPair = buildPair;
  exports.buildProgram = buildProgram;

  function buildMustache(path, params, hash, raw, loc) {
    return {
      type: "MustacheStatement",
      path: buildPath(path),
      params: params || [],
      hash: hash || buildHash([]),
      escaped: !raw,
      loc: buildLoc(loc)
    };
  }

  function buildBlock(path, params, hash, program, inverse, loc) {
    return {
      type: "BlockStatement",
      path: buildPath(path),
      params: params || [],
      hash: hash || buildHash([]),
      program: program || null,
      inverse: inverse || null,
      loc: buildLoc(loc)
    };
  }

  function buildElementModifier(path, params, hash, loc) {
    return {
      type: "ElementModifierStatement",
      path: buildPath(path),
      params: params || [],
      hash: hash || buildHash([]),
      loc: buildLoc(loc)
    };
  }

  function buildPartial(name, params, hash, indent) {
    return {
      type: "PartialStatement",
      name: name,
      params: params || [],
      hash: hash || buildHash([]),
      indent: indent
    };
  }

  function buildComment(value) {
    return {
      type: "CommentStatement",
      value: value
    };
  }

  function buildConcat(parts) {
    return {
      type: "ConcatStatement",
      parts: parts || []
    };
  }

  // Nodes

  function buildElement(tag, attributes, modifiers, children, loc) {
    return {
      type: "ElementNode",
      tag: tag || "",
      attributes: attributes || [],
      modifiers: modifiers || [],
      children: children || [],
      loc: buildLoc(loc)
    };
  }

  function buildComponent(tag, attributes, program, loc) {
    return {
      type: "ComponentNode",
      tag: tag,
      attributes: attributes,
      program: program,
      loc: buildLoc(loc),

      // this should be true only if this component node is guaranteed
      // to produce start and end points that can never change after the
      // initial render, regardless of changes to dynamic inputs. If
      // a component represents a "fragment" (any number of top-level nodes),
      // this will usually not be true.
      isStatic: false
    };
  }

  function buildAttr(name, value) {
    return {
      type: "AttrNode",
      name: name,
      value: value
    };
  }

  function buildText(chars, loc) {
    return {
      type: "TextNode",
      chars: chars || "",
      loc: buildLoc(loc)
    };
  }

  // Expressions

  function buildSexpr(path, params, hash) {
    return {
      type: "SubExpression",
      path: buildPath(path),
      params: params || [],
      hash: hash || buildHash([])
    };
  }

  function buildPath(original) {
    if (typeof original === 'string') {
      return {
        type: "PathExpression",
        original: original,
        parts: original.split('.')
      };
    } else {
      return original;
    }
  }

  function buildString(value) {
    return {
      type: "StringLiteral",
      value: value,
      original: value
    };
  }

  function buildBoolean(value) {
    return {
      type: "BooleanLiteral",
      value: value,
      original: value
    };
  }

  function buildNumber(value) {
    return {
      type: "NumberLiteral",
      value: value,
      original: value
    };
  }

  function buildNull() {
    return {
      type: "NullLiteral",
      value: null,
      original: null
    };
  }

  function buildUndefined() {
    return {
      type: "UndefinedLiteral",
      value: undefined,
      original: undefined
    };
  }

  // Miscellaneous

  function buildHash(pairs) {
    return {
      type: "Hash",
      pairs: pairs || []
    };
  }

  function buildPair(key, value) {
    return {
      type: "HashPair",
      key: key,
      value: value
    };
  }

  function buildProgram(body, blockParams, loc) {
    return {
      type: "Program",
      body: body || [],
      blockParams: blockParams || [],
      loc: buildLoc(loc)
    };
  }

  function buildSource(source) {
    return source || null;
  }

  function buildPosition(line, column) {
    return {
      line: typeof line === 'number' ? line : null,
      column: typeof column === 'number' ? column : null
    };
  }

  function buildLoc(startLine, startColumn, endLine, endColumn, source) {
    if (arguments.length === 1) {
      var loc = startLine;

      if (typeof loc === 'object') {
        return {
          source: buildSource(loc.source),
          start: buildPosition(loc.start.line, loc.start.column),
          end: buildPosition(loc.end.line, loc.end.column)
        };
      } else {
        return null;
      }
    } else {
      return {
        source: buildSource(source),
        start: buildPosition(startLine, startColumn),
        end: buildPosition(endLine, endColumn)
      };
    }
  }

  exports.default = {
    mustache: buildMustache,
    block: buildBlock,
    partial: buildPartial,
    comment: buildComment,
    element: buildElement,
    elementModifier: buildElementModifier,
    component: buildComponent,
    attr: buildAttr,
    text: buildText,
    sexpr: buildSexpr,
    path: buildPath,
    string: buildString,
    boolean: buildBoolean,
    number: buildNumber,
    undefined: buildUndefined,
    null: buildNull,
    concat: buildConcat,
    hash: buildHash,
    pair: buildPair,
    program: buildProgram,
    loc: buildLoc,
    pos: buildPosition
  };
});
enifed('htmlbars-syntax/generation/print', ['exports'], function (exports) {
  'use strict';

  exports.default = build;

  function build(ast) {
    if (!ast) {
      return '';
    }
    var output = [];

    switch (ast.type) {
      case 'Program':
        {
          var chainBlock = ast.chained && ast.body[0];
          if (chainBlock) {
            chainBlock.chained = true;
          }
          var body = buildEach(ast.body).join('');
          output.push(body);
        }
        break;
      case 'ElementNode':
        output.push('<', ast.tag);
        if (ast.attributes.length) {
          output.push(' ', buildEach(ast.attributes).join(' '));
        }
        if (ast.modifiers.length) {
          output.push(' ', buildEach(ast.modifiers).join(' '));
        }
        output.push('>');
        output.push.apply(output, buildEach(ast.children));
        output.push('</', ast.tag, '>');
        break;
      case 'AttrNode':
        output.push(ast.name, '=');
        var value = build(ast.value);
        if (ast.value.type === 'TextNode') {
          output.push('"', value, '"');
        } else {
          output.push(value);
        }
        break;
      case 'ConcatStatement':
        output.push('"');
        ast.parts.forEach(function (node) {
          if (node.type === 'StringLiteral') {
            output.push(node.original);
          } else {
            output.push(build(node));
          }
        });
        output.push('"');
        break;
      case 'TextNode':
        output.push(ast.chars);
        break;
      case 'MustacheStatement':
        {
          output.push(compactJoin(['{{', pathParams(ast), '}}']));
        }
        break;
      case 'ElementModifierStatement':
        {
          output.push(compactJoin(['{{', pathParams(ast), '}}']));
        }
        break;
      case 'PathExpression':
        output.push(ast.original);
        break;
      case 'SubExpression':
        {
          output.push('(', pathParams(ast), ')');
        }
        break;
      case 'BooleanLiteral':
        output.push(ast.value ? 'true' : false);
        break;
      case 'BlockStatement':
        {
          var lines = [];

          if (ast.chained) {
            lines.push(['{{else ', pathParams(ast), '}}'].join(''));
          } else {
            lines.push(openBlock(ast));
          }

          lines.push(build(ast.program));

          if (ast.inverse) {
            if (!ast.inverse.chained) {
              lines.push('{{else}}');
            }
            lines.push(build(ast.inverse));
          }

          if (!ast.chained) {
            lines.push(closeBlock(ast));
          }

          output.push(lines.join(''));
        }
        break;
      case 'PartialStatement':
        {
          output.push(compactJoin(['{{>', pathParams(ast), '}}']));
        }
        break;
      case 'CommentStatement':
        {
          output.push(compactJoin(['<!--', ast.value, '-->']));
        }
        break;
      case 'StringLiteral':
        {
          output.push('"' + ast.value + '"');
        }
        break;
      case 'NumberLiteral':
        {
          output.push(ast.value);
        }
        break;
      case 'UndefinedLiteral':
        {
          output.push('undefined');
        }
        break;
      case 'NullLiteral':
        {
          output.push('null');
        }
        break;
      case 'Hash':
        {
          output.push(ast.pairs.map(function (pair) {
            return build(pair);
          }).join(' '));
        }
        break;
      case 'HashPair':
        {
          output.push(ast.key + '=' + build(ast.value));
        }
        break;
    }
    return output.join('');
  }

  function compact(array) {
    var newArray = [];
    array.forEach(function (a) {
      if (typeof a !== 'undefined' && a !== null && a !== '') {
        newArray.push(a);
      }
    });
    return newArray;
  }

  function buildEach(asts) {
    var output = [];
    asts.forEach(function (node) {
      output.push(build(node));
    });
    return output;
  }

  function pathParams(ast) {
    var name = build(ast.name);
    var path = build(ast.path);
    var params = buildEach(ast.params).join(' ');
    var hash = build(ast.hash);
    return compactJoin([name, path, params, hash], ' ');
  }

  function compactJoin(array, delimiter) {
    return compact(array).join(delimiter || '');
  }

  function blockParams(block) {
    var params = block.program.blockParams;
    if (params.length) {
      return ' as |' + params.join(',') + '|';
    }
  }

  function openBlock(block) {
    return ['{{#', pathParams(block), blockParams(block), '}}'].join('');
  }

  function closeBlock(block) {
    return ['{{/', build(block.path), '}}'].join('');
  }
});
enifed('htmlbars-syntax/handlebars/compiler/ast', ['exports'], function (exports) {
  'use strict';

  var AST = {
    Program: function (statements, blockParams, strip, locInfo) {
      this.loc = locInfo;
      this.type = 'Program';
      this.body = statements;

      this.blockParams = blockParams;
      this.strip = strip;
    },

    MustacheStatement: function (path, params, hash, escaped, strip, locInfo) {
      this.loc = locInfo;
      this.type = 'MustacheStatement';

      this.path = path;
      this.params = params || [];
      this.hash = hash;
      this.escaped = escaped;

      this.strip = strip;
    },

    BlockStatement: function (path, params, hash, program, inverse, openStrip, inverseStrip, closeStrip, locInfo) {
      this.loc = locInfo;
      this.type = 'BlockStatement';

      this.path = path;
      this.params = params || [];
      this.hash = hash;
      this.program = program;
      this.inverse = inverse;

      this.openStrip = openStrip;
      this.inverseStrip = inverseStrip;
      this.closeStrip = closeStrip;
    },

    PartialStatement: function (name, params, hash, strip, locInfo) {
      this.loc = locInfo;
      this.type = 'PartialStatement';

      this.name = name;
      this.params = params || [];
      this.hash = hash;

      this.indent = '';
      this.strip = strip;
    },

    ContentStatement: function (string, locInfo) {
      this.loc = locInfo;
      this.type = 'ContentStatement';
      this.original = this.value = string;
    },

    CommentStatement: function (comment, strip, locInfo) {
      this.loc = locInfo;
      this.type = 'CommentStatement';
      this.value = comment;

      this.strip = strip;
    },

    SubExpression: function (path, params, hash, locInfo) {
      this.loc = locInfo;

      this.type = 'SubExpression';
      this.path = path;
      this.params = params || [];
      this.hash = hash;
    },

    PathExpression: function (data, depth, parts, original, locInfo) {
      this.loc = locInfo;
      this.type = 'PathExpression';

      this.data = data;
      this.original = original;
      this.parts = parts;
      this.depth = depth;
    },

    StringLiteral: function (string, locInfo) {
      this.loc = locInfo;
      this.type = 'StringLiteral';
      this.original = this.value = string;
    },

    NumberLiteral: function (number, locInfo) {
      this.loc = locInfo;
      this.type = 'NumberLiteral';
      this.original = this.value = Number(number);
    },

    BooleanLiteral: function (bool, locInfo) {
      this.loc = locInfo;
      this.type = 'BooleanLiteral';
      this.original = this.value = bool === 'true';
    },

    UndefinedLiteral: function (locInfo) {
      this.loc = locInfo;
      this.type = 'UndefinedLiteral';
      this.original = this.value = undefined;
    },

    NullLiteral: function (locInfo) {
      this.loc = locInfo;
      this.type = 'NullLiteral';
      this.original = this.value = null;
    },

    Hash: function (pairs, locInfo) {
      this.loc = locInfo;
      this.type = 'Hash';
      this.pairs = pairs;
    },
    HashPair: function (key, value, locInfo) {
      this.loc = locInfo;
      this.type = 'HashPair';
      this.key = key;
      this.value = value;
    },

    // Public API used to evaluate derived attributes regarding AST nodes
    helpers: {
      // a mustache is definitely a helper if:
      // * it is an eligible helper, and
      // * it has at least one parameter or hash segment
      helperExpression: function (node) {
        return !!(node.type === 'SubExpression' || node.params.length || node.hash);
      },

      scopedId: function (path) {
        return (/^\.|this\b/.test(path.original)
        );
      },

      // an ID is simple if it only has one part, and that part is not
      // `..` or `this`.
      simpleId: function (path) {
        return path.parts.length === 1 && !AST.helpers.scopedId(path) && !path.depth;
      }
    }
  };

  // Must be exported as an object rather than the root of the module as the jison lexer
  // must modify the object to operate properly.
  exports.default = AST;
});
enifed('htmlbars-syntax/handlebars/compiler/base', ['exports', 'htmlbars-syntax/handlebars/compiler/parser', 'htmlbars-syntax/handlebars/compiler/ast', 'htmlbars-syntax/handlebars/compiler/whitespace-control', 'htmlbars-syntax/handlebars/compiler/helpers', 'htmlbars-syntax/handlebars/utils'], function (exports, _htmlbarsSyntaxHandlebarsCompilerParser, _htmlbarsSyntaxHandlebarsCompilerAst, _htmlbarsSyntaxHandlebarsCompilerWhitespaceControl, _htmlbarsSyntaxHandlebarsCompilerHelpers, _htmlbarsSyntaxHandlebarsUtils) {
  'use strict';

  exports.parse = parse;
  exports.parser = _htmlbarsSyntaxHandlebarsCompilerParser.default;

  var yy = {};
  _htmlbarsSyntaxHandlebarsUtils.extend(yy, _htmlbarsSyntaxHandlebarsCompilerHelpers, _htmlbarsSyntaxHandlebarsCompilerAst.default);

  function parse(input, options) {
    // Just return if an already-compiled AST was passed in.
    if (input.type === 'Program') {
      return input;
    }

    _htmlbarsSyntaxHandlebarsCompilerParser.default.yy = yy;

    // Altering the shared object here, but this is ok as parser is a sync operation
    yy.locInfo = function (locInfo) {
      return new yy.SourceLocation(options && options.srcName, locInfo);
    };

    var strip = new _htmlbarsSyntaxHandlebarsCompilerWhitespaceControl.default();
    return strip.accept(_htmlbarsSyntaxHandlebarsCompilerParser.default.parse(input));
  }
});
enifed('htmlbars-syntax/handlebars/compiler/helpers', ['exports', 'htmlbars-syntax/handlebars/exception'], function (exports, _htmlbarsSyntaxHandlebarsException) {
  'use strict';

  exports.SourceLocation = SourceLocation;
  exports.id = id;
  exports.stripFlags = stripFlags;
  exports.stripComment = stripComment;
  exports.preparePath = preparePath;
  exports.prepareMustache = prepareMustache;
  exports.prepareRawBlock = prepareRawBlock;
  exports.prepareBlock = prepareBlock;

  function SourceLocation(source, locInfo) {
    this.source = source;
    this.start = {
      line: locInfo.first_line,
      column: locInfo.first_column
    };
    this.end = {
      line: locInfo.last_line,
      column: locInfo.last_column
    };
  }

  function id(token) {
    if (/^\[.*\]$/.test(token)) {
      return token.substr(1, token.length - 2);
    } else {
      return token;
    }
  }

  function stripFlags(open, close) {
    return {
      open: open.charAt(2) === '~',
      close: close.charAt(close.length - 3) === '~'
    };
  }

  function stripComment(comment) {
    return comment.replace(/^\{\{~?\!-?-?/, '').replace(/-?-?~?\}\}$/, '');
  }

  function preparePath(data, parts, locInfo) {
    locInfo = this.locInfo(locInfo);

    var original = data ? '@' : '',
        dig = [],
        depth = 0,
        depthString = '';

    for (var i = 0, l = parts.length; i < l; i++) {
      var part = parts[i].part,

      // If we have [] syntax then we do not treat path references as operators,
      // i.e. foo.[this] resolves to approximately context.foo['this']
      isLiteral = parts[i].original !== part;
      original += (parts[i].separator || '') + part;

      if (!isLiteral && (part === '..' || part === '.' || part === 'this')) {
        if (dig.length > 0) {
          throw new _htmlbarsSyntaxHandlebarsException.default('Invalid path: ' + original, { loc: locInfo });
        } else if (part === '..') {
          depth++;
          depthString += '../';
        }
      } else {
        dig.push(part);
      }
    }

    return new this.PathExpression(data, depth, dig, original, locInfo);
  }

  function prepareMustache(path, params, hash, open, strip, locInfo) {
    // Must use charAt to support IE pre-10
    var escapeFlag = open.charAt(3) || open.charAt(2),
        escaped = escapeFlag !== '{' && escapeFlag !== '&';

    return new this.MustacheStatement(path, params, hash, escaped, strip, this.locInfo(locInfo));
  }

  function prepareRawBlock(openRawBlock, content, close, locInfo) {
    if (openRawBlock.path.original !== close) {
      var errorNode = { loc: openRawBlock.path.loc };

      throw new _htmlbarsSyntaxHandlebarsException.default(openRawBlock.path.original + " doesn't match " + close, errorNode);
    }

    locInfo = this.locInfo(locInfo);
    var program = new this.Program([content], null, {}, locInfo);

    return new this.BlockStatement(openRawBlock.path, openRawBlock.params, openRawBlock.hash, program, undefined, {}, {}, {}, locInfo);
  }

  function prepareBlock(openBlock, program, inverseAndProgram, close, inverted, locInfo) {
    // When we are chaining inverse calls, we will not have a close path
    if (close && close.path && openBlock.path.original !== close.path.original) {
      var errorNode = { loc: openBlock.path.loc };

      throw new _htmlbarsSyntaxHandlebarsException.default(openBlock.path.original + ' doesn\'t match ' + close.path.original, errorNode);
    }

    program.blockParams = openBlock.blockParams;

    var inverse = undefined,
        inverseStrip = undefined;

    if (inverseAndProgram) {
      if (inverseAndProgram.chain) {
        inverseAndProgram.program.body[0].closeStrip = close.strip;
      }

      inverseStrip = inverseAndProgram.strip;
      inverse = inverseAndProgram.program;
    }

    if (inverted) {
      inverted = inverse;
      inverse = program;
      program = inverted;
    }

    return new this.BlockStatement(openBlock.path, openBlock.params, openBlock.hash, program, inverse, openBlock.strip, inverseStrip, close && close.strip, this.locInfo(locInfo));
  }
});
enifed("htmlbars-syntax/handlebars/compiler/parser", ["exports"], function (exports) {
    /* istanbul ignore next */
    /* Jison generated parser */
    "use strict";

    var handlebars = (function () {
        var parser = { trace: function trace() {},
            yy: {},
            symbols_: { "error": 2, "root": 3, "program": 4, "EOF": 5, "program_repetition0": 6, "statement": 7, "mustache": 8, "block": 9, "rawBlock": 10, "partial": 11, "content": 12, "COMMENT": 13, "CONTENT": 14, "openRawBlock": 15, "END_RAW_BLOCK": 16, "OPEN_RAW_BLOCK": 17, "helperName": 18, "openRawBlock_repetition0": 19, "openRawBlock_option0": 20, "CLOSE_RAW_BLOCK": 21, "openBlock": 22, "block_option0": 23, "closeBlock": 24, "openInverse": 25, "block_option1": 26, "OPEN_BLOCK": 27, "openBlock_repetition0": 28, "openBlock_option0": 29, "openBlock_option1": 30, "CLOSE": 31, "OPEN_INVERSE": 32, "openInverse_repetition0": 33, "openInverse_option0": 34, "openInverse_option1": 35, "openInverseChain": 36, "OPEN_INVERSE_CHAIN": 37, "openInverseChain_repetition0": 38, "openInverseChain_option0": 39, "openInverseChain_option1": 40, "inverseAndProgram": 41, "INVERSE": 42, "inverseChain": 43, "inverseChain_option0": 44, "OPEN_ENDBLOCK": 45, "OPEN": 46, "mustache_repetition0": 47, "mustache_option0": 48, "OPEN_UNESCAPED": 49, "mustache_repetition1": 50, "mustache_option1": 51, "CLOSE_UNESCAPED": 52, "OPEN_PARTIAL": 53, "partialName": 54, "partial_repetition0": 55, "partial_option0": 56, "param": 57, "sexpr": 58, "OPEN_SEXPR": 59, "sexpr_repetition0": 60, "sexpr_option0": 61, "CLOSE_SEXPR": 62, "hash": 63, "hash_repetition_plus0": 64, "hashSegment": 65, "ID": 66, "EQUALS": 67, "blockParams": 68, "OPEN_BLOCK_PARAMS": 69, "blockParams_repetition_plus0": 70, "CLOSE_BLOCK_PARAMS": 71, "path": 72, "dataName": 73, "STRING": 74, "NUMBER": 75, "BOOLEAN": 76, "UNDEFINED": 77, "NULL": 78, "DATA": 79, "pathSegments": 80, "SEP": 81, "$accept": 0, "$end": 1 },
            terminals_: { 2: "error", 5: "EOF", 13: "COMMENT", 14: "CONTENT", 16: "END_RAW_BLOCK", 17: "OPEN_RAW_BLOCK", 21: "CLOSE_RAW_BLOCK", 27: "OPEN_BLOCK", 31: "CLOSE", 32: "OPEN_INVERSE", 37: "OPEN_INVERSE_CHAIN", 42: "INVERSE", 45: "OPEN_ENDBLOCK", 46: "OPEN", 49: "OPEN_UNESCAPED", 52: "CLOSE_UNESCAPED", 53: "OPEN_PARTIAL", 59: "OPEN_SEXPR", 62: "CLOSE_SEXPR", 66: "ID", 67: "EQUALS", 69: "OPEN_BLOCK_PARAMS", 71: "CLOSE_BLOCK_PARAMS", 74: "STRING", 75: "NUMBER", 76: "BOOLEAN", 77: "UNDEFINED", 78: "NULL", 79: "DATA", 81: "SEP" },
            productions_: [0, [3, 2], [4, 1], [7, 1], [7, 1], [7, 1], [7, 1], [7, 1], [7, 1], [12, 1], [10, 3], [15, 5], [9, 4], [9, 4], [22, 6], [25, 6], [36, 6], [41, 2], [43, 3], [43, 1], [24, 3], [8, 5], [8, 5], [11, 5], [57, 1], [57, 1], [58, 5], [63, 1], [65, 3], [68, 3], [18, 1], [18, 1], [18, 1], [18, 1], [18, 1], [18, 1], [18, 1], [54, 1], [54, 1], [73, 2], [72, 1], [80, 3], [80, 1], [6, 0], [6, 2], [19, 0], [19, 2], [20, 0], [20, 1], [23, 0], [23, 1], [26, 0], [26, 1], [28, 0], [28, 2], [29, 0], [29, 1], [30, 0], [30, 1], [33, 0], [33, 2], [34, 0], [34, 1], [35, 0], [35, 1], [38, 0], [38, 2], [39, 0], [39, 1], [40, 0], [40, 1], [44, 0], [44, 1], [47, 0], [47, 2], [48, 0], [48, 1], [50, 0], [50, 2], [51, 0], [51, 1], [55, 0], [55, 2], [56, 0], [56, 1], [60, 0], [60, 2], [61, 0], [61, 1], [64, 1], [64, 2], [70, 1], [70, 2]],
            performAction: function anonymous(yytext, yyleng, yylineno, yy, yystate, $$, _$) {

                var $0 = $$.length - 1;
                switch (yystate) {
                    case 1:
                        return $$[$0 - 1];
                        break;
                    case 2:
                        this.$ = new yy.Program($$[$0], null, {}, yy.locInfo(this._$));
                        break;
                    case 3:
                        this.$ = $$[$0];
                        break;
                    case 4:
                        this.$ = $$[$0];
                        break;
                    case 5:
                        this.$ = $$[$0];
                        break;
                    case 6:
                        this.$ = $$[$0];
                        break;
                    case 7:
                        this.$ = $$[$0];
                        break;
                    case 8:
                        this.$ = new yy.CommentStatement(yy.stripComment($$[$0]), yy.stripFlags($$[$0], $$[$0]), yy.locInfo(this._$));
                        break;
                    case 9:
                        this.$ = new yy.ContentStatement($$[$0], yy.locInfo(this._$));
                        break;
                    case 10:
                        this.$ = yy.prepareRawBlock($$[$0 - 2], $$[$0 - 1], $$[$0], this._$);
                        break;
                    case 11:
                        this.$ = { path: $$[$0 - 3], params: $$[$0 - 2], hash: $$[$0 - 1] };
                        break;
                    case 12:
                        this.$ = yy.prepareBlock($$[$0 - 3], $$[$0 - 2], $$[$0 - 1], $$[$0], false, this._$);
                        break;
                    case 13:
                        this.$ = yy.prepareBlock($$[$0 - 3], $$[$0 - 2], $$[$0 - 1], $$[$0], true, this._$);
                        break;
                    case 14:
                        this.$ = { path: $$[$0 - 4], params: $$[$0 - 3], hash: $$[$0 - 2], blockParams: $$[$0 - 1], strip: yy.stripFlags($$[$0 - 5], $$[$0]) };
                        break;
                    case 15:
                        this.$ = { path: $$[$0 - 4], params: $$[$0 - 3], hash: $$[$0 - 2], blockParams: $$[$0 - 1], strip: yy.stripFlags($$[$0 - 5], $$[$0]) };
                        break;
                    case 16:
                        this.$ = { path: $$[$0 - 4], params: $$[$0 - 3], hash: $$[$0 - 2], blockParams: $$[$0 - 1], strip: yy.stripFlags($$[$0 - 5], $$[$0]) };
                        break;
                    case 17:
                        this.$ = { strip: yy.stripFlags($$[$0 - 1], $$[$0 - 1]), program: $$[$0] };
                        break;
                    case 18:
                        var inverse = yy.prepareBlock($$[$0 - 2], $$[$0 - 1], $$[$0], $$[$0], false, this._$),
                            program = new yy.Program([inverse], null, {}, yy.locInfo(this._$));
                        program.chained = true;

                        this.$ = { strip: $$[$0 - 2].strip, program: program, chain: true };

                        break;
                    case 19:
                        this.$ = $$[$0];
                        break;
                    case 20:
                        this.$ = { path: $$[$0 - 1], strip: yy.stripFlags($$[$0 - 2], $$[$0]) };
                        break;
                    case 21:
                        this.$ = yy.prepareMustache($$[$0 - 3], $$[$0 - 2], $$[$0 - 1], $$[$0 - 4], yy.stripFlags($$[$0 - 4], $$[$0]), this._$);
                        break;
                    case 22:
                        this.$ = yy.prepareMustache($$[$0 - 3], $$[$0 - 2], $$[$0 - 1], $$[$0 - 4], yy.stripFlags($$[$0 - 4], $$[$0]), this._$);
                        break;
                    case 23:
                        this.$ = new yy.PartialStatement($$[$0 - 3], $$[$0 - 2], $$[$0 - 1], yy.stripFlags($$[$0 - 4], $$[$0]), yy.locInfo(this._$));
                        break;
                    case 24:
                        this.$ = $$[$0];
                        break;
                    case 25:
                        this.$ = $$[$0];
                        break;
                    case 26:
                        this.$ = new yy.SubExpression($$[$0 - 3], $$[$0 - 2], $$[$0 - 1], yy.locInfo(this._$));
                        break;
                    case 27:
                        this.$ = new yy.Hash($$[$0], yy.locInfo(this._$));
                        break;
                    case 28:
                        this.$ = new yy.HashPair(yy.id($$[$0 - 2]), $$[$0], yy.locInfo(this._$));
                        break;
                    case 29:
                        this.$ = yy.id($$[$0 - 1]);
                        break;
                    case 30:
                        this.$ = $$[$0];
                        break;
                    case 31:
                        this.$ = $$[$0];
                        break;
                    case 32:
                        this.$ = new yy.StringLiteral($$[$0], yy.locInfo(this._$));
                        break;
                    case 33:
                        this.$ = new yy.NumberLiteral($$[$0], yy.locInfo(this._$));
                        break;
                    case 34:
                        this.$ = new yy.BooleanLiteral($$[$0], yy.locInfo(this._$));
                        break;
                    case 35:
                        this.$ = new yy.UndefinedLiteral(yy.locInfo(this._$));
                        break;
                    case 36:
                        this.$ = new yy.NullLiteral(yy.locInfo(this._$));
                        break;
                    case 37:
                        this.$ = $$[$0];
                        break;
                    case 38:
                        this.$ = $$[$0];
                        break;
                    case 39:
                        this.$ = yy.preparePath(true, $$[$0], this._$);
                        break;
                    case 40:
                        this.$ = yy.preparePath(false, $$[$0], this._$);
                        break;
                    case 41:
                        $$[$0 - 2].push({ part: yy.id($$[$0]), original: $$[$0], separator: $$[$0 - 1] });this.$ = $$[$0 - 2];
                        break;
                    case 42:
                        this.$ = [{ part: yy.id($$[$0]), original: $$[$0] }];
                        break;
                    case 43:
                        this.$ = [];
                        break;
                    case 44:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 45:
                        this.$ = [];
                        break;
                    case 46:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 53:
                        this.$ = [];
                        break;
                    case 54:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 59:
                        this.$ = [];
                        break;
                    case 60:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 65:
                        this.$ = [];
                        break;
                    case 66:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 73:
                        this.$ = [];
                        break;
                    case 74:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 77:
                        this.$ = [];
                        break;
                    case 78:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 81:
                        this.$ = [];
                        break;
                    case 82:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 85:
                        this.$ = [];
                        break;
                    case 86:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 89:
                        this.$ = [$$[$0]];
                        break;
                    case 90:
                        $$[$0 - 1].push($$[$0]);
                        break;
                    case 91:
                        this.$ = [$$[$0]];
                        break;
                    case 92:
                        $$[$0 - 1].push($$[$0]);
                        break;
                }
            },
            table: [{ 3: 1, 4: 2, 5: [2, 43], 6: 3, 13: [2, 43], 14: [2, 43], 17: [2, 43], 27: [2, 43], 32: [2, 43], 46: [2, 43], 49: [2, 43], 53: [2, 43] }, { 1: [3] }, { 5: [1, 4] }, { 5: [2, 2], 7: 5, 8: 6, 9: 7, 10: 8, 11: 9, 12: 10, 13: [1, 11], 14: [1, 18], 15: 16, 17: [1, 21], 22: 14, 25: 15, 27: [1, 19], 32: [1, 20], 37: [2, 2], 42: [2, 2], 45: [2, 2], 46: [1, 12], 49: [1, 13], 53: [1, 17] }, { 1: [2, 1] }, { 5: [2, 44], 13: [2, 44], 14: [2, 44], 17: [2, 44], 27: [2, 44], 32: [2, 44], 37: [2, 44], 42: [2, 44], 45: [2, 44], 46: [2, 44], 49: [2, 44], 53: [2, 44] }, { 5: [2, 3], 13: [2, 3], 14: [2, 3], 17: [2, 3], 27: [2, 3], 32: [2, 3], 37: [2, 3], 42: [2, 3], 45: [2, 3], 46: [2, 3], 49: [2, 3], 53: [2, 3] }, { 5: [2, 4], 13: [2, 4], 14: [2, 4], 17: [2, 4], 27: [2, 4], 32: [2, 4], 37: [2, 4], 42: [2, 4], 45: [2, 4], 46: [2, 4], 49: [2, 4], 53: [2, 4] }, { 5: [2, 5], 13: [2, 5], 14: [2, 5], 17: [2, 5], 27: [2, 5], 32: [2, 5], 37: [2, 5], 42: [2, 5], 45: [2, 5], 46: [2, 5], 49: [2, 5], 53: [2, 5] }, { 5: [2, 6], 13: [2, 6], 14: [2, 6], 17: [2, 6], 27: [2, 6], 32: [2, 6], 37: [2, 6], 42: [2, 6], 45: [2, 6], 46: [2, 6], 49: [2, 6], 53: [2, 6] }, { 5: [2, 7], 13: [2, 7], 14: [2, 7], 17: [2, 7], 27: [2, 7], 32: [2, 7], 37: [2, 7], 42: [2, 7], 45: [2, 7], 46: [2, 7], 49: [2, 7], 53: [2, 7] }, { 5: [2, 8], 13: [2, 8], 14: [2, 8], 17: [2, 8], 27: [2, 8], 32: [2, 8], 37: [2, 8], 42: [2, 8], 45: [2, 8], 46: [2, 8], 49: [2, 8], 53: [2, 8] }, { 18: 22, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 18: 33, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 4: 34, 6: 3, 13: [2, 43], 14: [2, 43], 17: [2, 43], 27: [2, 43], 32: [2, 43], 37: [2, 43], 42: [2, 43], 45: [2, 43], 46: [2, 43], 49: [2, 43], 53: [2, 43] }, { 4: 35, 6: 3, 13: [2, 43], 14: [2, 43], 17: [2, 43], 27: [2, 43], 32: [2, 43], 42: [2, 43], 45: [2, 43], 46: [2, 43], 49: [2, 43], 53: [2, 43] }, { 12: 36, 14: [1, 18] }, { 18: 38, 54: 37, 58: 39, 59: [1, 40], 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 5: [2, 9], 13: [2, 9], 14: [2, 9], 16: [2, 9], 17: [2, 9], 27: [2, 9], 32: [2, 9], 37: [2, 9], 42: [2, 9], 45: [2, 9], 46: [2, 9], 49: [2, 9], 53: [2, 9] }, { 18: 41, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 18: 42, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 18: 43, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 31: [2, 73], 47: 44, 59: [2, 73], 66: [2, 73], 74: [2, 73], 75: [2, 73], 76: [2, 73], 77: [2, 73], 78: [2, 73], 79: [2, 73] }, { 21: [2, 30], 31: [2, 30], 52: [2, 30], 59: [2, 30], 62: [2, 30], 66: [2, 30], 69: [2, 30], 74: [2, 30], 75: [2, 30], 76: [2, 30], 77: [2, 30], 78: [2, 30], 79: [2, 30] }, { 21: [2, 31], 31: [2, 31], 52: [2, 31], 59: [2, 31], 62: [2, 31], 66: [2, 31], 69: [2, 31], 74: [2, 31], 75: [2, 31], 76: [2, 31], 77: [2, 31], 78: [2, 31], 79: [2, 31] }, { 21: [2, 32], 31: [2, 32], 52: [2, 32], 59: [2, 32], 62: [2, 32], 66: [2, 32], 69: [2, 32], 74: [2, 32], 75: [2, 32], 76: [2, 32], 77: [2, 32], 78: [2, 32], 79: [2, 32] }, { 21: [2, 33], 31: [2, 33], 52: [2, 33], 59: [2, 33], 62: [2, 33], 66: [2, 33], 69: [2, 33], 74: [2, 33], 75: [2, 33], 76: [2, 33], 77: [2, 33], 78: [2, 33], 79: [2, 33] }, { 21: [2, 34], 31: [2, 34], 52: [2, 34], 59: [2, 34], 62: [2, 34], 66: [2, 34], 69: [2, 34], 74: [2, 34], 75: [2, 34], 76: [2, 34], 77: [2, 34], 78: [2, 34], 79: [2, 34] }, { 21: [2, 35], 31: [2, 35], 52: [2, 35], 59: [2, 35], 62: [2, 35], 66: [2, 35], 69: [2, 35], 74: [2, 35], 75: [2, 35], 76: [2, 35], 77: [2, 35], 78: [2, 35], 79: [2, 35] }, { 21: [2, 36], 31: [2, 36], 52: [2, 36], 59: [2, 36], 62: [2, 36], 66: [2, 36], 69: [2, 36], 74: [2, 36], 75: [2, 36], 76: [2, 36], 77: [2, 36], 78: [2, 36], 79: [2, 36] }, { 21: [2, 40], 31: [2, 40], 52: [2, 40], 59: [2, 40], 62: [2, 40], 66: [2, 40], 69: [2, 40], 74: [2, 40], 75: [2, 40], 76: [2, 40], 77: [2, 40], 78: [2, 40], 79: [2, 40], 81: [1, 45] }, { 66: [1, 32], 80: 46 }, { 21: [2, 42], 31: [2, 42], 52: [2, 42], 59: [2, 42], 62: [2, 42], 66: [2, 42], 69: [2, 42], 74: [2, 42], 75: [2, 42], 76: [2, 42], 77: [2, 42], 78: [2, 42], 79: [2, 42], 81: [2, 42] }, { 50: 47, 52: [2, 77], 59: [2, 77], 66: [2, 77], 74: [2, 77], 75: [2, 77], 76: [2, 77], 77: [2, 77], 78: [2, 77], 79: [2, 77] }, { 23: 48, 36: 50, 37: [1, 52], 41: 51, 42: [1, 53], 43: 49, 45: [2, 49] }, { 26: 54, 41: 55, 42: [1, 53], 45: [2, 51] }, { 16: [1, 56] }, { 31: [2, 81], 55: 57, 59: [2, 81], 66: [2, 81], 74: [2, 81], 75: [2, 81], 76: [2, 81], 77: [2, 81], 78: [2, 81], 79: [2, 81] }, { 31: [2, 37], 59: [2, 37], 66: [2, 37], 74: [2, 37], 75: [2, 37], 76: [2, 37], 77: [2, 37], 78: [2, 37], 79: [2, 37] }, { 31: [2, 38], 59: [2, 38], 66: [2, 38], 74: [2, 38], 75: [2, 38], 76: [2, 38], 77: [2, 38], 78: [2, 38], 79: [2, 38] }, { 18: 58, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 28: 59, 31: [2, 53], 59: [2, 53], 66: [2, 53], 69: [2, 53], 74: [2, 53], 75: [2, 53], 76: [2, 53], 77: [2, 53], 78: [2, 53], 79: [2, 53] }, { 31: [2, 59], 33: 60, 59: [2, 59], 66: [2, 59], 69: [2, 59], 74: [2, 59], 75: [2, 59], 76: [2, 59], 77: [2, 59], 78: [2, 59], 79: [2, 59] }, { 19: 61, 21: [2, 45], 59: [2, 45], 66: [2, 45], 74: [2, 45], 75: [2, 45], 76: [2, 45], 77: [2, 45], 78: [2, 45], 79: [2, 45] }, { 18: 65, 31: [2, 75], 48: 62, 57: 63, 58: 66, 59: [1, 40], 63: 64, 64: 67, 65: 68, 66: [1, 69], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 66: [1, 70] }, { 21: [2, 39], 31: [2, 39], 52: [2, 39], 59: [2, 39], 62: [2, 39], 66: [2, 39], 69: [2, 39], 74: [2, 39], 75: [2, 39], 76: [2, 39], 77: [2, 39], 78: [2, 39], 79: [2, 39], 81: [1, 45] }, { 18: 65, 51: 71, 52: [2, 79], 57: 72, 58: 66, 59: [1, 40], 63: 73, 64: 67, 65: 68, 66: [1, 69], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 24: 74, 45: [1, 75] }, { 45: [2, 50] }, { 4: 76, 6: 3, 13: [2, 43], 14: [2, 43], 17: [2, 43], 27: [2, 43], 32: [2, 43], 37: [2, 43], 42: [2, 43], 45: [2, 43], 46: [2, 43], 49: [2, 43], 53: [2, 43] }, { 45: [2, 19] }, { 18: 77, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 4: 78, 6: 3, 13: [2, 43], 14: [2, 43], 17: [2, 43], 27: [2, 43], 32: [2, 43], 45: [2, 43], 46: [2, 43], 49: [2, 43], 53: [2, 43] }, { 24: 79, 45: [1, 75] }, { 45: [2, 52] }, { 5: [2, 10], 13: [2, 10], 14: [2, 10], 17: [2, 10], 27: [2, 10], 32: [2, 10], 37: [2, 10], 42: [2, 10], 45: [2, 10], 46: [2, 10], 49: [2, 10], 53: [2, 10] }, { 18: 65, 31: [2, 83], 56: 80, 57: 81, 58: 66, 59: [1, 40], 63: 82, 64: 67, 65: 68, 66: [1, 69], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 59: [2, 85], 60: 83, 62: [2, 85], 66: [2, 85], 74: [2, 85], 75: [2, 85], 76: [2, 85], 77: [2, 85], 78: [2, 85], 79: [2, 85] }, { 18: 65, 29: 84, 31: [2, 55], 57: 85, 58: 66, 59: [1, 40], 63: 86, 64: 67, 65: 68, 66: [1, 69], 69: [2, 55], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 18: 65, 31: [2, 61], 34: 87, 57: 88, 58: 66, 59: [1, 40], 63: 89, 64: 67, 65: 68, 66: [1, 69], 69: [2, 61], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 18: 65, 20: 90, 21: [2, 47], 57: 91, 58: 66, 59: [1, 40], 63: 92, 64: 67, 65: 68, 66: [1, 69], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 31: [1, 93] }, { 31: [2, 74], 59: [2, 74], 66: [2, 74], 74: [2, 74], 75: [2, 74], 76: [2, 74], 77: [2, 74], 78: [2, 74], 79: [2, 74] }, { 31: [2, 76] }, { 21: [2, 24], 31: [2, 24], 52: [2, 24], 59: [2, 24], 62: [2, 24], 66: [2, 24], 69: [2, 24], 74: [2, 24], 75: [2, 24], 76: [2, 24], 77: [2, 24], 78: [2, 24], 79: [2, 24] }, { 21: [2, 25], 31: [2, 25], 52: [2, 25], 59: [2, 25], 62: [2, 25], 66: [2, 25], 69: [2, 25], 74: [2, 25], 75: [2, 25], 76: [2, 25], 77: [2, 25], 78: [2, 25], 79: [2, 25] }, { 21: [2, 27], 31: [2, 27], 52: [2, 27], 62: [2, 27], 65: 94, 66: [1, 95], 69: [2, 27] }, { 21: [2, 89], 31: [2, 89], 52: [2, 89], 62: [2, 89], 66: [2, 89], 69: [2, 89] }, { 21: [2, 42], 31: [2, 42], 52: [2, 42], 59: [2, 42], 62: [2, 42], 66: [2, 42], 67: [1, 96], 69: [2, 42], 74: [2, 42], 75: [2, 42], 76: [2, 42], 77: [2, 42], 78: [2, 42], 79: [2, 42], 81: [2, 42] }, { 21: [2, 41], 31: [2, 41], 52: [2, 41], 59: [2, 41], 62: [2, 41], 66: [2, 41], 69: [2, 41], 74: [2, 41], 75: [2, 41], 76: [2, 41], 77: [2, 41], 78: [2, 41], 79: [2, 41], 81: [2, 41] }, { 52: [1, 97] }, { 52: [2, 78], 59: [2, 78], 66: [2, 78], 74: [2, 78], 75: [2, 78], 76: [2, 78], 77: [2, 78], 78: [2, 78], 79: [2, 78] }, { 52: [2, 80] }, { 5: [2, 12], 13: [2, 12], 14: [2, 12], 17: [2, 12], 27: [2, 12], 32: [2, 12], 37: [2, 12], 42: [2, 12], 45: [2, 12], 46: [2, 12], 49: [2, 12], 53: [2, 12] }, { 18: 98, 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 36: 50, 37: [1, 52], 41: 51, 42: [1, 53], 43: 100, 44: 99, 45: [2, 71] }, { 31: [2, 65], 38: 101, 59: [2, 65], 66: [2, 65], 69: [2, 65], 74: [2, 65], 75: [2, 65], 76: [2, 65], 77: [2, 65], 78: [2, 65], 79: [2, 65] }, { 45: [2, 17] }, { 5: [2, 13], 13: [2, 13], 14: [2, 13], 17: [2, 13], 27: [2, 13], 32: [2, 13], 37: [2, 13], 42: [2, 13], 45: [2, 13], 46: [2, 13], 49: [2, 13], 53: [2, 13] }, { 31: [1, 102] }, { 31: [2, 82], 59: [2, 82], 66: [2, 82], 74: [2, 82], 75: [2, 82], 76: [2, 82], 77: [2, 82], 78: [2, 82], 79: [2, 82] }, { 31: [2, 84] }, { 18: 65, 57: 104, 58: 66, 59: [1, 40], 61: 103, 62: [2, 87], 63: 105, 64: 67, 65: 68, 66: [1, 69], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 30: 106, 31: [2, 57], 68: 107, 69: [1, 108] }, { 31: [2, 54], 59: [2, 54], 66: [2, 54], 69: [2, 54], 74: [2, 54], 75: [2, 54], 76: [2, 54], 77: [2, 54], 78: [2, 54], 79: [2, 54] }, { 31: [2, 56], 69: [2, 56] }, { 31: [2, 63], 35: 109, 68: 110, 69: [1, 108] }, { 31: [2, 60], 59: [2, 60], 66: [2, 60], 69: [2, 60], 74: [2, 60], 75: [2, 60], 76: [2, 60], 77: [2, 60], 78: [2, 60], 79: [2, 60] }, { 31: [2, 62], 69: [2, 62] }, { 21: [1, 111] }, { 21: [2, 46], 59: [2, 46], 66: [2, 46], 74: [2, 46], 75: [2, 46], 76: [2, 46], 77: [2, 46], 78: [2, 46], 79: [2, 46] }, { 21: [2, 48] }, { 5: [2, 21], 13: [2, 21], 14: [2, 21], 17: [2, 21], 27: [2, 21], 32: [2, 21], 37: [2, 21], 42: [2, 21], 45: [2, 21], 46: [2, 21], 49: [2, 21], 53: [2, 21] }, { 21: [2, 90], 31: [2, 90], 52: [2, 90], 62: [2, 90], 66: [2, 90], 69: [2, 90] }, { 67: [1, 96] }, { 18: 65, 57: 112, 58: 66, 59: [1, 40], 66: [1, 32], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 5: [2, 22], 13: [2, 22], 14: [2, 22], 17: [2, 22], 27: [2, 22], 32: [2, 22], 37: [2, 22], 42: [2, 22], 45: [2, 22], 46: [2, 22], 49: [2, 22], 53: [2, 22] }, { 31: [1, 113] }, { 45: [2, 18] }, { 45: [2, 72] }, { 18: 65, 31: [2, 67], 39: 114, 57: 115, 58: 66, 59: [1, 40], 63: 116, 64: 67, 65: 68, 66: [1, 69], 69: [2, 67], 72: 23, 73: 24, 74: [1, 25], 75: [1, 26], 76: [1, 27], 77: [1, 28], 78: [1, 29], 79: [1, 31], 80: 30 }, { 5: [2, 23], 13: [2, 23], 14: [2, 23], 17: [2, 23], 27: [2, 23], 32: [2, 23], 37: [2, 23], 42: [2, 23], 45: [2, 23], 46: [2, 23], 49: [2, 23], 53: [2, 23] }, { 62: [1, 117] }, { 59: [2, 86], 62: [2, 86], 66: [2, 86], 74: [2, 86], 75: [2, 86], 76: [2, 86], 77: [2, 86], 78: [2, 86], 79: [2, 86] }, { 62: [2, 88] }, { 31: [1, 118] }, { 31: [2, 58] }, { 66: [1, 120], 70: 119 }, { 31: [1, 121] }, { 31: [2, 64] }, { 14: [2, 11] }, { 21: [2, 28], 31: [2, 28], 52: [2, 28], 62: [2, 28], 66: [2, 28], 69: [2, 28] }, { 5: [2, 20], 13: [2, 20], 14: [2, 20], 17: [2, 20], 27: [2, 20], 32: [2, 20], 37: [2, 20], 42: [2, 20], 45: [2, 20], 46: [2, 20], 49: [2, 20], 53: [2, 20] }, { 31: [2, 69], 40: 122, 68: 123, 69: [1, 108] }, { 31: [2, 66], 59: [2, 66], 66: [2, 66], 69: [2, 66], 74: [2, 66], 75: [2, 66], 76: [2, 66], 77: [2, 66], 78: [2, 66], 79: [2, 66] }, { 31: [2, 68], 69: [2, 68] }, { 21: [2, 26], 31: [2, 26], 52: [2, 26], 59: [2, 26], 62: [2, 26], 66: [2, 26], 69: [2, 26], 74: [2, 26], 75: [2, 26], 76: [2, 26], 77: [2, 26], 78: [2, 26], 79: [2, 26] }, { 13: [2, 14], 14: [2, 14], 17: [2, 14], 27: [2, 14], 32: [2, 14], 37: [2, 14], 42: [2, 14], 45: [2, 14], 46: [2, 14], 49: [2, 14], 53: [2, 14] }, { 66: [1, 125], 71: [1, 124] }, { 66: [2, 91], 71: [2, 91] }, { 13: [2, 15], 14: [2, 15], 17: [2, 15], 27: [2, 15], 32: [2, 15], 42: [2, 15], 45: [2, 15], 46: [2, 15], 49: [2, 15], 53: [2, 15] }, { 31: [1, 126] }, { 31: [2, 70] }, { 31: [2, 29] }, { 66: [2, 92], 71: [2, 92] }, { 13: [2, 16], 14: [2, 16], 17: [2, 16], 27: [2, 16], 32: [2, 16], 37: [2, 16], 42: [2, 16], 45: [2, 16], 46: [2, 16], 49: [2, 16], 53: [2, 16] }],
            defaultActions: { 4: [2, 1], 49: [2, 50], 51: [2, 19], 55: [2, 52], 64: [2, 76], 73: [2, 80], 78: [2, 17], 82: [2, 84], 92: [2, 48], 99: [2, 18], 100: [2, 72], 105: [2, 88], 107: [2, 58], 110: [2, 64], 111: [2, 11], 123: [2, 70], 124: [2, 29] },
            parseError: function parseError(str, hash) {
                throw new Error(str);
            },
            parse: function parse(input) {
                var self = this,
                    stack = [0],
                    vstack = [null],
                    lstack = [],
                    table = this.table,
                    yytext = "",
                    yylineno = 0,
                    yyleng = 0,
                    recovering = 0,
                    TERROR = 2,
                    EOF = 1;
                this.lexer.setInput(input);
                this.lexer.yy = this.yy;
                this.yy.lexer = this.lexer;
                this.yy.parser = this;
                if (typeof this.lexer.yylloc == "undefined") this.lexer.yylloc = {};
                var yyloc = this.lexer.yylloc;
                lstack.push(yyloc);
                var ranges = this.lexer.options && this.lexer.options.ranges;
                if (typeof this.yy.parseError === "function") this.parseError = this.yy.parseError;
                function popStack(n) {
                    stack.length = stack.length - 2 * n;
                    vstack.length = vstack.length - n;
                    lstack.length = lstack.length - n;
                }
                function lex() {
                    var token;
                    token = self.lexer.lex() || 1;
                    if (typeof token !== "number") {
                        token = self.symbols_[token] || token;
                    }
                    return token;
                }
                var symbol,
                    preErrorSymbol,
                    state,
                    action,
                    a,
                    r,
                    yyval = {},
                    p,
                    len,
                    newState,
                    expected;
                while (true) {
                    state = stack[stack.length - 1];
                    if (this.defaultActions[state]) {
                        action = this.defaultActions[state];
                    } else {
                        if (symbol === null || typeof symbol == "undefined") {
                            symbol = lex();
                        }
                        action = table[state] && table[state][symbol];
                    }
                    if (typeof action === "undefined" || !action.length || !action[0]) {
                        var errStr = "";
                        if (!recovering) {
                            expected = [];
                            for (p in table[state]) if (this.terminals_[p] && p > 2) {
                                expected.push("'" + this.terminals_[p] + "'");
                            }
                            if (this.lexer.showPosition) {
                                errStr = "Parse error on line " + (yylineno + 1) + ":\n" + this.lexer.showPosition() + "\nExpecting " + expected.join(", ") + ", got '" + (this.terminals_[symbol] || symbol) + "'";
                            } else {
                                errStr = "Parse error on line " + (yylineno + 1) + ": Unexpected " + (symbol == 1 ? "end of input" : "'" + (this.terminals_[symbol] || symbol) + "'");
                            }
                            this.parseError(errStr, { text: this.lexer.match, token: this.terminals_[symbol] || symbol, line: this.lexer.yylineno, loc: yyloc, expected: expected });
                        }
                    }
                    if (action[0] instanceof Array && action.length > 1) {
                        throw new Error("Parse Error: multiple actions possible at state: " + state + ", token: " + symbol);
                    }
                    switch (action[0]) {
                        case 1:
                            stack.push(symbol);
                            vstack.push(this.lexer.yytext);
                            lstack.push(this.lexer.yylloc);
                            stack.push(action[1]);
                            symbol = null;
                            if (!preErrorSymbol) {
                                yyleng = this.lexer.yyleng;
                                yytext = this.lexer.yytext;
                                yylineno = this.lexer.yylineno;
                                yyloc = this.lexer.yylloc;
                                if (recovering > 0) recovering--;
                            } else {
                                symbol = preErrorSymbol;
                                preErrorSymbol = null;
                            }
                            break;
                        case 2:
                            len = this.productions_[action[1]][1];
                            yyval.$ = vstack[vstack.length - len];
                            yyval._$ = { first_line: lstack[lstack.length - (len || 1)].first_line, last_line: lstack[lstack.length - 1].last_line, first_column: lstack[lstack.length - (len || 1)].first_column, last_column: lstack[lstack.length - 1].last_column };
                            if (ranges) {
                                yyval._$.range = [lstack[lstack.length - (len || 1)].range[0], lstack[lstack.length - 1].range[1]];
                            }
                            r = this.performAction.call(yyval, yytext, yyleng, yylineno, this.yy, action[1], vstack, lstack);
                            if (typeof r !== "undefined") {
                                return r;
                            }
                            if (len) {
                                stack = stack.slice(0, -1 * len * 2);
                                vstack = vstack.slice(0, -1 * len);
                                lstack = lstack.slice(0, -1 * len);
                            }
                            stack.push(this.productions_[action[1]][0]);
                            vstack.push(yyval.$);
                            lstack.push(yyval._$);
                            newState = table[stack[stack.length - 2]][stack[stack.length - 1]];
                            stack.push(newState);
                            break;
                        case 3:
                            return true;
                    }
                }
                return true;
            }
        };
        /* Jison generated lexer */
        var lexer = (function () {
            var lexer = { EOF: 1,
                parseError: function parseError(str, hash) {
                    if (this.yy.parser) {
                        this.yy.parser.parseError(str, hash);
                    } else {
                        throw new Error(str);
                    }
                },
                setInput: function (input) {
                    this._input = input;
                    this._more = this._less = this.done = false;
                    this.yylineno = this.yyleng = 0;
                    this.yytext = this.matched = this.match = '';
                    this.conditionStack = ['INITIAL'];
                    this.yylloc = { first_line: 1, first_column: 0, last_line: 1, last_column: 0 };
                    if (this.options.ranges) this.yylloc.range = [0, 0];
                    this.offset = 0;
                    return this;
                },
                input: function () {
                    var ch = this._input[0];
                    this.yytext += ch;
                    this.yyleng++;
                    this.offset++;
                    this.match += ch;
                    this.matched += ch;
                    var lines = ch.match(/(?:\r\n?|\n).*/g);
                    if (lines) {
                        this.yylineno++;
                        this.yylloc.last_line++;
                    } else {
                        this.yylloc.last_column++;
                    }
                    if (this.options.ranges) this.yylloc.range[1]++;

                    this._input = this._input.slice(1);
                    return ch;
                },
                unput: function (ch) {
                    var len = ch.length;
                    var lines = ch.split(/(?:\r\n?|\n)/g);

                    this._input = ch + this._input;
                    this.yytext = this.yytext.substr(0, this.yytext.length - len - 1);
                    //this.yyleng -= len;
                    this.offset -= len;
                    var oldLines = this.match.split(/(?:\r\n?|\n)/g);
                    this.match = this.match.substr(0, this.match.length - 1);
                    this.matched = this.matched.substr(0, this.matched.length - 1);

                    if (lines.length - 1) this.yylineno -= lines.length - 1;
                    var r = this.yylloc.range;

                    this.yylloc = { first_line: this.yylloc.first_line,
                        last_line: this.yylineno + 1,
                        first_column: this.yylloc.first_column,
                        last_column: lines ? (lines.length === oldLines.length ? this.yylloc.first_column : 0) + oldLines[oldLines.length - lines.length].length - lines[0].length : this.yylloc.first_column - len
                    };

                    if (this.options.ranges) {
                        this.yylloc.range = [r[0], r[0] + this.yyleng - len];
                    }
                    return this;
                },
                more: function () {
                    this._more = true;
                    return this;
                },
                less: function (n) {
                    this.unput(this.match.slice(n));
                },
                pastInput: function () {
                    var past = this.matched.substr(0, this.matched.length - this.match.length);
                    return (past.length > 20 ? '...' : '') + past.substr(-20).replace(/\n/g, "");
                },
                upcomingInput: function () {
                    var next = this.match;
                    if (next.length < 20) {
                        next += this._input.substr(0, 20 - next.length);
                    }
                    return (next.substr(0, 20) + (next.length > 20 ? '...' : '')).replace(/\n/g, "");
                },
                showPosition: function () {
                    var pre = this.pastInput();
                    var c = new Array(pre.length + 1).join("-");
                    return pre + this.upcomingInput() + "\n" + c + "^";
                },
                next: function () {
                    if (this.done) {
                        return this.EOF;
                    }
                    if (!this._input) this.done = true;

                    var token, match, tempMatch, index, col, lines;
                    if (!this._more) {
                        this.yytext = '';
                        this.match = '';
                    }
                    var rules = this._currentRules();
                    for (var i = 0; i < rules.length; i++) {
                        tempMatch = this._input.match(this.rules[rules[i]]);
                        if (tempMatch && (!match || tempMatch[0].length > match[0].length)) {
                            match = tempMatch;
                            index = i;
                            if (!this.options.flex) break;
                        }
                    }
                    if (match) {
                        lines = match[0].match(/(?:\r\n?|\n).*/g);
                        if (lines) this.yylineno += lines.length;
                        this.yylloc = { first_line: this.yylloc.last_line,
                            last_line: this.yylineno + 1,
                            first_column: this.yylloc.last_column,
                            last_column: lines ? lines[lines.length - 1].length - lines[lines.length - 1].match(/\r?\n?/)[0].length : this.yylloc.last_column + match[0].length };
                        this.yytext += match[0];
                        this.match += match[0];
                        this.matches = match;
                        this.yyleng = this.yytext.length;
                        if (this.options.ranges) {
                            this.yylloc.range = [this.offset, this.offset += this.yyleng];
                        }
                        this._more = false;
                        this._input = this._input.slice(match[0].length);
                        this.matched += match[0];
                        token = this.performAction.call(this, this.yy, this, rules[index], this.conditionStack[this.conditionStack.length - 1]);
                        if (this.done && this._input) this.done = false;
                        if (token) return token;else return;
                    }
                    if (this._input === "") {
                        return this.EOF;
                    } else {
                        return this.parseError('Lexical error on line ' + (this.yylineno + 1) + '. Unrecognized text.\n' + this.showPosition(), { text: "", token: null, line: this.yylineno });
                    }
                },
                lex: function lex() {
                    var r = this.next();
                    if (typeof r !== 'undefined') {
                        return r;
                    } else {
                        return this.lex();
                    }
                },
                begin: function begin(condition) {
                    this.conditionStack.push(condition);
                },
                popState: function popState() {
                    return this.conditionStack.pop();
                },
                _currentRules: function _currentRules() {
                    return this.conditions[this.conditionStack[this.conditionStack.length - 1]].rules;
                },
                topState: function () {
                    return this.conditionStack[this.conditionStack.length - 2];
                },
                pushState: function begin(condition) {
                    this.begin(condition);
                } };
            lexer.options = {};
            lexer.performAction = function anonymous(yy, yy_, $avoiding_name_collisions, YY_START) {

                function strip(start, end) {
                    return yy_.yytext = yy_.yytext.substr(start, yy_.yyleng - end);
                }

                var YYSTATE = YY_START;
                switch ($avoiding_name_collisions) {
                    case 0:
                        if (yy_.yytext.slice(-2) === "\\\\") {
                            strip(0, 1);
                            this.begin("mu");
                        } else if (yy_.yytext.slice(-1) === "\\") {
                            strip(0, 1);
                            this.begin("emu");
                        } else {
                            this.begin("mu");
                        }
                        if (yy_.yytext) return 14;

                        break;
                    case 1:
                        return 14;
                        break;
                    case 2:
                        this.popState();
                        return 14;

                        break;
                    case 3:
                        yy_.yytext = yy_.yytext.substr(5, yy_.yyleng - 9);
                        this.popState();
                        return 16;

                        break;
                    case 4:
                        return 14;
                        break;
                    case 5:
                        this.popState();
                        return 13;

                        break;
                    case 6:
                        return 59;
                        break;
                    case 7:
                        return 62;
                        break;
                    case 8:
                        return 17;
                        break;
                    case 9:
                        this.popState();
                        this.begin('raw');
                        return 21;

                        break;
                    case 10:
                        return 53;
                        break;
                    case 11:
                        return 27;
                        break;
                    case 12:
                        return 45;
                        break;
                    case 13:
                        this.popState();return 42;
                        break;
                    case 14:
                        this.popState();return 42;
                        break;
                    case 15:
                        return 32;
                        break;
                    case 16:
                        return 37;
                        break;
                    case 17:
                        return 49;
                        break;
                    case 18:
                        return 46;
                        break;
                    case 19:
                        this.unput(yy_.yytext);
                        this.popState();
                        this.begin('com');

                        break;
                    case 20:
                        this.popState();
                        return 13;

                        break;
                    case 21:
                        return 46;
                        break;
                    case 22:
                        return 67;
                        break;
                    case 23:
                        return 66;
                        break;
                    case 24:
                        return 66;
                        break;
                    case 25:
                        return 81;
                        break;
                    case 26:
                        // ignore whitespace
                        break;
                    case 27:
                        this.popState();return 52;
                        break;
                    case 28:
                        this.popState();return 31;
                        break;
                    case 29:
                        yy_.yytext = strip(1, 2).replace(/\\"/g, '"');return 74;
                        break;
                    case 30:
                        yy_.yytext = strip(1, 2).replace(/\\'/g, "'");return 74;
                        break;
                    case 31:
                        return 79;
                        break;
                    case 32:
                        return 76;
                        break;
                    case 33:
                        return 76;
                        break;
                    case 34:
                        return 77;
                        break;
                    case 35:
                        return 78;
                        break;
                    case 36:
                        return 75;
                        break;
                    case 37:
                        return 69;
                        break;
                    case 38:
                        return 71;
                        break;
                    case 39:
                        return 66;
                        break;
                    case 40:
                        return 66;
                        break;
                    case 41:
                        return 'INVALID';
                        break;
                    case 42:
                        return 5;
                        break;
                }
            };
            lexer.rules = [/^(?:[^\x00]*?(?=(\{\{)))/, /^(?:[^\x00]+)/, /^(?:[^\x00]{2,}?(?=(\{\{|\\\{\{|\\\\\{\{|$)))/, /^(?:\{\{\{\{\/[^\s!"#%-,\.\/;->@\[-\^`\{-~]+(?=[=}\s\/.])\}\}\}\})/, /^(?:[^\x00]*?(?=(\{\{\{\{\/)))/, /^(?:[\s\S]*?--(~)?\}\})/, /^(?:\()/, /^(?:\))/, /^(?:\{\{\{\{)/, /^(?:\}\}\}\})/, /^(?:\{\{(~)?>)/, /^(?:\{\{(~)?#)/, /^(?:\{\{(~)?\/)/, /^(?:\{\{(~)?\^\s*(~)?\}\})/, /^(?:\{\{(~)?\s*else\s*(~)?\}\})/, /^(?:\{\{(~)?\^)/, /^(?:\{\{(~)?\s*else\b)/, /^(?:\{\{(~)?\{)/, /^(?:\{\{(~)?&)/, /^(?:\{\{(~)?!--)/, /^(?:\{\{(~)?![\s\S]*?\}\})/, /^(?:\{\{(~)?)/, /^(?:=)/, /^(?:\.\.)/, /^(?:\.(?=([=~}\s\/.)|])))/, /^(?:[\/.])/, /^(?:\s+)/, /^(?:\}(~)?\}\})/, /^(?:(~)?\}\})/, /^(?:"(\\["]|[^"])*")/, /^(?:'(\\[']|[^'])*')/, /^(?:@)/, /^(?:true(?=([~}\s)])))/, /^(?:false(?=([~}\s)])))/, /^(?:undefined(?=([~}\s)])))/, /^(?:null(?=([~}\s)])))/, /^(?:-?[0-9]+(?:\.[0-9]+)?(?=([~}\s)])))/, /^(?:as\s+\|)/, /^(?:\|)/, /^(?:([^\s!"#%-,\.\/;->@\[-\^`\{-~]+(?=([=~}\s\/.)|]))))/, /^(?:\[[^\]]*\])/, /^(?:.)/, /^(?:$)/];
            lexer.conditions = { "mu": { "rules": [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42], "inclusive": false }, "emu": { "rules": [2], "inclusive": false }, "com": { "rules": [5], "inclusive": false }, "raw": { "rules": [3, 4], "inclusive": false }, "INITIAL": { "rules": [0, 1, 42], "inclusive": true } };
            return lexer;
        })();
        parser.lexer = lexer;
        function Parser() {
            this.yy = {};
        }Parser.prototype = parser;parser.Parser = Parser;
        return new Parser();
    })();exports.default = handlebars;
});
enifed('htmlbars-syntax/handlebars/compiler/visitor', ['exports', 'htmlbars-syntax/handlebars/exception', 'htmlbars-syntax/handlebars/compiler/ast'], function (exports, _htmlbarsSyntaxHandlebarsException, _htmlbarsSyntaxHandlebarsCompilerAst) {
  'use strict';

  function Visitor() {
    this.parents = [];
  }

  Visitor.prototype = {
    constructor: Visitor,
    mutating: false,

    // Visits a given value. If mutating, will replace the value if necessary.
    acceptKey: function (node, name) {
      var value = this.accept(node[name]);
      if (this.mutating) {
        // Hacky sanity check:
        if (value && (!value.type || !_htmlbarsSyntaxHandlebarsCompilerAst.default[value.type])) {
          throw new _htmlbarsSyntaxHandlebarsException.default('Unexpected node type "' + value.type + '" found when accepting ' + name + ' on ' + node.type);
        }
        node[name] = value;
      }
    },

    // Performs an accept operation with added sanity check to ensure
    // required keys are not removed.
    acceptRequired: function (node, name) {
      this.acceptKey(node, name);

      if (!node[name]) {
        throw new _htmlbarsSyntaxHandlebarsException.default(node.type + ' requires ' + name);
      }
    },

    // Traverses a given array. If mutating, empty respnses will be removed
    // for child elements.
    acceptArray: function (array) {
      for (var i = 0, l = array.length; i < l; i++) {
        this.acceptKey(array, i);

        if (!array[i]) {
          array.splice(i, 1);
          i--;
          l--;
        }
      }
    },

    accept: function (object) {
      if (!object) {
        return;
      }

      if (this.current) {
        this.parents.unshift(this.current);
      }
      this.current = object;

      var ret = this[object.type](object);

      this.current = this.parents.shift();

      if (!this.mutating || ret) {
        return ret;
      } else if (ret !== false) {
        return object;
      }
    },

    Program: function (program) {
      this.acceptArray(program.body);
    },

    MustacheStatement: function (mustache) {
      this.acceptRequired(mustache, 'path');
      this.acceptArray(mustache.params);
      this.acceptKey(mustache, 'hash');
    },

    BlockStatement: function (block) {
      this.acceptRequired(block, 'path');
      this.acceptArray(block.params);
      this.acceptKey(block, 'hash');

      this.acceptKey(block, 'program');
      this.acceptKey(block, 'inverse');
    },

    PartialStatement: function (partial) {
      this.acceptRequired(partial, 'name');
      this.acceptArray(partial.params);
      this.acceptKey(partial, 'hash');
    },

    ContentStatement: function () /* content */{},
    CommentStatement: function () /* comment */{},

    SubExpression: function (sexpr) {
      this.acceptRequired(sexpr, 'path');
      this.acceptArray(sexpr.params);
      this.acceptKey(sexpr, 'hash');
    },

    PathExpression: function () /* path */{},

    StringLiteral: function () /* string */{},
    NumberLiteral: function () /* number */{},
    BooleanLiteral: function () /* bool */{},
    UndefinedLiteral: function () /* literal */{},
    NullLiteral: function () /* literal */{},

    Hash: function (hash) {
      this.acceptArray(hash.pairs);
    },
    HashPair: function (pair) {
      this.acceptRequired(pair, 'value');
    }
  };

  exports.default = Visitor;
});
enifed('htmlbars-syntax/handlebars/compiler/whitespace-control', ['exports', 'htmlbars-syntax/handlebars/compiler/visitor'], function (exports, _htmlbarsSyntaxHandlebarsCompilerVisitor) {
  'use strict';

  function WhitespaceControl() {}
  WhitespaceControl.prototype = new _htmlbarsSyntaxHandlebarsCompilerVisitor.default();

  WhitespaceControl.prototype.Program = function (program) {
    var isRoot = !this.isRootSeen;
    this.isRootSeen = true;

    var body = program.body;
    for (var i = 0, l = body.length; i < l; i++) {
      var current = body[i],
          strip = this.accept(current);

      if (!strip) {
        continue;
      }

      var _isPrevWhitespace = isPrevWhitespace(body, i, isRoot),
          _isNextWhitespace = isNextWhitespace(body, i, isRoot),
          openStandalone = strip.openStandalone && _isPrevWhitespace,
          closeStandalone = strip.closeStandalone && _isNextWhitespace,
          inlineStandalone = strip.inlineStandalone && _isPrevWhitespace && _isNextWhitespace;

      if (strip.close) {
        omitRight(body, i, true);
      }
      if (strip.open) {
        omitLeft(body, i, true);
      }

      if (inlineStandalone) {
        omitRight(body, i);

        if (omitLeft(body, i)) {
          // If we are on a standalone node, save the indent info for partials
          if (current.type === 'PartialStatement') {
            // Pull out the whitespace from the final line
            current.indent = /([ \t]+$)/.exec(body[i - 1].original)[1];
          }
        }
      }
      if (openStandalone) {
        omitRight((current.program || current.inverse).body);

        // Strip out the previous content node if it's whitespace only
        omitLeft(body, i);
      }
      if (closeStandalone) {
        // Always strip the next node
        omitRight(body, i);

        omitLeft((current.inverse || current.program).body);
      }
    }

    return program;
  };
  WhitespaceControl.prototype.BlockStatement = function (block) {
    this.accept(block.program);
    this.accept(block.inverse);

    // Find the inverse program that is involed with whitespace stripping.
    var program = block.program || block.inverse,
        inverse = block.program && block.inverse,
        firstInverse = inverse,
        lastInverse = inverse;

    if (inverse && inverse.chained) {
      firstInverse = inverse.body[0].program;

      // Walk the inverse chain to find the last inverse that is actually in the chain.
      while (lastInverse.chained) {
        lastInverse = lastInverse.body[lastInverse.body.length - 1].program;
      }
    }

    var strip = {
      open: block.openStrip.open,
      close: block.closeStrip.close,

      // Determine the standalone candiacy. Basically flag our content as being possibly standalone
      // so our parent can determine if we actually are standalone
      openStandalone: isNextWhitespace(program.body),
      closeStandalone: isPrevWhitespace((firstInverse || program).body)
    };

    if (block.openStrip.close) {
      omitRight(program.body, null, true);
    }

    if (inverse) {
      var inverseStrip = block.inverseStrip;

      if (inverseStrip.open) {
        omitLeft(program.body, null, true);
      }

      if (inverseStrip.close) {
        omitRight(firstInverse.body, null, true);
      }
      if (block.closeStrip.open) {
        omitLeft(lastInverse.body, null, true);
      }

      // Find standalone else statments
      if (isPrevWhitespace(program.body) && isNextWhitespace(firstInverse.body)) {
        omitLeft(program.body);
        omitRight(firstInverse.body);
      }
    } else if (block.closeStrip.open) {
      omitLeft(program.body, null, true);
    }

    return strip;
  };

  WhitespaceControl.prototype.MustacheStatement = function (mustache) {
    return mustache.strip;
  };

  WhitespaceControl.prototype.PartialStatement = WhitespaceControl.prototype.CommentStatement = function (node) {
    /* istanbul ignore next */
    var strip = node.strip || {};
    return {
      inlineStandalone: true,
      open: strip.open,
      close: strip.close
    };
  };

  function isPrevWhitespace(body, i, isRoot) {
    if (i === undefined) {
      i = body.length;
    }

    // Nodes that end with newlines are considered whitespace (but are special
    // cased for strip operations)
    var prev = body[i - 1],
        sibling = body[i - 2];
    if (!prev) {
      return isRoot;
    }

    if (prev.type === 'ContentStatement') {
      return (sibling || !isRoot ? /\r?\n\s*?$/ : /(^|\r?\n)\s*?$/).test(prev.original);
    }
  }
  function isNextWhitespace(body, i, isRoot) {
    if (i === undefined) {
      i = -1;
    }

    var next = body[i + 1],
        sibling = body[i + 2];
    if (!next) {
      return isRoot;
    }

    if (next.type === 'ContentStatement') {
      return (sibling || !isRoot ? /^\s*?\r?\n/ : /^\s*?(\r?\n|$)/).test(next.original);
    }
  }

  // Marks the node to the right of the position as omitted.
  // I.e. {{foo}}' ' will mark the ' ' node as omitted.
  //
  // If i is undefined, then the first child will be marked as such.
  //
  // If mulitple is truthy then all whitespace will be stripped out until non-whitespace
  // content is met.
  function omitRight(body, i, multiple) {
    var current = body[i == null ? 0 : i + 1];
    if (!current || current.type !== 'ContentStatement' || !multiple && current.rightStripped) {
      return;
    }

    var original = current.value;
    current.value = current.value.replace(multiple ? /^\s+/ : /^[ \t]*\r?\n?/, '');
    current.rightStripped = current.value !== original;
  }

  // Marks the node to the left of the position as omitted.
  // I.e. ' '{{foo}} will mark the ' ' node as omitted.
  //
  // If i is undefined then the last child will be marked as such.
  //
  // If mulitple is truthy then all whitespace will be stripped out until non-whitespace
  // content is met.
  function omitLeft(body, i, multiple) {
    var current = body[i == null ? body.length - 1 : i - 1];
    if (!current || current.type !== 'ContentStatement' || !multiple && current.leftStripped) {
      return;
    }

    // We omit the last node if it's whitespace only and not preceeded by a non-content node.
    var original = current.value;
    current.value = current.value.replace(multiple ? /\s+$/ : /[ \t]+$/, '');
    current.leftStripped = current.value !== original;
    return current.leftStripped;
  }

  exports.default = WhitespaceControl;
});
enifed('htmlbars-syntax/handlebars/exception', ['exports'], function (exports) {
  'use strict';

  var errorProps = ['description', 'fileName', 'lineNumber', 'message', 'name', 'number', 'stack'];

  function Exception(message, node) {
    var loc = node && node.loc,
        line = undefined,
        column = undefined;
    if (loc) {
      line = loc.start.line;
      column = loc.start.column;

      message += ' - ' + line + ':' + column;
    }

    var tmp = Error.prototype.constructor.call(this, message);

    // Unfortunately errors are not enumerable in Chrome (at least), so `for prop in tmp` doesn't work.
    for (var idx = 0; idx < errorProps.length; idx++) {
      this[errorProps[idx]] = tmp[errorProps[idx]];
    }

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, Exception);
    }

    if (loc) {
      this.lineNumber = line;
      this.column = column;
    }
  }

  Exception.prototype = new Error();

  exports.default = Exception;
});
enifed('htmlbars-syntax/handlebars/safe-string', ['exports'], function (exports) {
  // Build out our basic SafeString type
  'use strict';

  function SafeString(string) {
    this.string = string;
  }

  SafeString.prototype.toString = SafeString.prototype.toHTML = function () {
    return '' + this.string;
  };

  exports.default = SafeString;
});
enifed('htmlbars-syntax/handlebars/utils', ['exports'], function (exports) {
  'use strict';

  exports.extend = extend;
  exports.indexOf = indexOf;
  exports.escapeExpression = escapeExpression;
  exports.isEmpty = isEmpty;
  exports.blockParams = blockParams;
  exports.appendContextPath = appendContextPath;
  var escape = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#x27;',
    '`': '&#x60;'
  };

  var badChars = /[&<>"'`]/g,
      possible = /[&<>"'`]/;

  function escapeChar(chr) {
    return escape[chr];
  }

  function extend(obj /* , ...source */) {
    for (var i = 1; i < arguments.length; i++) {
      for (var key in arguments[i]) {
        if (Object.prototype.hasOwnProperty.call(arguments[i], key)) {
          obj[key] = arguments[i][key];
        }
      }
    }

    return obj;
  }

  var toString = Object.prototype.toString;

  exports.toString = toString;
  // Sourced from lodash
  // https://github.com/bestiejs/lodash/blob/master/LICENSE.txt
  /*eslint-disable func-style, no-var */
  var isFunction = function (value) {
    return typeof value === 'function';
  };
  // fallback for older versions of Chrome and Safari
  /* istanbul ignore next */
  if (isFunction(/x/)) {
    exports.isFunction = isFunction = function (value) {
      return typeof value === 'function' && toString.call(value) === '[object Function]';
    };
  }
  var isFunction;
  exports.isFunction = isFunction;
  /*eslint-enable func-style, no-var */

  /* istanbul ignore next */
  var isArray = Array.isArray || function (value) {
    return value && typeof value === 'object' ? toString.call(value) === '[object Array]' : false;
  };

  exports.isArray = isArray;
  // Older IE versions do not directly support indexOf so we must implement our own, sadly.

  function indexOf(array, value) {
    for (var i = 0, len = array.length; i < len; i++) {
      if (array[i] === value) {
        return i;
      }
    }
    return -1;
  }

  function escapeExpression(string) {
    if (typeof string !== 'string') {
      // don't escape SafeStrings, since they're already safe
      if (string && string.toHTML) {
        return string.toHTML();
      } else if (string == null) {
        return '';
      } else if (!string) {
        return string + '';
      }

      // Force a string conversion as this will be done by the append regardless and
      // the regex test will do this transparently behind the scenes, causing issues if
      // an object's to string has escaped characters in it.
      string = '' + string;
    }

    if (!possible.test(string)) {
      return string;
    }
    return string.replace(badChars, escapeChar);
  }

  function isEmpty(value) {
    if (!value && value !== 0) {
      return true;
    } else if (isArray(value) && value.length === 0) {
      return true;
    } else {
      return false;
    }
  }

  function blockParams(params, ids) {
    params.path = ids;
    return params;
  }

  function appendContextPath(contextPath, id) {
    return (contextPath ? contextPath + '.' : '') + id;
  }
});
enifed("htmlbars-syntax/parser", ["exports", "htmlbars-syntax/handlebars/compiler/base", "htmlbars-syntax", "simple-html-tokenizer/evented-tokenizer", "simple-html-tokenizer/entity-parser", "simple-html-tokenizer/html5-named-char-refs", "htmlbars-syntax/parser/handlebars-node-visitors", "htmlbars-syntax/parser/tokenizer-event-handlers"], function (exports, _htmlbarsSyntaxHandlebarsCompilerBase, _htmlbarsSyntax, _simpleHtmlTokenizerEventedTokenizer, _simpleHtmlTokenizerEntityParser, _simpleHtmlTokenizerHtml5NamedCharRefs, _htmlbarsSyntaxParserHandlebarsNodeVisitors, _htmlbarsSyntaxParserTokenizerEventHandlers) {
  "use strict";

  exports.preprocess = preprocess;
  exports.Parser = Parser;

  function preprocess(html, options) {
    var ast = typeof html === 'object' ? html : _htmlbarsSyntaxHandlebarsCompilerBase.parse(html);
    var combined = new Parser(html, options).acceptNode(ast);

    if (options && options.plugins && options.plugins.ast) {
      for (var i = 0, l = options.plugins.ast.length; i < l; i++) {
        var plugin = new options.plugins.ast[i](options);

        plugin.syntax = _htmlbarsSyntax;

        combined = plugin.transform(combined);
      }
    }

    return combined;
  }

  exports.default = preprocess;

  var entityParser = new _simpleHtmlTokenizerEntityParser.default(_simpleHtmlTokenizerHtml5NamedCharRefs.default);

  function Parser(source, options) {
    this.options = options || {};
    this.elementStack = [];
    this.tokenizer = new _simpleHtmlTokenizerEventedTokenizer.default(this, entityParser);

    this.currentNode = null;
    this.currentAttribute = null;

    if (typeof source === 'string') {
      this.source = source.split(/(?:\r\n?|\n)/g);
    }
  }

  for (var key in _htmlbarsSyntaxParserHandlebarsNodeVisitors.default) {
    Parser.prototype[key] = _htmlbarsSyntaxParserHandlebarsNodeVisitors.default[key];
  }

  for (var key in _htmlbarsSyntaxParserTokenizerEventHandlers.default) {
    Parser.prototype[key] = _htmlbarsSyntaxParserTokenizerEventHandlers.default[key];
  }

  Parser.prototype.acceptNode = function (node) {
    return this[node.type](node);
  };

  Parser.prototype.currentElement = function () {
    return this.elementStack[this.elementStack.length - 1];
  };

  Parser.prototype.sourceForMustache = function (mustache) {
    var firstLine = mustache.loc.start.line - 1;
    var lastLine = mustache.loc.end.line - 1;
    var currentLine = firstLine - 1;
    var firstColumn = mustache.loc.start.column + 2;
    var lastColumn = mustache.loc.end.column - 2;
    var string = [];
    var line;

    if (!this.source) {
      return '{{' + mustache.path.id.original + '}}';
    }

    while (currentLine < lastLine) {
      currentLine++;
      line = this.source[currentLine];

      if (currentLine === firstLine) {
        if (firstLine === lastLine) {
          string.push(line.slice(firstColumn, lastColumn));
        } else {
          string.push(line.slice(firstColumn));
        }
      } else if (currentLine === lastLine) {
        string.push(line.slice(0, lastColumn));
      } else {
        string.push(line);
      }
    }

    return string.join('\n');
  };
});
enifed("htmlbars-syntax/parser/handlebars-node-visitors", ["exports", "htmlbars-syntax/builders", "htmlbars-syntax/utils"], function (exports, _htmlbarsSyntaxBuilders, _htmlbarsSyntaxUtils) {
  "use strict";

  exports.default = {

    Program: function (program) {
      var body = [];
      var node = _htmlbarsSyntaxBuilders.default.program(body, program.blockParams, program.loc);
      var i,
          l = program.body.length;

      this.elementStack.push(node);

      if (l === 0) {
        return this.elementStack.pop();
      }

      for (i = 0; i < l; i++) {
        this.acceptNode(program.body[i]);
      }

      // Ensure that that the element stack is balanced properly.
      var poppedNode = this.elementStack.pop();
      if (poppedNode !== node) {
        throw new Error("Unclosed element `" + poppedNode.tag + "` (on line " + poppedNode.loc.start.line + ").");
      }

      return node;
    },

    BlockStatement: function (block) {
      delete block.inverseStrip;
      delete block.openString;
      delete block.closeStrip;

      if (this.tokenizer.state === 'comment') {
        this.appendToCommentData('{{' + this.sourceForMustache(block) + '}}');
        return;
      }

      if (this.tokenizer.state !== 'comment' && this.tokenizer.state !== 'data' && this.tokenizer.state !== 'beforeData') {
        throw new Error("A block may only be used inside an HTML element or another block.");
      }

      block = acceptCommonNodes(this, block);
      var program = block.program ? this.acceptNode(block.program) : null;
      var inverse = block.inverse ? this.acceptNode(block.inverse) : null;

      var node = _htmlbarsSyntaxBuilders.default.block(block.path, block.params, block.hash, program, inverse, block.loc);
      var parentProgram = this.currentElement();
      _htmlbarsSyntaxUtils.appendChild(parentProgram, node);
    },

    MustacheStatement: function (rawMustache) {
      var tokenizer = this.tokenizer;
      var path = rawMustache.path;
      var params = rawMustache.params;
      var hash = rawMustache.hash;
      var escaped = rawMustache.escaped;
      var loc = rawMustache.loc;

      var mustache = _htmlbarsSyntaxBuilders.default.mustache(path, params, hash, !escaped, loc);

      if (tokenizer.state === 'comment') {
        this.appendToCommentData('{{' + this.sourceForMustache(mustache) + '}}');
        return;
      }

      acceptCommonNodes(this, mustache);

      switch (tokenizer.state) {
        // Tag helpers
        case "tagName":
          addElementModifier(this.currentNode, mustache);
          tokenizer.state = "beforeAttributeName";
          break;
        case "beforeAttributeName":
          addElementModifier(this.currentNode, mustache);
          break;
        case "attributeName":
        case "afterAttributeName":
          this.beginAttributeValue(false);
          this.finishAttributeValue();
          addElementModifier(this.currentNode, mustache);
          tokenizer.state = "beforeAttributeName";
          break;
        case "afterAttributeValueQuoted":
          addElementModifier(this.currentNode, mustache);
          tokenizer.state = "beforeAttributeName";
          break;

        // Attribute values
        case "beforeAttributeValue":
          appendDynamicAttributeValuePart(this.currentAttribute, mustache);
          tokenizer.state = 'attributeValueUnquoted';
          break;
        case "attributeValueDoubleQuoted":
        case "attributeValueSingleQuoted":
        case "attributeValueUnquoted":
          appendDynamicAttributeValuePart(this.currentAttribute, mustache);
          break;

        // TODO: Only append child when the tokenizer state makes
        // sense to do so, otherwise throw an error.
        default:
          _htmlbarsSyntaxUtils.appendChild(this.currentElement(), mustache);
      }

      return mustache;
    },

    ContentStatement: function (content) {
      var changeLines = 0;
      if (content.rightStripped) {
        changeLines = leadingNewlineDifference(content.original, content.value);
      }

      this.tokenizer.line = content.loc.start.line + changeLines;
      this.tokenizer.column = changeLines ? 0 : content.loc.start.column;

      this.tokenizer.tokenizePart(content.value);
      this.tokenizer.flushData();
    },

    CommentStatement: function (comment) {
      return comment;
    },

    PartialStatement: function (partial) {
      _htmlbarsSyntaxUtils.appendChild(this.currentElement(), partial);
      return partial;
    },

    SubExpression: function (sexpr) {
      return acceptCommonNodes(this, sexpr);
    },

    PathExpression: function (path) {
      delete path.data;
      delete path.depth;

      return path;
    },

    Hash: function (hash) {
      for (var i = 0; i < hash.pairs.length; i++) {
        this.acceptNode(hash.pairs[i].value);
      }

      return hash;
    },

    StringLiteral: function () {},
    BooleanLiteral: function () {},
    NumberLiteral: function () {},
    UndefinedLiteral: function () {},
    NullLiteral: function () {}
  };

  function leadingNewlineDifference(original, value) {
    if (value === '') {
      // if it is empty, just return the count of newlines
      // in original
      return original.split("\n").length - 1;
    }

    // otherwise, return the number of newlines prior to
    // `value`
    var difference = original.split(value)[0];
    var lines = difference.split(/\n/);

    return lines.length - 1;
  }

  function acceptCommonNodes(compiler, node) {
    compiler.acceptNode(node.path);

    if (node.params) {
      for (var i = 0; i < node.params.length; i++) {
        compiler.acceptNode(node.params[i]);
      }
    } else {
      node.params = [];
    }

    if (node.hash) {
      compiler.acceptNode(node.hash);
    } else {
      node.hash = _htmlbarsSyntaxBuilders.default.hash();
    }

    return node;
  }

  function addElementModifier(element, mustache) {
    var path = mustache.path;
    var params = mustache.params;
    var hash = mustache.hash;
    var loc = mustache.loc;

    var modifier = _htmlbarsSyntaxBuilders.default.elementModifier(path, params, hash, loc);
    element.modifiers.push(modifier);
  }

  function appendDynamicAttributeValuePart(attribute, part) {
    attribute.isDynamic = true;
    attribute.parts.push(part);
  }
});
enifed("htmlbars-syntax/parser/tokenizer-event-handlers", ["exports", "htmlbars-util/void-tag-names", "htmlbars-syntax/builders", "htmlbars-syntax/utils"], function (exports, _htmlbarsUtilVoidTagNames, _htmlbarsSyntaxBuilders, _htmlbarsSyntaxUtils) {
  "use strict";

  exports.default = {
    reset: function () {
      this.currentNode = null;
    },

    // Comment

    beginComment: function () {
      this.currentNode = _htmlbarsSyntaxBuilders.default.comment("");
    },

    appendToCommentData: function (char) {
      this.currentNode.value += char;
    },

    finishComment: function () {
      _htmlbarsSyntaxUtils.appendChild(this.currentElement(), this.currentNode);
    },

    // Data

    beginData: function () {
      this.currentNode = _htmlbarsSyntaxBuilders.default.text();
    },

    appendToData: function (char) {
      this.currentNode.chars += char;
    },

    finishData: function () {
      _htmlbarsSyntaxUtils.appendChild(this.currentElement(), this.currentNode);
    },

    // Tags - basic

    beginStartTag: function () {
      this.currentNode = {
        type: 'StartTag',
        name: "",
        attributes: [],
        modifiers: [],
        selfClosing: false,
        loc: null
      };
    },

    beginEndTag: function () {
      this.currentNode = {
        type: 'EndTag',
        name: "",
        attributes: [],
        modifiers: [],
        selfClosing: false,
        loc: null
      };
    },

    finishTag: function () {
      var _tokenizer = this.tokenizer;
      var tagLine = _tokenizer.tagLine;
      var tagColumn = _tokenizer.tagColumn;
      var line = _tokenizer.line;
      var column = _tokenizer.column;

      var tag = this.currentNode;
      tag.loc = _htmlbarsSyntaxBuilders.default.loc(tagLine, tagColumn, line, column);

      if (tag.type === 'StartTag') {
        this.finishStartTag();

        if (_htmlbarsUtilVoidTagNames.default.hasOwnProperty(tag.name) || tag.selfClosing) {
          this.finishEndTag(true);
        }
      } else if (tag.type === 'EndTag') {
        this.finishEndTag(false);
      }
    },

    finishStartTag: function () {
      var _currentNode = this.currentNode;
      var name = _currentNode.name;
      var attributes = _currentNode.attributes;
      var modifiers = _currentNode.modifiers;

      validateStartTag(this.currentNode, this.tokenizer);

      var loc = _htmlbarsSyntaxBuilders.default.loc(this.tokenizer.tagLine, this.tokenizer.tagColumn);
      var element = _htmlbarsSyntaxBuilders.default.element(name, attributes, modifiers, [], loc);
      this.elementStack.push(element);
    },

    finishEndTag: function (isVoid) {
      var tag = this.currentNode;

      var element = this.elementStack.pop();
      var parent = this.currentElement();
      var disableComponentGeneration = this.options.disableComponentGeneration === true;

      validateEndTag(tag, element, isVoid);

      element.loc.end.line = this.tokenizer.line;
      element.loc.end.column = this.tokenizer.column;

      if (disableComponentGeneration || cannotBeComponent(element.tag)) {
        _htmlbarsSyntaxUtils.appendChild(parent, element);
      } else {
        var program = _htmlbarsSyntaxBuilders.default.program(element.children);
        _htmlbarsSyntaxUtils.parseComponentBlockParams(element, program);
        var component = _htmlbarsSyntaxBuilders.default.component(element.tag, element.attributes, program, element.loc);
        _htmlbarsSyntaxUtils.appendChild(parent, component);
      }
    },

    markTagAsSelfClosing: function () {
      this.currentNode.selfClosing = true;
    },

    // Tags - name

    appendToTagName: function (char) {
      this.currentNode.name += char;
    },

    // Tags - attributes

    beginAttribute: function () {
      var tag = this.currentNode;
      if (tag.type === 'EndTag') {
        throw new Error("Invalid end tag: closing tag must not have attributes, " + ("in `" + tag.name + "` (on line " + this.tokenizer.line + ")."));
      }

      this.currentAttribute = {
        name: "",
        parts: [],
        isQuoted: false,
        isDynamic: false
      };
    },

    appendToAttributeName: function (char) {
      this.currentAttribute.name += char;
    },

    beginAttributeValue: function (isQuoted) {
      this.currentAttribute.isQuoted = isQuoted;
    },

    appendToAttributeValue: function (char) {
      var parts = this.currentAttribute.parts;

      if (typeof parts[parts.length - 1] === 'string') {
        parts[parts.length - 1] += char;
      } else {
        parts.push(char);
      }
    },

    finishAttributeValue: function () {
      var _currentAttribute = this.currentAttribute;
      var name = _currentAttribute.name;
      var parts = _currentAttribute.parts;
      var isQuoted = _currentAttribute.isQuoted;
      var isDynamic = _currentAttribute.isDynamic;

      var value = assembleAttributeValue(parts, isQuoted, isDynamic, this.tokenizer.line);

      this.currentNode.attributes.push(_htmlbarsSyntaxBuilders.default.attr(name, value));
    }
  };

  function assembleAttributeValue(parts, isQuoted, isDynamic, line) {
    if (isDynamic) {
      if (isQuoted) {
        return assembleConcatenatedValue(parts);
      } else {
        if (parts.length === 1 || parts.length === 2 && parts[1] === '/') {
          return parts[0];
        } else {
          throw new Error("An unquoted attribute value must be a string or a mustache, " + "preceeded by whitespace or a '=' character, and " + ("followed by whitespace, a '>' character or a '/>' (on line " + line + ")"));
        }
      }
    } else {
      return _htmlbarsSyntaxBuilders.default.text(parts.length > 0 ? parts[0] : "");
    }
  }

  function assembleConcatenatedValue(parts) {
    for (var i = 0; i < parts.length; i++) {
      var part = parts[i];

      if (typeof part === 'string') {
        parts[i] = _htmlbarsSyntaxBuilders.default.string(parts[i]);
      } else {
        if (part.type === 'MustacheStatement') {
          parts[i] = _htmlbarsSyntaxUtils.unwrapMustache(part);
        } else {
          throw new Error("Unsupported node in quoted attribute value: " + part.type);
        }
      }
    }

    return _htmlbarsSyntaxBuilders.default.concat(parts);
  }

  function cannotBeComponent(tagName) {
    return tagName.indexOf("-") === -1 && tagName.indexOf(".") === -1;
  }

  function validateStartTag(tag, tokenizer) {
    // No support for <script> tags
    if (tag.name === "script") {
      throw new Error("`SCRIPT` tags are not allowed in HTMLBars templates (on line " + tokenizer.tagLine + ")");
    }
  }

  function validateEndTag(tag, element, selfClosing) {
    if (_htmlbarsUtilVoidTagNames.default[tag.name] && !selfClosing) {
      // EngTag is also called by StartTag for void and self-closing tags (i.e.
      // <input> or <br />, so we need to check for that here. Otherwise, we would
      // throw an error for those cases.
      throw new Error("Invalid end tag " + formatEndTagInfo(tag) + " (void elements cannot have end tags).");
    } else if (element.tag === undefined) {
      throw new Error("Closing tag " + formatEndTagInfo(tag) + " without an open tag.");
    } else if (element.tag !== tag.name) {
      throw new Error("Closing tag " + formatEndTagInfo(tag) + " did not match last open tag `" + element.tag + "` (on line " + element.loc.start.line + ").");
    }
  }

  function formatEndTagInfo(tag) {
    return "`" + tag.name + "` (on line " + tag.loc.end.line + ")";
  }
});
enifed("htmlbars-syntax/traversal/errors", ["exports"], function (exports) {
  "use strict";

  exports.cannotRemoveNode = cannotRemoveNode;
  exports.cannotReplaceNode = cannotReplaceNode;
  exports.cannotReplaceOrRemoveInKeyHandlerYet = cannotReplaceOrRemoveInKeyHandlerYet;
  function TraversalError(message, node, parent, key) {
    this.name = "TraversalError";
    this.message = message;
    this.node = node;
    this.parent = parent;
    this.key = key;
  }

  TraversalError.prototype = Object.create(Error.prototype);
  TraversalError.prototype.constructor = TraversalError;

  exports.default = TraversalError;

  function cannotRemoveNode(node, parent, key) {
    return new TraversalError("Cannot remove a node unless it is part of an array", node, parent, key);
  }

  function cannotReplaceNode(node, parent, key) {
    return new TraversalError("Cannot replace a node with multiple nodes unless it is part of an array", node, parent, key);
  }

  function cannotReplaceOrRemoveInKeyHandlerYet(node, key) {
    return new TraversalError("Replacing and removing in key handlers is not yet supported.", node, null, key);
  }
});
enifed('htmlbars-syntax/traversal/traverse', ['exports', 'htmlbars-syntax/types/visitor-keys', 'htmlbars-syntax/traversal/errors'], function (exports, _htmlbarsSyntaxTypesVisitorKeys, _htmlbarsSyntaxTraversalErrors) {
  'use strict';

  exports.default = traverse;
  exports.normalizeVisitor = normalizeVisitor;

  function visitNode(visitor, node) {
    var handler = visitor[node.type] || visitor.All;
    var result = undefined;

    if (handler && handler.enter) {
      result = handler.enter.call(null, node);
    }

    if (result === undefined) {
      var keys = _htmlbarsSyntaxTypesVisitorKeys.default[node.type];

      for (var i = 0; i < keys.length; i++) {
        visitKey(visitor, handler, node, keys[i]);
      }

      if (handler && handler.exit) {
        result = handler.exit.call(null, node);
      }
    }

    return result;
  }

  function visitKey(visitor, handler, node, key) {
    var value = node[key];
    if (!value) {
      return;
    }

    var keyHandler = handler && (handler.keys[key] || handler.keys.All);
    var result = undefined;

    if (keyHandler && keyHandler.enter) {
      result = keyHandler.enter.call(null, node, key);
      if (result !== undefined) {
        throw _htmlbarsSyntaxTraversalErrors.cannotReplaceOrRemoveInKeyHandlerYet(node, key);
      }
    }

    if (Array.isArray(value)) {
      visitArray(visitor, value);
    } else {
      var _result = visitNode(visitor, value);
      if (_result !== undefined) {
        assignKey(node, key, _result);
      }
    }

    if (keyHandler && keyHandler.exit) {
      result = keyHandler.exit.call(null, node, key);
      if (result !== undefined) {
        throw _htmlbarsSyntaxTraversalErrors.cannotReplaceOrRemoveInKeyHandlerYet(node, key);
      }
    }
  }

  function visitArray(visitor, array) {
    for (var i = 0; i < array.length; i++) {
      var result = visitNode(visitor, array[i]);
      if (result !== undefined) {
        i += spliceArray(array, i, result) - 1;
      }
    }
  }

  function assignKey(node, key, result) {
    if (result === null) {
      throw _htmlbarsSyntaxTraversalErrors.cannotRemoveNode(node[key], node, key);
    } else if (Array.isArray(result)) {
      if (result.length === 1) {
        node[key] = result[0];
      } else {
        if (result.length === 0) {
          throw _htmlbarsSyntaxTraversalErrors.cannotRemoveNode(node[key], node, key);
        } else {
          throw _htmlbarsSyntaxTraversalErrors.cannotReplaceNode(node[key], node, key);
        }
      }
    } else {
      node[key] = result;
    }
  }

  function spliceArray(array, index, result) {
    if (result === null) {
      array.splice(index, 1);
      return 0;
    } else if (Array.isArray(result)) {
      array.splice.apply(array, [index, 1].concat(result));
      return result.length;
    } else {
      array.splice(index, 1, result);
      return 1;
    }
  }

  function traverse(node, visitor) {
    visitNode(normalizeVisitor(visitor), node);
  }

  function normalizeVisitor(visitor) {
    var normalizedVisitor = {};

    for (var type in visitor) {
      var handler = visitor[type] || visitor.All;
      var normalizedKeys = {};

      if (typeof handler === 'object') {
        var keys = handler.keys;
        if (keys) {
          for (var key in keys) {
            var keyHandler = keys[key];
            if (typeof keyHandler === 'object') {
              normalizedKeys[key] = {
                enter: typeof keyHandler.enter === 'function' ? keyHandler.enter : null,
                exit: typeof keyHandler.exit === 'function' ? keyHandler.exit : null
              };
            } else if (typeof keyHandler === 'function') {
              normalizedKeys[key] = {
                enter: keyHandler,
                exit: null
              };
            }
          }
        }

        normalizedVisitor[type] = {
          enter: typeof handler.enter === 'function' ? handler.enter : null,
          exit: typeof handler.exit === 'function' ? handler.exit : null,
          keys: normalizedKeys
        };
      } else if (typeof handler === 'function') {
        normalizedVisitor[type] = {
          enter: handler,
          exit: null,
          keys: normalizedKeys
        };
      }
    }

    return normalizedVisitor;
  }
});
enifed('htmlbars-syntax/traversal/walker', ['exports'], function (exports) {
  'use strict';

  function Walker(order) {
    this.order = order;
    this.stack = [];
  }

  exports.default = Walker;

  Walker.prototype.visit = function (node, callback) {
    if (!node) {
      return;
    }

    this.stack.push(node);

    if (this.order === 'post') {
      this.children(node, callback);
      callback(node, this);
    } else {
      callback(node, this);
      this.children(node, callback);
    }

    this.stack.pop();
  };

  var visitors = {
    Program: function (walker, node, callback) {
      for (var i = 0; i < node.body.length; i++) {
        walker.visit(node.body[i], callback);
      }
    },

    ElementNode: function (walker, node, callback) {
      for (var i = 0; i < node.children.length; i++) {
        walker.visit(node.children[i], callback);
      }
    },

    BlockStatement: function (walker, node, callback) {
      walker.visit(node.program, callback);
      walker.visit(node.inverse, callback);
    },

    ComponentNode: function (walker, node, callback) {
      walker.visit(node.program, callback);
    }
  };

  Walker.prototype.children = function (node, callback) {
    var visitor = visitors[node.type];
    if (visitor) {
      visitor(this, node, callback);
    }
  };
});
enifed('htmlbars-syntax/types/visitor-keys', ['exports'], function (exports) {
  'use strict';

  exports.default = {
    Program: ['body'],

    MustacheStatement: ['path', 'params', 'hash'],
    BlockStatement: ['path', 'params', 'hash', 'program', 'inverse'],
    ElementModifierStatement: ['path', 'params', 'hash'],
    PartialStatement: ['name', 'params', 'hash'],
    CommentStatement: [],
    ElementNode: ['attributes', 'modifiers', 'children'],
    ComponentNode: ['attributes', 'program'],
    AttrNode: ['value'],
    TextNode: [],

    ConcatStatement: ['parts'],
    SubExpression: ['path', 'params', 'hash'],
    PathExpression: [],

    StringLiteral: [],
    BooleanLiteral: [],
    NumberLiteral: [],
    NullLiteral: [],
    UndefinedLiteral: [],

    Hash: ['pairs'],
    HashPair: ['value']
  };
});
enifed('htmlbars-syntax/utils', ['exports', 'htmlbars-util/array-utils'], function (exports, _htmlbarsUtilArrayUtils) {
  'use strict';

  exports.parseComponentBlockParams = parseComponentBlockParams;
  exports.childrenFor = childrenFor;
  exports.appendChild = appendChild;
  exports.isHelper = isHelper;
  exports.unwrapMustache = unwrapMustache;

  // Regex to validate the identifier for block parameters.
  // Based on the ID validation regex in Handlebars.

  var ID_INVERSE_PATTERN = /[!"#%-,\.\/;->@\[-\^`\{-~]/;

  // Checks the component's attributes to see if it uses block params.
  // If it does, registers the block params with the program and
  // removes the corresponding attributes from the element.

  function parseComponentBlockParams(element, program) {
    var l = element.attributes.length;
    var attrNames = [];

    for (var i = 0; i < l; i++) {
      attrNames.push(element.attributes[i].name);
    }

    var asIndex = _htmlbarsUtilArrayUtils.indexOfArray(attrNames, 'as');

    if (asIndex !== -1 && l > asIndex && attrNames[asIndex + 1].charAt(0) === '|') {
      // Some basic validation, since we're doing the parsing ourselves
      var paramsString = attrNames.slice(asIndex).join(' ');
      if (paramsString.charAt(paramsString.length - 1) !== '|' || paramsString.match(/\|/g).length !== 2) {
        throw new Error('Invalid block parameters syntax: \'' + paramsString + '\'');
      }

      var params = [];
      for (i = asIndex + 1; i < l; i++) {
        var param = attrNames[i].replace(/\|/g, '');
        if (param !== '') {
          if (ID_INVERSE_PATTERN.test(param)) {
            throw new Error('Invalid identifier for block parameters: \'' + param + '\' in \'' + paramsString + '\'');
          }
          params.push(param);
        }
      }

      if (params.length === 0) {
        throw new Error('Cannot use zero block parameters: \'' + paramsString + '\'');
      }

      element.attributes = element.attributes.slice(0, asIndex);
      program.blockParams = params;
    }
  }

  function childrenFor(node) {
    if (node.type === 'Program') {
      return node.body;
    }
    if (node.type === 'ElementNode') {
      return node.children;
    }
  }

  function appendChild(parent, node) {
    childrenFor(parent).push(node);
  }

  function isHelper(mustache) {
    return mustache.params && mustache.params.length > 0 || mustache.hash && mustache.hash.pairs.length > 0;
  }

  function unwrapMustache(mustache) {
    if (isHelper(mustache)) {
      return mustache;
    } else {
      return mustache.path;
    }
  }
});
enifed("htmlbars-test-helpers", ["exports", "simple-html-tokenizer/index", "htmlbars-util/array-utils"], function (exports, _simpleHtmlTokenizerIndex, _htmlbarsUtilArrayUtils) {
  "use strict";

  exports.equalInnerHTML = equalInnerHTML;
  exports.equalHTML = equalHTML;
  exports.equalTokens = equalTokens;
  exports.normalizeInnerHTML = normalizeInnerHTML;
  exports.isCheckedInputHTML = isCheckedInputHTML;
  exports.getTextContent = getTextContent;

  function equalInnerHTML(fragment, html) {
    var actualHTML = normalizeInnerHTML(fragment.innerHTML);
    QUnit.push(actualHTML === html, actualHTML, html);
  }

  function equalHTML(node, html) {
    var fragment;
    if (!node.nodeType && node.length) {
      fragment = document.createDocumentFragment();
      while (node[0]) {
        fragment.appendChild(node[0]);
      }
    } else {
      fragment = node;
    }

    var div = document.createElement("div");
    div.appendChild(fragment.cloneNode(true));

    equalInnerHTML(div, html);
  }

  function generateTokens(fragmentOrHtml) {
    var div = document.createElement("div");
    if (typeof fragmentOrHtml === 'string') {
      div.innerHTML = fragmentOrHtml;
    } else {
      div.appendChild(fragmentOrHtml.cloneNode(true));
    }

    return { tokens: _simpleHtmlTokenizerIndex.tokenize(div.innerHTML), html: div.innerHTML };
  }

  function equalTokens(fragment, html, message) {
    if (fragment.fragment) {
      fragment = fragment.fragment;
    }
    if (html.fragment) {
      html = html.fragment;
    }

    var fragTokens = generateTokens(fragment);
    var htmlTokens = generateTokens(html);

    function normalizeTokens(token) {
      if (token.type === 'StartTag') {
        token.attributes = token.attributes.sort(function (a, b) {
          if (a[0] > b[0]) {
            return 1;
          }
          if (a[0] < b[0]) {
            return -1;
          }
          return 0;
        });
      }
    }

    _htmlbarsUtilArrayUtils.forEach(fragTokens.tokens, normalizeTokens);
    _htmlbarsUtilArrayUtils.forEach(htmlTokens.tokens, normalizeTokens);

    var msg = "Expected: " + html + "; Actual: " + fragTokens.html;

    if (message) {
      msg += " (" + message + ")";
    }

    deepEqual(fragTokens.tokens, htmlTokens.tokens, msg);
  }

  // detect side-effects of cloning svg elements in IE9-11
  var ieSVGInnerHTML = (function () {
    if (!document.createElementNS) {
      return false;
    }
    var div = document.createElement('div');
    var node = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    div.appendChild(node);
    var clone = div.cloneNode(true);
    return clone.innerHTML === '<svg xmlns="http://www.w3.org/2000/svg" />';
  })();

  function normalizeInnerHTML(actualHTML) {
    if (ieSVGInnerHTML) {
      // Replace `<svg xmlns="http://www.w3.org/2000/svg" height="50%" />` with `<svg height="50%"></svg>`, etc.
      // drop namespace attribute
      actualHTML = actualHTML.replace(/ xmlns="[^"]+"/, '');
      // replace self-closing elements
      actualHTML = actualHTML.replace(/<([^ >]+) [^\/>]*\/>/gi, function (tag, tagName) {
        return tag.slice(0, tag.length - 3) + '></' + tagName + '>';
      });
    }

    return actualHTML;
  }

  // detect weird IE8 checked element string
  var checkedInput = document.createElement('input');
  checkedInput.setAttribute('checked', 'checked');
  var checkedInputString = checkedInput.outerHTML;

  function isCheckedInputHTML(element) {
    equal(element.outerHTML, checkedInputString);
  }

  // check which property has the node's text content
  var textProperty = document.createElement('div').textContent === undefined ? 'innerText' : 'textContent';

  function getTextContent(el) {
    // textNode
    if (el.nodeType === 3) {
      return el.nodeValue;
    } else {
      return el[textProperty];
    }
  }
});
enifed('htmlbars-util', ['exports', 'htmlbars-util/safe-string', 'htmlbars-util/handlebars/utils', 'htmlbars-util/namespaces', 'htmlbars-util/morph-utils'], function (exports, _htmlbarsUtilSafeString, _htmlbarsUtilHandlebarsUtils, _htmlbarsUtilNamespaces, _htmlbarsUtilMorphUtils) {
  'use strict';

  exports.SafeString = _htmlbarsUtilSafeString.default;
  exports.escapeExpression = _htmlbarsUtilHandlebarsUtils.escapeExpression;
  exports.getAttrNamespace = _htmlbarsUtilNamespaces.getAttrNamespace;
  exports.validateChildMorphs = _htmlbarsUtilMorphUtils.validateChildMorphs;
  exports.linkParams = _htmlbarsUtilMorphUtils.linkParams;
  exports.dump = _htmlbarsUtilMorphUtils.dump;
});
enifed('htmlbars-util/array-utils', ['exports'], function (exports) {
  'use strict';

  exports.forEach = forEach;
  exports.map = map;

  function forEach(array, callback, binding) {
    var i, l;
    if (binding === undefined) {
      for (i = 0, l = array.length; i < l; i++) {
        callback(array[i], i, array);
      }
    } else {
      for (i = 0, l = array.length; i < l; i++) {
        callback.call(binding, array[i], i, array);
      }
    }
  }

  function map(array, callback) {
    var output = [];
    var i, l;

    for (i = 0, l = array.length; i < l; i++) {
      output.push(callback(array[i], i, array));
    }

    return output;
  }

  var getIdx;
  if (Array.prototype.indexOf) {
    getIdx = function (array, obj, from) {
      return array.indexOf(obj, from);
    };
  } else {
    getIdx = function (array, obj, from) {
      if (from === undefined || from === null) {
        from = 0;
      } else if (from < 0) {
        from = Math.max(0, array.length + from);
      }
      for (var i = from, l = array.length; i < l; i++) {
        if (array[i] === obj) {
          return i;
        }
      }
      return -1;
    };
  }

  var isArray = Array.isArray || function (array) {
    return Object.prototype.toString.call(array) === '[object Array]';
  };

  exports.isArray = isArray;
  var indexOfArray = getIdx;
  exports.indexOfArray = indexOfArray;
});
enifed('htmlbars-util/handlebars/safe-string', ['exports'], function (exports) {
  // Build out our basic SafeString type
  'use strict';

  function SafeString(string) {
    this.string = string;
  }

  SafeString.prototype.toString = SafeString.prototype.toHTML = function () {
    return '' + this.string;
  };

  exports.default = SafeString;
});
enifed('htmlbars-util/handlebars/utils', ['exports'], function (exports) {
  'use strict';

  exports.extend = extend;
  exports.indexOf = indexOf;
  exports.escapeExpression = escapeExpression;
  exports.isEmpty = isEmpty;
  exports.blockParams = blockParams;
  exports.appendContextPath = appendContextPath;
  var escape = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#x27;',
    '`': '&#x60;'
  };

  var badChars = /[&<>"'`]/g,
      possible = /[&<>"'`]/;

  function escapeChar(chr) {
    return escape[chr];
  }

  function extend(obj /* , ...source */) {
    for (var i = 1; i < arguments.length; i++) {
      for (var key in arguments[i]) {
        if (Object.prototype.hasOwnProperty.call(arguments[i], key)) {
          obj[key] = arguments[i][key];
        }
      }
    }

    return obj;
  }

  var toString = Object.prototype.toString;

  exports.toString = toString;
  // Sourced from lodash
  // https://github.com/bestiejs/lodash/blob/master/LICENSE.txt
  /*eslint-disable func-style, no-var */
  var isFunction = function (value) {
    return typeof value === 'function';
  };
  // fallback for older versions of Chrome and Safari
  /* istanbul ignore next */
  if (isFunction(/x/)) {
    exports.isFunction = isFunction = function (value) {
      return typeof value === 'function' && toString.call(value) === '[object Function]';
    };
  }
  var isFunction;
  exports.isFunction = isFunction;
  /*eslint-enable func-style, no-var */

  /* istanbul ignore next */
  var isArray = Array.isArray || function (value) {
    return value && typeof value === 'object' ? toString.call(value) === '[object Array]' : false;
  };

  exports.isArray = isArray;
  // Older IE versions do not directly support indexOf so we must implement our own, sadly.

  function indexOf(array, value) {
    for (var i = 0, len = array.length; i < len; i++) {
      if (array[i] === value) {
        return i;
      }
    }
    return -1;
  }

  function escapeExpression(string) {
    if (typeof string !== 'string') {
      // don't escape SafeStrings, since they're already safe
      if (string && string.toHTML) {
        return string.toHTML();
      } else if (string == null) {
        return '';
      } else if (!string) {
        return string + '';
      }

      // Force a string conversion as this will be done by the append regardless and
      // the regex test will do this transparently behind the scenes, causing issues if
      // an object's to string has escaped characters in it.
      string = '' + string;
    }

    if (!possible.test(string)) {
      return string;
    }
    return string.replace(badChars, escapeChar);
  }

  function isEmpty(value) {
    if (!value && value !== 0) {
      return true;
    } else if (isArray(value) && value.length === 0) {
      return true;
    } else {
      return false;
    }
  }

  function blockParams(params, ids) {
    params.path = ids;
    return params;
  }

  function appendContextPath(contextPath, id) {
    return (contextPath ? contextPath + '.' : '') + id;
  }
});
enifed("htmlbars-util/morph-utils", ["exports"], function (exports) {
  /*globals console*/

  "use strict";

  exports.visitChildren = visitChildren;
  exports.validateChildMorphs = validateChildMorphs;
  exports.linkParams = linkParams;
  exports.dump = dump;

  function visitChildren(nodes, callback) {
    if (!nodes || nodes.length === 0) {
      return;
    }

    nodes = nodes.slice();

    while (nodes.length) {
      var node = nodes.pop();
      callback(node);

      if (node.childNodes) {
        nodes.push.apply(nodes, node.childNodes);
      } else if (node.firstChildMorph) {
        var current = node.firstChildMorph;

        while (current) {
          nodes.push(current);
          current = current.nextMorph;
        }
      } else if (node.morphList) {
        var current = node.morphList.firstChildMorph;

        while (current) {
          nodes.push(current);
          current = current.nextMorph;
        }
      }
    }
  }

  function validateChildMorphs(env, morph, visitor) {
    var morphList = morph.morphList;
    if (morph.morphList) {
      var current = morphList.firstChildMorph;

      while (current) {
        var next = current.nextMorph;
        validateChildMorphs(env, current, visitor);
        current = next;
      }
    } else if (morph.lastResult) {
      morph.lastResult.revalidateWith(env, undefined, undefined, undefined, visitor);
    } else if (morph.childNodes) {
      // This means that the childNodes were wired up manually
      for (var i = 0, l = morph.childNodes.length; i < l; i++) {
        validateChildMorphs(env, morph.childNodes[i], visitor);
      }
    }
  }

  function linkParams(env, scope, morph, path, params, hash) {
    if (morph.linkedParams) {
      return;
    }

    if (env.hooks.linkRenderNode(morph, env, scope, path, params, hash)) {
      morph.linkedParams = { params: params, hash: hash };
    }
  }

  function dump(node) {
    console.group(node, node.isDirty);

    if (node.childNodes) {
      map(node.childNodes, dump);
    } else if (node.firstChildMorph) {
      var current = node.firstChildMorph;

      while (current) {
        dump(current);
        current = current.nextMorph;
      }
    } else if (node.morphList) {
      dump(node.morphList);
    }

    console.groupEnd();
  }

  function map(nodes, cb) {
    for (var i = 0, l = nodes.length; i < l; i++) {
      cb(nodes[i]);
    }
  }
});
enifed('htmlbars-util/namespaces', ['exports'], function (exports) {
  // ref http://dev.w3.org/html5/spec-LC/namespaces.html
  'use strict';

  exports.getAttrNamespace = getAttrNamespace;
  var defaultNamespaces = {
    html: 'http://www.w3.org/1999/xhtml',
    mathml: 'http://www.w3.org/1998/Math/MathML',
    svg: 'http://www.w3.org/2000/svg',
    xlink: 'http://www.w3.org/1999/xlink',
    xml: 'http://www.w3.org/XML/1998/namespace'
  };

  function getAttrNamespace(attrName, detectedNamespace) {
    if (detectedNamespace) {
      return detectedNamespace;
    }

    var namespace;

    var colonIndex = attrName.indexOf(':');
    if (colonIndex !== -1) {
      var prefix = attrName.slice(0, colonIndex);
      namespace = defaultNamespaces[prefix];
    }

    return namespace || null;
  }
});
enifed("htmlbars-util/object-utils", ["exports"], function (exports) {
  "use strict";

  exports.merge = merge;
  exports.shallowCopy = shallowCopy;
  exports.keySet = keySet;
  exports.keyLength = keyLength;

  function merge(options, defaults) {
    for (var prop in defaults) {
      if (options.hasOwnProperty(prop)) {
        continue;
      }
      options[prop] = defaults[prop];
    }
    return options;
  }

  function shallowCopy(obj) {
    return merge({}, obj);
  }

  function keySet(obj) {
    var set = {};

    for (var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
        set[prop] = true;
      }
    }

    return set;
  }

  function keyLength(obj) {
    var count = 0;

    for (var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
        count++;
      }
    }

    return count;
  }
});
enifed("htmlbars-util/quoting", ["exports"], function (exports) {
  "use strict";

  exports.hash = hash;
  exports.repeat = repeat;
  function escapeString(str) {
    str = str.replace(/\\/g, "\\\\");
    str = str.replace(/"/g, '\\"');
    str = str.replace(/\n/g, "\\n");
    return str;
  }

  exports.escapeString = escapeString;

  function string(str) {
    return '"' + escapeString(str) + '"';
  }

  exports.string = string;

  function array(a) {
    return "[" + a + "]";
  }

  exports.array = array;

  function hash(pairs) {
    return "{" + pairs.join(", ") + "}";
  }

  function repeat(chars, times) {
    var str = "";
    while (times--) {
      str += chars;
    }
    return str;
  }
});
enifed('htmlbars-util/safe-string', ['exports', 'htmlbars-util/handlebars/safe-string'], function (exports, _htmlbarsUtilHandlebarsSafeString) {
  'use strict';

  exports.default = _htmlbarsUtilHandlebarsSafeString.default;
});
enifed("htmlbars-util/template-utils", ["exports", "htmlbars-util/morph-utils", "htmlbars-runtime/render"], function (exports, _htmlbarsUtilMorphUtils, _htmlbarsRuntimeRender) {
  "use strict";

  exports.RenderState = RenderState;
  exports.blockFor = blockFor;
  exports.renderAndCleanup = renderAndCleanup;
  exports.clearMorph = clearMorph;
  exports.clearMorphList = clearMorphList;

  function RenderState(renderNode, morphList) {
    // The morph list that is no longer needed and can be
    // destroyed.
    this.morphListToClear = morphList;

    // The morph list that needs to be pruned of any items
    // that were not yielded on a subsequent render.
    this.morphListToPrune = null;

    // A map of morphs for each item yielded in during this
    // rendering pass. Any morphs in the DOM but not in this map
    // will be pruned during cleanup.
    this.handledMorphs = {};
    this.collisions = undefined;

    // The morph to clear once rendering is complete. By
    // default, we set this to the previous morph (to catch
    // the case where nothing is yielded; in that case, we
    // should just clear the morph). Otherwise this gets set
    // to null if anything is rendered.
    this.morphToClear = renderNode;

    this.shadowOptions = null;
  }

  function Block(render, template, blockOptions) {
    this.render = render;
    this.template = template;
    this.blockOptions = blockOptions;
    this.arity = template.arity;
  }

  Block.prototype.invoke = function (env, blockArguments, _self, renderNode, parentScope, visitor) {
    if (renderNode.lastResult) {
      renderNode.lastResult.revalidateWith(env, undefined, _self, blockArguments, visitor);
    } else {
      this._firstRender(env, blockArguments, _self, renderNode, parentScope);
    }
  };

  Block.prototype._firstRender = function (env, blockArguments, _self, renderNode, parentScope) {
    var options = { renderState: new RenderState(renderNode) };
    var render = this.render;
    var template = this.template;
    var scope = this.blockOptions.scope;

    var shadowScope = scope ? env.hooks.createChildScope(scope) : env.hooks.createFreshScope();

    env.hooks.bindShadowScope(env, parentScope, shadowScope, this.blockOptions.options);

    if (_self !== undefined) {
      env.hooks.bindSelf(env, shadowScope, _self);
    } else if (this.blockOptions.self !== undefined) {
      env.hooks.bindSelf(env, shadowScope, this.blockOptions.self);
    }

    bindBlocks(env, shadowScope, this.blockOptions.yieldTo);

    renderAndCleanup(renderNode, env, options, null, function () {
      options.renderState.morphToClear = null;
      var renderOptions = new _htmlbarsRuntimeRender.RenderOptions(renderNode, undefined, blockArguments);
      render(template, env, shadowScope, renderOptions);
    });
  };

  function blockFor(render, template, blockOptions) {
    return new Block(render, template, blockOptions);
  }

  function bindBlocks(env, shadowScope, blocks) {
    if (!blocks) {
      return;
    }
    if (blocks instanceof Block) {
      env.hooks.bindBlock(env, shadowScope, blocks);
    } else {
      for (var name in blocks) {
        if (blocks.hasOwnProperty(name)) {
          env.hooks.bindBlock(env, shadowScope, blocks[name], name);
        }
      }
    }
  }

  function renderAndCleanup(morph, env, options, shadowOptions, callback) {
    // The RenderState object is used to collect information about what the
    // helper or hook being invoked has yielded. Once it has finished either
    // yielding multiple items (via yieldItem) or a single template (via
    // yieldTemplate), we detect what was rendered and how it differs from
    // the previous render, cleaning up old state in DOM as appropriate.
    var renderState = options.renderState;
    renderState.collisions = undefined;
    renderState.shadowOptions = shadowOptions;

    // Invoke the callback, instructing it to save information about what it
    // renders into RenderState.
    var result = callback(options);

    // The hook can opt-out of cleanup if it handled cleanup itself.
    if (result && result.handled) {
      return;
    }

    var morphMap = morph.morphMap;

    // Walk the morph list, clearing any items that were yielded in a previous
    // render but were not yielded during this render.
    var morphList = renderState.morphListToPrune;
    if (morphList) {
      var handledMorphs = renderState.handledMorphs;
      var item = morphList.firstChildMorph;

      while (item) {
        var next = item.nextMorph;

        // If we don't see the key in handledMorphs, it wasn't
        // yielded in and we can safely remove it from DOM.
        if (!(item.key in handledMorphs)) {
          morphMap[item.key] = undefined;
          clearMorph(item, env, true);
          item.destroy();
        }

        item = next;
      }
    }

    morphList = renderState.morphListToClear;
    if (morphList) {
      clearMorphList(morphList, morph, env);
    }

    var toClear = renderState.morphToClear;
    if (toClear) {
      clearMorph(toClear, env);
    }
  }

  function clearMorph(morph, env, destroySelf) {
    var cleanup = env.hooks.cleanupRenderNode;
    var destroy = env.hooks.destroyRenderNode;
    var willCleanup = env.hooks.willCleanupTree;
    var didCleanup = env.hooks.didCleanupTree;

    function destroyNode(node) {
      if (cleanup) {
        cleanup(node);
      }
      if (destroy) {
        destroy(node);
      }
    }

    if (willCleanup) {
      willCleanup(env, morph, destroySelf);
    }
    if (cleanup) {
      cleanup(morph);
    }
    if (destroySelf && destroy) {
      destroy(morph);
    }

    _htmlbarsUtilMorphUtils.visitChildren(morph.childNodes, destroyNode);

    // TODO: Deal with logical children that are not in the DOM tree
    morph.clear();
    if (didCleanup) {
      didCleanup(env, morph, destroySelf);
    }

    morph.lastResult = null;
    morph.lastYielded = null;
    morph.childNodes = null;
  }

  function clearMorphList(morphList, morph, env) {
    var item = morphList.firstChildMorph;

    while (item) {
      var next = item.nextMorph;
      morph.morphMap[item.key] = undefined;
      clearMorph(item, env, true);
      item.destroy();

      item = next;
    }

    // Remove the MorphList from the morph.
    morphList.clear();
    morph.morphList = null;
  }
});
enifed("htmlbars-util/void-tag-names", ["exports", "htmlbars-util/array-utils"], function (exports, _htmlbarsUtilArrayUtils) {
  "use strict";

  // The HTML elements in this list are speced by
  // http://www.w3.org/TR/html-markup/syntax.html#syntax-elements,
  // and will be forced to close regardless of if they have a
  // self-closing /> at the end.
  var voidTagNames = "area base br col command embed hr img input keygen link meta param source track wbr";
  var voidMap = {};

  _htmlbarsUtilArrayUtils.forEach(voidTagNames.split(" "), function (tagName) {
    voidMap[tagName] = true;
  });

  exports.default = voidMap;
});
enifed('morph-range', ['exports', 'morph-range/utils'], function (exports, _morphRangeUtils) {
  'use strict';

  // constructor just initializes the fields
  // use one of the static initializers to create a valid morph.
  function Morph(domHelper, contextualElement) {
    this.domHelper = domHelper;
    // context if content if current content is detached
    this.contextualElement = contextualElement;
    // inclusive range of morph
    // these should be nodeType 1, 3, or 8
    this.firstNode = null;
    this.lastNode = null;

    // flag to force text to setContent to be treated as html
    this.parseTextAsHTML = false;

    // morph list graph
    this.parentMorphList = null;
    this.previousMorph = null;
    this.nextMorph = null;
  }

  Morph.empty = function (domHelper, contextualElement) {
    var morph = new Morph(domHelper, contextualElement);
    morph.clear();
    return morph;
  };

  Morph.create = function (domHelper, contextualElement, node) {
    var morph = new Morph(domHelper, contextualElement);
    morph.setNode(node);
    return morph;
  };

  Morph.attach = function (domHelper, contextualElement, firstNode, lastNode) {
    var morph = new Morph(domHelper, contextualElement);
    morph.setRange(firstNode, lastNode);
    return morph;
  };

  Morph.prototype.setContent = function Morph$setContent(content) {
    if (content === null || content === undefined) {
      return this.clear();
    }

    var type = typeof content;
    switch (type) {
      case 'string':
        if (this.parseTextAsHTML) {
          return this.domHelper.setMorphHTML(this, content);
        }
        return this.setText(content);
      case 'object':
        if (typeof content.nodeType === 'number') {
          return this.setNode(content);
        }
        /* Handlebars.SafeString */
        if (typeof content.toHTML === 'function') {
          return this.setHTML(content.toHTML());
        }
        if (this.parseTextAsHTML) {
          return this.setHTML(content.toString());
        }
      /* falls through */
      case 'boolean':
      case 'number':
        return this.setText(content.toString());
      case 'function':
        raiseCannotBindToFunction(content);
      default:
        throw new TypeError('unsupported content');
    }
  };

  function raiseCannotBindToFunction(content) {
    var functionName = content.name;
    var message;

    if (functionName) {
      message = 'Unsupported Content: Cannot bind to function `' + functionName + '`';
    } else {
      message = 'Unsupported Content: Cannot bind to function';
    }

    throw new TypeError(message);
  }

  Morph.prototype.clear = function Morph$clear() {
    var node = this.setNode(this.domHelper.createComment(''));
    return node;
  };

  Morph.prototype.setText = function Morph$setText(text) {
    var firstNode = this.firstNode;
    var lastNode = this.lastNode;

    if (firstNode && lastNode === firstNode && firstNode.nodeType === 3) {
      firstNode.nodeValue = text;
      return firstNode;
    }

    return this.setNode(text ? this.domHelper.createTextNode(text) : this.domHelper.createComment(''));
  };

  Morph.prototype.setNode = function Morph$setNode(newNode) {
    var firstNode, lastNode;
    switch (newNode.nodeType) {
      case 3:
        firstNode = newNode;
        lastNode = newNode;
        break;
      case 11:
        firstNode = newNode.firstChild;
        lastNode = newNode.lastChild;
        if (firstNode === null) {
          firstNode = this.domHelper.createComment('');
          newNode.appendChild(firstNode);
          lastNode = firstNode;
        }
        break;
      default:
        firstNode = newNode;
        lastNode = newNode;
        break;
    }

    this.setRange(firstNode, lastNode);

    return newNode;
  };

  Morph.prototype.setRange = function (firstNode, lastNode) {
    var previousFirstNode = this.firstNode;
    if (previousFirstNode !== null) {

      var parentNode = previousFirstNode.parentNode;
      if (parentNode !== null) {
        _morphRangeUtils.insertBefore(parentNode, firstNode, lastNode, previousFirstNode);
        _morphRangeUtils.clear(parentNode, previousFirstNode, this.lastNode);
      }
    }

    this.firstNode = firstNode;
    this.lastNode = lastNode;

    if (this.parentMorphList) {
      this._syncFirstNode();
      this._syncLastNode();
    }
  };

  Morph.prototype.destroy = function Morph$destroy() {
    this.unlink();

    var firstNode = this.firstNode;
    var lastNode = this.lastNode;
    var parentNode = firstNode && firstNode.parentNode;

    this.firstNode = null;
    this.lastNode = null;

    _morphRangeUtils.clear(parentNode, firstNode, lastNode);
  };

  Morph.prototype.unlink = function Morph$unlink() {
    var parentMorphList = this.parentMorphList;
    var previousMorph = this.previousMorph;
    var nextMorph = this.nextMorph;

    if (previousMorph) {
      if (nextMorph) {
        previousMorph.nextMorph = nextMorph;
        nextMorph.previousMorph = previousMorph;
      } else {
        previousMorph.nextMorph = null;
        parentMorphList.lastChildMorph = previousMorph;
      }
    } else {
      if (nextMorph) {
        nextMorph.previousMorph = null;
        parentMorphList.firstChildMorph = nextMorph;
      } else if (parentMorphList) {
        parentMorphList.lastChildMorph = parentMorphList.firstChildMorph = null;
      }
    }

    this.parentMorphList = null;
    this.nextMorph = null;
    this.previousMorph = null;

    if (parentMorphList && parentMorphList.mountedMorph) {
      if (!parentMorphList.firstChildMorph) {
        // list is empty
        parentMorphList.mountedMorph.clear();
        return;
      } else {
        parentMorphList.firstChildMorph._syncFirstNode();
        parentMorphList.lastChildMorph._syncLastNode();
      }
    }
  };

  Morph.prototype.setHTML = function (text) {
    var fragment = this.domHelper.parseHTML(text, this.contextualElement);
    return this.setNode(fragment);
  };

  Morph.prototype.setMorphList = function Morph$appendMorphList(morphList) {
    morphList.mountedMorph = this;
    this.clear();

    var originalFirstNode = this.firstNode;

    if (morphList.firstChildMorph) {
      this.firstNode = morphList.firstChildMorph.firstNode;
      this.lastNode = morphList.lastChildMorph.lastNode;

      var current = morphList.firstChildMorph;

      while (current) {
        var next = current.nextMorph;
        current.insertBeforeNode(originalFirstNode, null);
        current = next;
      }
      originalFirstNode.parentNode.removeChild(originalFirstNode);
    }
  };

  Morph.prototype._syncFirstNode = function Morph$syncFirstNode() {
    var morph = this;
    var parentMorphList;
    while (parentMorphList = morph.parentMorphList) {
      if (parentMorphList.mountedMorph === null) {
        break;
      }
      if (morph !== parentMorphList.firstChildMorph) {
        break;
      }
      if (morph.firstNode === parentMorphList.mountedMorph.firstNode) {
        break;
      }

      parentMorphList.mountedMorph.firstNode = morph.firstNode;

      morph = parentMorphList.mountedMorph;
    }
  };

  Morph.prototype._syncLastNode = function Morph$syncLastNode() {
    var morph = this;
    var parentMorphList;
    while (parentMorphList = morph.parentMorphList) {
      if (parentMorphList.mountedMorph === null) {
        break;
      }
      if (morph !== parentMorphList.lastChildMorph) {
        break;
      }
      if (morph.lastNode === parentMorphList.mountedMorph.lastNode) {
        break;
      }

      parentMorphList.mountedMorph.lastNode = morph.lastNode;

      morph = parentMorphList.mountedMorph;
    }
  };

  Morph.prototype.insertBeforeNode = function Morph$insertBeforeNode(parentNode, refNode) {
    _morphRangeUtils.insertBefore(parentNode, this.firstNode, this.lastNode, refNode);
  };

  Morph.prototype.appendToNode = function Morph$appendToNode(parentNode) {
    _morphRangeUtils.insertBefore(parentNode, this.firstNode, this.lastNode, null);
  };

  exports.default = Morph;
});
enifed('morph-range/morph-list', ['exports', 'morph-range/utils'], function (exports, _morphRangeUtils) {
  'use strict';

  function MorphList() {
    // morph graph
    this.firstChildMorph = null;
    this.lastChildMorph = null;

    this.mountedMorph = null;
  }

  var prototype = MorphList.prototype;

  prototype.clear = function MorphList$clear() {
    var current = this.firstChildMorph;

    while (current) {
      var next = current.nextMorph;
      current.previousMorph = null;
      current.nextMorph = null;
      current.parentMorphList = null;
      current = next;
    }

    this.firstChildMorph = this.lastChildMorph = null;
  };

  prototype.destroy = function MorphList$destroy() {};

  prototype.appendMorph = function MorphList$appendMorph(morph) {
    this.insertBeforeMorph(morph, null);
  };

  prototype.insertBeforeMorph = function MorphList$insertBeforeMorph(morph, referenceMorph) {
    if (morph.parentMorphList !== null) {
      morph.unlink();
    }
    if (referenceMorph && referenceMorph.parentMorphList !== this) {
      throw new Error('The morph before which the new morph is to be inserted is not a child of this morph.');
    }

    var mountedMorph = this.mountedMorph;

    if (mountedMorph) {

      var parentNode = mountedMorph.firstNode.parentNode;
      var referenceNode = referenceMorph ? referenceMorph.firstNode : mountedMorph.lastNode.nextSibling;

      _morphRangeUtils.insertBefore(parentNode, morph.firstNode, morph.lastNode, referenceNode);

      // was not in list mode replace current content
      if (!this.firstChildMorph) {
        _morphRangeUtils.clear(this.mountedMorph.firstNode.parentNode, this.mountedMorph.firstNode, this.mountedMorph.lastNode);
      }
    }

    morph.parentMorphList = this;

    var previousMorph = referenceMorph ? referenceMorph.previousMorph : this.lastChildMorph;
    if (previousMorph) {
      previousMorph.nextMorph = morph;
      morph.previousMorph = previousMorph;
    } else {
      this.firstChildMorph = morph;
    }

    if (referenceMorph) {
      referenceMorph.previousMorph = morph;
      morph.nextMorph = referenceMorph;
    } else {
      this.lastChildMorph = morph;
    }

    this.firstChildMorph._syncFirstNode();
    this.lastChildMorph._syncLastNode();
  };

  prototype.removeChildMorph = function MorphList$removeChildMorph(morph) {
    if (morph.parentMorphList !== this) {
      throw new Error("Cannot remove a morph from a parent it is not inside of");
    }

    morph.destroy();
  };

  exports.default = MorphList;
});
enifed('morph-range/morph-list.umd', ['exports', 'morph-range/morph-list'], function (exports, _morphRangeMorphList) {
  'use strict';

  (function (root, factory) {
    if (typeof define === 'function' && define.amd) {
      define([], factory);
    } else if (typeof exports === 'object') {
      module.exports = factory();
    } else {
      root.MorphList = factory();
    }
  })(undefined, function () {
    return _morphRangeMorphList.default;
  });
});
enifed("morph-range/utils", ["exports"], function (exports) {
  // inclusive of both nodes
  "use strict";

  exports.clear = clear;
  exports.insertBefore = insertBefore;

  function clear(parentNode, firstNode, lastNode) {
    if (!parentNode) {
      return;
    }

    var node = firstNode;
    var nextNode;
    do {
      nextNode = node.nextSibling;
      parentNode.removeChild(node);
      if (node === lastNode) {
        break;
      }
      node = nextNode;
    } while (node);
  }

  function insertBefore(parentNode, firstNode, lastNode, refNode) {
    var node = firstNode;
    var nextNode;
    do {
      nextNode = node.nextSibling;
      parentNode.insertBefore(node, refNode);
      if (node === lastNode) {
        break;
      }
      node = nextNode;
    } while (node);
  }
});
enifed("simple-html-tokenizer/entity-parser", ["exports"], function (exports) {
  "use strict";

  function EntityParser(named) {
    this.named = named;
  }

  var HEXCHARCODE = /^#[xX]([A-Fa-f0-9]+)$/;
  var CHARCODE = /^#([0-9]+)$/;
  var NAMED = /^([A-Za-z0-9]+)$/;

  EntityParser.prototype.parse = function (entity) {
    if (!entity) {
      return;
    }
    var matches = entity.match(HEXCHARCODE);
    if (matches) {
      return String.fromCharCode(parseInt(matches[1], 16));
    }
    matches = entity.match(CHARCODE);
    if (matches) {
      return String.fromCharCode(parseInt(matches[1], 10));
    }
    matches = entity.match(NAMED);
    if (matches) {
      return this.named[matches[1]];
    }
  };

  exports.default = EntityParser;
});
enifed('simple-html-tokenizer/evented-tokenizer', ['exports', 'simple-html-tokenizer/utils'], function (exports, _simpleHtmlTokenizerUtils) {
  'use strict';

  function EventedTokenizer(delegate, entityParser) {
    this.delegate = delegate;
    this.entityParser = entityParser;

    this.state = null;
    this.input = null;

    this.index = -1;
    this.line = -1;
    this.column = -1;
    this.tagLine = -1;
    this.tagColumn = -1;

    this.reset();
  }

  EventedTokenizer.prototype = {
    reset: function () {
      this.state = 'beforeData';
      this.input = '';

      this.index = 0;
      this.line = 1;
      this.column = 0;

      this.tagLine = -1;
      this.tagColumn = -1;

      this.delegate.reset();
    },

    tokenize: function (input) {
      this.reset();
      this.tokenizePart(input);
      this.tokenizeEOF();
    },

    tokenizePart: function (input) {
      this.input += _simpleHtmlTokenizerUtils.preprocessInput(input);

      while (this.index < this.input.length) {
        this.states[this.state].call(this);
      }
    },

    tokenizeEOF: function () {
      this.flushData();
    },

    flushData: function () {
      if (this.state === 'data') {
        this.delegate.finishData();
        this.state = 'beforeData';
      }
    },

    peek: function () {
      return this.input.charAt(this.index);
    },

    consume: function () {
      var char = this.peek();

      this.index++;

      if (char === "\n") {
        this.line++;
        this.column = 0;
      } else {
        this.column++;
      }

      return char;
    },

    consumeCharRef: function () {
      var endIndex = this.input.indexOf(';', this.index);
      if (endIndex === -1) {
        return;
      }
      var entity = this.input.slice(this.index, endIndex);
      var chars = this.entityParser.parse(entity);
      if (chars) {
        this.index = endIndex + 1;
        return chars;
      }
    },

    markTagStart: function () {
      this.tagLine = this.line;
      this.tagColumn = this.column;
    },

    states: {
      beforeData: function () {
        var char = this.peek();

        if (char === "<") {
          this.state = 'tagOpen';
          this.markTagStart();
          this.consume();
        } else {
          this.state = 'data';
          this.delegate.beginData();
        }
      },

      data: function () {
        var char = this.peek();

        if (char === "<") {
          this.delegate.finishData();
          this.state = 'tagOpen';
          this.markTagStart();
          this.consume();
        } else if (char === "&") {
          this.consume();
          this.delegate.appendToData(this.consumeCharRef() || "&");
        } else {
          this.consume();
          this.delegate.appendToData(char);
        }
      },

      tagOpen: function () {
        var char = this.consume();

        if (char === "!") {
          this.state = 'markupDeclaration';
        } else if (char === "/") {
          this.state = 'endTagOpen';
        } else if (_simpleHtmlTokenizerUtils.isAlpha(char)) {
          this.state = 'tagName';
          this.delegate.beginStartTag();
          this.delegate.appendToTagName(char.toLowerCase());
        }
      },

      markupDeclaration: function () {
        var char = this.consume();

        if (char === "-" && this.input.charAt(this.index) === "-") {
          this.index++;
          this.state = 'commentStart';
          this.delegate.beginComment();
        }
      },

      commentStart: function () {
        var char = this.consume();

        if (char === "-") {
          this.state = 'commentStartDash';
        } else if (char === ">") {
          this.delegate.finishComment();
          this.state = 'beforeData';
        } else {
          this.delegate.appendToCommentData(char);
          this.state = 'comment';
        }
      },

      commentStartDash: function () {
        var char = this.consume();

        if (char === "-") {
          this.state = 'commentEnd';
        } else if (char === ">") {
          this.delegate.finishComment();
          this.state = 'beforeData';
        } else {
          this.delegate.appendToCommentData("-");
          this.state = 'comment';
        }
      },

      comment: function () {
        var char = this.consume();

        if (char === "-") {
          this.state = 'commentEndDash';
        } else {
          this.delegate.appendToCommentData(char);
        }
      },

      commentEndDash: function () {
        var char = this.consume();

        if (char === "-") {
          this.state = 'commentEnd';
        } else {
          this.delegate.appendToCommentData("-" + char);
          this.state = 'comment';
        }
      },

      commentEnd: function () {
        var char = this.consume();

        if (char === ">") {
          this.delegate.finishComment();
          this.state = 'beforeData';
        } else {
          this.delegate.appendToCommentData("--" + char);
          this.state = 'comment';
        }
      },

      tagName: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {
          this.state = 'beforeAttributeName';
        } else if (char === "/") {
          this.state = 'selfClosingStartTag';
        } else if (char === ">") {
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.delegate.appendToTagName(char);
        }
      },

      beforeAttributeName: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {
          return;
        } else if (char === "/") {
          this.state = 'selfClosingStartTag';
        } else if (char === ">") {
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.state = 'attributeName';
          this.delegate.beginAttribute();
          this.delegate.appendToAttributeName(char);
        }
      },

      attributeName: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {
          this.state = 'afterAttributeName';
        } else if (char === "/") {
          this.delegate.beginAttributeValue(false);
          this.delegate.finishAttributeValue();
          this.state = 'selfClosingStartTag';
        } else if (char === "=") {
          this.state = 'beforeAttributeValue';
        } else if (char === ">") {
          this.delegate.beginAttributeValue(false);
          this.delegate.finishAttributeValue();
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.delegate.appendToAttributeName(char);
        }
      },

      afterAttributeName: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {
          return;
        } else if (char === "/") {
          this.delegate.beginAttributeValue(false);
          this.delegate.finishAttributeValue();
          this.state = 'selfClosingStartTag';
        } else if (char === "=") {
          this.state = 'beforeAttributeValue';
        } else if (char === ">") {
          this.delegate.beginAttributeValue(false);
          this.delegate.finishAttributeValue();
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.delegate.beginAttributeValue(false);
          this.delegate.finishAttributeValue();
          this.state = 'attributeName';
          this.delegate.beginAttribute();
          this.delegate.appendToAttributeName(char);
        }
      },

      beforeAttributeValue: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {} else if (char === '"') {
          this.state = 'attributeValueDoubleQuoted';
          this.delegate.beginAttributeValue(true);
        } else if (char === "'") {
          this.state = 'attributeValueSingleQuoted';
          this.delegate.beginAttributeValue(true);
        } else if (char === ">") {
          this.delegate.beginAttributeValue(false);
          this.delegate.finishAttributeValue();
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.state = 'attributeValueUnquoted';
          this.delegate.beginAttributeValue(false);
          this.delegate.appendToAttributeValue(char);
        }
      },

      attributeValueDoubleQuoted: function () {
        var char = this.consume();

        if (char === '"') {
          this.delegate.finishAttributeValue();
          this.state = 'afterAttributeValueQuoted';
        } else if (char === "&") {
          this.delegate.appendToAttributeValue(this.consumeCharRef('"') || "&");
        } else {
          this.delegate.appendToAttributeValue(char);
        }
      },

      attributeValueSingleQuoted: function () {
        var char = this.consume();

        if (char === "'") {
          this.delegate.finishAttributeValue();
          this.state = 'afterAttributeValueQuoted';
        } else if (char === "&") {
          this.delegate.appendToAttributeValue(this.consumeCharRef("'") || "&");
        } else {
          this.delegate.appendToAttributeValue(char);
        }
      },

      attributeValueUnquoted: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {
          this.delegate.finishAttributeValue();
          this.state = 'beforeAttributeName';
        } else if (char === "&") {
          this.delegate.appendToAttributeValue(this.consumeCharRef(">") || "&");
        } else if (char === ">") {
          this.delegate.finishAttributeValue();
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.delegate.appendToAttributeValue(char);
        }
      },

      afterAttributeValueQuoted: function () {
        var char = this.peek();

        if (_simpleHtmlTokenizerUtils.isSpace(char)) {
          this.consume();
          this.state = 'beforeAttributeName';
        } else if (char === "/") {
          this.consume();
          this.state = 'selfClosingStartTag';
        } else if (char === ">") {
          this.consume();
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.state = 'beforeAttributeName';
        }
      },

      selfClosingStartTag: function () {
        var char = this.peek();

        if (char === ">") {
          this.consume();
          this.delegate.markTagAsSelfClosing();
          this.delegate.finishTag();
          this.state = 'beforeData';
        } else {
          this.state = 'beforeAttributeName';
        }
      },

      endTagOpen: function () {
        var char = this.consume();

        if (_simpleHtmlTokenizerUtils.isAlpha(char)) {
          this.state = 'tagName';
          this.delegate.beginEndTag();
          this.delegate.appendToTagName(char.toLowerCase());
        }
      }
    }
  };

  exports.default = EventedTokenizer;
});
enifed("simple-html-tokenizer/html5-named-char-refs", ["exports"], function (exports) {
  "use strict";

  exports.default = {
    Aacute: "Á", aacute: "á", Abreve: "Ă", abreve: "ă", ac: "∾", acd: "∿", acE: "∾̳", Acirc: "Â", acirc: "â", acute: "´", Acy: "А", acy: "а", AElig: "Æ", aelig: "æ", af: "\u2061", Afr: "𝔄", afr: "𝔞", Agrave: "À", agrave: "à", alefsym: "ℵ", aleph: "ℵ", Alpha: "Α", alpha: "α", Amacr: "Ā", amacr: "ā", amalg: "⨿", AMP: "&", amp: "&", And: "⩓", and: "∧", andand: "⩕", andd: "⩜", andslope: "⩘", andv: "⩚", ang: "∠", ange: "⦤", angle: "∠", angmsd: "∡", angmsdaa: "⦨", angmsdab: "⦩", angmsdac: "⦪", angmsdad: "⦫", angmsdae: "⦬", angmsdaf: "⦭", angmsdag: "⦮", angmsdah: "⦯", angrt: "∟", angrtvb: "⊾", angrtvbd: "⦝", angsph: "∢", angst: "Å", angzarr: "⍼", Aogon: "Ą", aogon: "ą", Aopf: "𝔸", aopf: "𝕒", ap: "≈", apacir: "⩯", apE: "⩰", ape: "≊", apid: "≋", apos: "'", ApplyFunction: "\u2061", approx: "≈", approxeq: "≊", Aring: "Å", aring: "å", Ascr: "𝒜", ascr: "𝒶", Assign: "≔", ast: "*", asymp: "≈", asympeq: "≍", Atilde: "Ã", atilde: "ã", Auml: "Ä", auml: "ä", awconint: "∳", awint: "⨑", backcong: "≌", backepsilon: "϶", backprime: "‵", backsim: "∽", backsimeq: "⋍", Backslash: "∖", Barv: "⫧", barvee: "⊽", Barwed: "⌆", barwed: "⌅", barwedge: "⌅", bbrk: "⎵", bbrktbrk: "⎶", bcong: "≌", Bcy: "Б", bcy: "б", bdquo: "„", becaus: "∵", Because: "∵", because: "∵", bemptyv: "⦰", bepsi: "϶", bernou: "ℬ", Bernoullis: "ℬ", Beta: "Β", beta: "β", beth: "ℶ", between: "≬", Bfr: "𝔅", bfr: "𝔟", bigcap: "⋂", bigcirc: "◯", bigcup: "⋃", bigodot: "⨀", bigoplus: "⨁", bigotimes: "⨂", bigsqcup: "⨆", bigstar: "★", bigtriangledown: "▽", bigtriangleup: "△", biguplus: "⨄", bigvee: "⋁", bigwedge: "⋀", bkarow: "⤍", blacklozenge: "⧫", blacksquare: "▪", blacktriangle: "▴", blacktriangledown: "▾", blacktriangleleft: "◂", blacktriangleright: "▸", blank: "␣", blk12: "▒", blk14: "░", blk34: "▓", block: "█", bne: "=⃥", bnequiv: "≡⃥", bNot: "⫭", bnot: "⌐", Bopf: "𝔹", bopf: "𝕓", bot: "⊥", bottom: "⊥", bowtie: "⋈", boxbox: "⧉", boxDL: "╗", boxDl: "╖", boxdL: "╕", boxdl: "┐", boxDR: "╔", boxDr: "╓", boxdR: "╒", boxdr: "┌", boxH: "═", boxh: "─", boxHD: "╦", boxHd: "╤", boxhD: "╥", boxhd: "┬", boxHU: "╩", boxHu: "╧", boxhU: "╨", boxhu: "┴", boxminus: "⊟", boxplus: "⊞", boxtimes: "⊠", boxUL: "╝", boxUl: "╜", boxuL: "╛", boxul: "┘", boxUR: "╚", boxUr: "╙", boxuR: "╘", boxur: "└", boxV: "║", boxv: "│", boxVH: "╬", boxVh: "╫", boxvH: "╪", boxvh: "┼", boxVL: "╣", boxVl: "╢", boxvL: "╡", boxvl: "┤", boxVR: "╠", boxVr: "╟", boxvR: "╞", boxvr: "├", bprime: "‵", Breve: "˘", breve: "˘", brvbar: "¦", Bscr: "ℬ", bscr: "𝒷", bsemi: "⁏", bsim: "∽", bsime: "⋍", bsol: "\\", bsolb: "⧅", bsolhsub: "⟈", bull: "•", bullet: "•", bump: "≎", bumpE: "⪮", bumpe: "≏", Bumpeq: "≎", bumpeq: "≏", Cacute: "Ć", cacute: "ć", Cap: "⋒", cap: "∩", capand: "⩄", capbrcup: "⩉", capcap: "⩋", capcup: "⩇", capdot: "⩀", CapitalDifferentialD: "ⅅ", caps: "∩︀", caret: "⁁", caron: "ˇ", Cayleys: "ℭ", ccaps: "⩍", Ccaron: "Č", ccaron: "č", Ccedil: "Ç", ccedil: "ç", Ccirc: "Ĉ", ccirc: "ĉ", Cconint: "∰", ccups: "⩌", ccupssm: "⩐", Cdot: "Ċ", cdot: "ċ", cedil: "¸", Cedilla: "¸", cemptyv: "⦲", cent: "¢", CenterDot: "·", centerdot: "·", Cfr: "ℭ", cfr: "𝔠", CHcy: "Ч", chcy: "ч", check: "✓", checkmark: "✓", Chi: "Χ", chi: "χ", cir: "○", circ: "ˆ", circeq: "≗", circlearrowleft: "↺", circlearrowright: "↻", circledast: "⊛", circledcirc: "⊚", circleddash: "⊝", CircleDot: "⊙", circledR: "®", circledS: "Ⓢ", CircleMinus: "⊖", CirclePlus: "⊕", CircleTimes: "⊗", cirE: "⧃", cire: "≗", cirfnint: "⨐", cirmid: "⫯", cirscir: "⧂", ClockwiseContourIntegral: "∲", CloseCurlyDoubleQuote: "”", CloseCurlyQuote: "’", clubs: "♣", clubsuit: "♣", Colon: "∷", colon: ":", Colone: "⩴", colone: "≔", coloneq: "≔", comma: ",", commat: "@", comp: "∁", compfn: "∘", complement: "∁", complexes: "ℂ", cong: "≅", congdot: "⩭", Congruent: "≡", Conint: "∯", conint: "∮", ContourIntegral: "∮", Copf: "ℂ", copf: "𝕔", coprod: "∐", Coproduct: "∐", COPY: "©", copy: "©", copysr: "℗", CounterClockwiseContourIntegral: "∳", crarr: "↵", Cross: "⨯", cross: "✗", Cscr: "𝒞", cscr: "𝒸", csub: "⫏", csube: "⫑", csup: "⫐", csupe: "⫒", ctdot: "⋯", cudarrl: "⤸", cudarrr: "⤵", cuepr: "⋞", cuesc: "⋟", cularr: "↶", cularrp: "⤽", Cup: "⋓", cup: "∪", cupbrcap: "⩈", CupCap: "≍", cupcap: "⩆", cupcup: "⩊", cupdot: "⊍", cupor: "⩅", cups: "∪︀", curarr: "↷", curarrm: "⤼", curlyeqprec: "⋞", curlyeqsucc: "⋟", curlyvee: "⋎", curlywedge: "⋏", curren: "¤", curvearrowleft: "↶", curvearrowright: "↷", cuvee: "⋎", cuwed: "⋏", cwconint: "∲", cwint: "∱", cylcty: "⌭", Dagger: "‡", dagger: "†", daleth: "ℸ", Darr: "↡", dArr: "⇓", darr: "↓", dash: "‐", Dashv: "⫤", dashv: "⊣", dbkarow: "⤏", dblac: "˝", Dcaron: "Ď", dcaron: "ď", Dcy: "Д", dcy: "д", DD: "ⅅ", dd: "ⅆ", ddagger: "‡", ddarr: "⇊", DDotrahd: "⤑", ddotseq: "⩷", deg: "°", Del: "∇", Delta: "Δ", delta: "δ", demptyv: "⦱", dfisht: "⥿", Dfr: "𝔇", dfr: "𝔡", dHar: "⥥", dharl: "⇃", dharr: "⇂", DiacriticalAcute: "´", DiacriticalDot: "˙", DiacriticalDoubleAcute: "˝", DiacriticalGrave: "`", DiacriticalTilde: "˜", diam: "⋄", Diamond: "⋄", diamond: "⋄", diamondsuit: "♦", diams: "♦", die: "¨", DifferentialD: "ⅆ", digamma: "ϝ", disin: "⋲", div: "÷", divide: "÷", divideontimes: "⋇", divonx: "⋇", DJcy: "Ђ", djcy: "ђ", dlcorn: "⌞", dlcrop: "⌍", dollar: "$", Dopf: "𝔻", dopf: "𝕕", Dot: "¨", dot: "˙", DotDot: "⃜", doteq: "≐", doteqdot: "≑", DotEqual: "≐", dotminus: "∸", dotplus: "∔", dotsquare: "⊡", doublebarwedge: "⌆", DoubleContourIntegral: "∯", DoubleDot: "¨", DoubleDownArrow: "⇓", DoubleLeftArrow: "⇐", DoubleLeftRightArrow: "⇔", DoubleLeftTee: "⫤", DoubleLongLeftArrow: "⟸", DoubleLongLeftRightArrow: "⟺", DoubleLongRightArrow: "⟹", DoubleRightArrow: "⇒", DoubleRightTee: "⊨", DoubleUpArrow: "⇑", DoubleUpDownArrow: "⇕", DoubleVerticalBar: "∥", DownArrow: "↓", Downarrow: "⇓", downarrow: "↓", DownArrowBar: "⤓", DownArrowUpArrow: "⇵", DownBreve: "̑", downdownarrows: "⇊", downharpoonleft: "⇃", downharpoonright: "⇂", DownLeftRightVector: "⥐", DownLeftTeeVector: "⥞", DownLeftVector: "↽", DownLeftVectorBar: "⥖", DownRightTeeVector: "⥟", DownRightVector: "⇁", DownRightVectorBar: "⥗", DownTee: "⊤", DownTeeArrow: "↧", drbkarow: "⤐", drcorn: "⌟", drcrop: "⌌", Dscr: "𝒟", dscr: "𝒹", DScy: "Ѕ", dscy: "ѕ", dsol: "⧶", Dstrok: "Đ", dstrok: "đ", dtdot: "⋱", dtri: "▿", dtrif: "▾", duarr: "⇵", duhar: "⥯", dwangle: "⦦", DZcy: "Џ", dzcy: "џ", dzigrarr: "⟿", Eacute: "É", eacute: "é", easter: "⩮", Ecaron: "Ě", ecaron: "ě", ecir: "≖", Ecirc: "Ê", ecirc: "ê", ecolon: "≕", Ecy: "Э", ecy: "э", eDDot: "⩷", Edot: "Ė", eDot: "≑", edot: "ė", ee: "ⅇ", efDot: "≒", Efr: "𝔈", efr: "𝔢", eg: "⪚", Egrave: "È", egrave: "è", egs: "⪖", egsdot: "⪘", el: "⪙", Element: "∈", elinters: "⏧", ell: "ℓ", els: "⪕", elsdot: "⪗", Emacr: "Ē", emacr: "ē", empty: "∅", emptyset: "∅", EmptySmallSquare: "◻", emptyv: "∅", EmptyVerySmallSquare: "▫", emsp: " ", emsp13: " ", emsp14: " ", ENG: "Ŋ", eng: "ŋ", ensp: " ", Eogon: "Ę", eogon: "ę", Eopf: "𝔼", eopf: "𝕖", epar: "⋕", eparsl: "⧣", eplus: "⩱", epsi: "ε", Epsilon: "Ε", epsilon: "ε", epsiv: "ϵ", eqcirc: "≖", eqcolon: "≕", eqsim: "≂", eqslantgtr: "⪖", eqslantless: "⪕", Equal: "⩵", equals: "=", EqualTilde: "≂", equest: "≟", Equilibrium: "⇌", equiv: "≡", equivDD: "⩸", eqvparsl: "⧥", erarr: "⥱", erDot: "≓", Escr: "ℰ", escr: "ℯ", esdot: "≐", Esim: "⩳", esim: "≂", Eta: "Η", eta: "η", ETH: "Ð", eth: "ð", Euml: "Ë", euml: "ë", euro: "€", excl: "!", exist: "∃", Exists: "∃", expectation: "ℰ", ExponentialE: "ⅇ", exponentiale: "ⅇ", fallingdotseq: "≒", Fcy: "Ф", fcy: "ф", female: "♀", ffilig: "ﬃ", fflig: "ﬀ", ffllig: "ﬄ", Ffr: "𝔉", ffr: "𝔣", filig: "ﬁ", FilledSmallSquare: "◼", FilledVerySmallSquare: "▪", fjlig: "fj", flat: "♭", fllig: "ﬂ", fltns: "▱", fnof: "ƒ", Fopf: "𝔽", fopf: "𝕗", ForAll: "∀", forall: "∀", fork: "⋔", forkv: "⫙", Fouriertrf: "ℱ", fpartint: "⨍", frac12: "½", frac13: "⅓", frac14: "¼", frac15: "⅕", frac16: "⅙", frac18: "⅛", frac23: "⅔", frac25: "⅖", frac34: "¾", frac35: "⅗", frac38: "⅜", frac45: "⅘", frac56: "⅚", frac58: "⅝", frac78: "⅞", frasl: "⁄", frown: "⌢", Fscr: "ℱ", fscr: "𝒻", gacute: "ǵ", Gamma: "Γ", gamma: "γ", Gammad: "Ϝ", gammad: "ϝ", gap: "⪆", Gbreve: "Ğ", gbreve: "ğ", Gcedil: "Ģ", Gcirc: "Ĝ", gcirc: "ĝ", Gcy: "Г", gcy: "г", Gdot: "Ġ", gdot: "ġ", gE: "≧", ge: "≥", gEl: "⪌", gel: "⋛", geq: "≥", geqq: "≧", geqslant: "⩾", ges: "⩾", gescc: "⪩", gesdot: "⪀", gesdoto: "⪂", gesdotol: "⪄", gesl: "⋛︀", gesles: "⪔", Gfr: "𝔊", gfr: "𝔤", Gg: "⋙", gg: "≫", ggg: "⋙", gimel: "ℷ", GJcy: "Ѓ", gjcy: "ѓ", gl: "≷", gla: "⪥", glE: "⪒", glj: "⪤", gnap: "⪊", gnapprox: "⪊", gnE: "≩", gne: "⪈", gneq: "⪈", gneqq: "≩", gnsim: "⋧", Gopf: "𝔾", gopf: "𝕘", grave: "`", GreaterEqual: "≥", GreaterEqualLess: "⋛", GreaterFullEqual: "≧", GreaterGreater: "⪢", GreaterLess: "≷", GreaterSlantEqual: "⩾", GreaterTilde: "≳", Gscr: "𝒢", gscr: "ℊ", gsim: "≳", gsime: "⪎", gsiml: "⪐", GT: ">", Gt: "≫", gt: ">", gtcc: "⪧", gtcir: "⩺", gtdot: "⋗", gtlPar: "⦕", gtquest: "⩼", gtrapprox: "⪆", gtrarr: "⥸", gtrdot: "⋗", gtreqless: "⋛", gtreqqless: "⪌", gtrless: "≷", gtrsim: "≳", gvertneqq: "≩︀", gvnE: "≩︀", Hacek: "ˇ", hairsp: " ", half: "½", hamilt: "ℋ", HARDcy: "Ъ", hardcy: "ъ", hArr: "⇔", harr: "↔", harrcir: "⥈", harrw: "↭", Hat: "^", hbar: "ℏ", Hcirc: "Ĥ", hcirc: "ĥ", hearts: "♥", heartsuit: "♥", hellip: "…", hercon: "⊹", Hfr: "ℌ", hfr: "𝔥", HilbertSpace: "ℋ", hksearow: "⤥", hkswarow: "⤦", hoarr: "⇿", homtht: "∻", hookleftarrow: "↩", hookrightarrow: "↪", Hopf: "ℍ", hopf: "𝕙", horbar: "―", HorizontalLine: "─", Hscr: "ℋ", hscr: "𝒽", hslash: "ℏ", Hstrok: "Ħ", hstrok: "ħ", HumpDownHump: "≎", HumpEqual: "≏", hybull: "⁃", hyphen: "‐", Iacute: "Í", iacute: "í", ic: "\u2063", Icirc: "Î", icirc: "î", Icy: "И", icy: "и", Idot: "İ", IEcy: "Е", iecy: "е", iexcl: "¡", iff: "⇔", Ifr: "ℑ", ifr: "𝔦", Igrave: "Ì", igrave: "ì", ii: "ⅈ", iiiint: "⨌", iiint: "∭", iinfin: "⧜", iiota: "℩", IJlig: "Ĳ", ijlig: "ĳ", Im: "ℑ", Imacr: "Ī", imacr: "ī", image: "ℑ", ImaginaryI: "ⅈ", imagline: "ℐ", imagpart: "ℑ", imath: "ı", imof: "⊷", imped: "Ƶ", Implies: "⇒", in: "∈", incare: "℅", infin: "∞", infintie: "⧝", inodot: "ı", Int: "∬", int: "∫", intcal: "⊺", integers: "ℤ", Integral: "∫", intercal: "⊺", Intersection: "⋂", intlarhk: "⨗", intprod: "⨼", InvisibleComma: "\u2063", InvisibleTimes: "\u2062", IOcy: "Ё", iocy: "ё", Iogon: "Į", iogon: "į", Iopf: "𝕀", iopf: "𝕚", Iota: "Ι", iota: "ι", iprod: "⨼", iquest: "¿", Iscr: "ℐ", iscr: "𝒾", isin: "∈", isindot: "⋵", isinE: "⋹", isins: "⋴", isinsv: "⋳", isinv: "∈", it: "\u2062", Itilde: "Ĩ", itilde: "ĩ", Iukcy: "І", iukcy: "і", Iuml: "Ï", iuml: "ï", Jcirc: "Ĵ", jcirc: "ĵ", Jcy: "Й", jcy: "й", Jfr: "𝔍", jfr: "𝔧", jmath: "ȷ", Jopf: "𝕁", jopf: "𝕛", Jscr: "𝒥", jscr: "𝒿", Jsercy: "Ј", jsercy: "ј", Jukcy: "Є", jukcy: "є", Kappa: "Κ", kappa: "κ", kappav: "ϰ", Kcedil: "Ķ", kcedil: "ķ", Kcy: "К", kcy: "к", Kfr: "𝔎", kfr: "𝔨", kgreen: "ĸ", KHcy: "Х", khcy: "х", KJcy: "Ќ", kjcy: "ќ", Kopf: "𝕂", kopf: "𝕜", Kscr: "𝒦", kscr: "𝓀", lAarr: "⇚", Lacute: "Ĺ", lacute: "ĺ", laemptyv: "⦴", lagran: "ℒ", Lambda: "Λ", lambda: "λ", Lang: "⟪", lang: "⟨", langd: "⦑", langle: "⟨", lap: "⪅", Laplacetrf: "ℒ", laquo: "«", Larr: "↞", lArr: "⇐", larr: "←", larrb: "⇤", larrbfs: "⤟", larrfs: "⤝", larrhk: "↩", larrlp: "↫", larrpl: "⤹", larrsim: "⥳", larrtl: "↢", lat: "⪫", lAtail: "⤛", latail: "⤙", late: "⪭", lates: "⪭︀", lBarr: "⤎", lbarr: "⤌", lbbrk: "❲", lbrace: "{", lbrack: "[", lbrke: "⦋", lbrksld: "⦏", lbrkslu: "⦍", Lcaron: "Ľ", lcaron: "ľ", Lcedil: "Ļ", lcedil: "ļ", lceil: "⌈", lcub: "{", Lcy: "Л", lcy: "л", ldca: "⤶", ldquo: "“", ldquor: "„", ldrdhar: "⥧", ldrushar: "⥋", ldsh: "↲", lE: "≦", le: "≤", LeftAngleBracket: "⟨", LeftArrow: "←", Leftarrow: "⇐", leftarrow: "←", LeftArrowBar: "⇤", LeftArrowRightArrow: "⇆", leftarrowtail: "↢", LeftCeiling: "⌈", LeftDoubleBracket: "⟦", LeftDownTeeVector: "⥡", LeftDownVector: "⇃", LeftDownVectorBar: "⥙", LeftFloor: "⌊", leftharpoondown: "↽", leftharpoonup: "↼", leftleftarrows: "⇇", LeftRightArrow: "↔", Leftrightarrow: "⇔", leftrightarrow: "↔", leftrightarrows: "⇆", leftrightharpoons: "⇋", leftrightsquigarrow: "↭", LeftRightVector: "⥎", LeftTee: "⊣", LeftTeeArrow: "↤", LeftTeeVector: "⥚", leftthreetimes: "⋋", LeftTriangle: "⊲", LeftTriangleBar: "⧏", LeftTriangleEqual: "⊴", LeftUpDownVector: "⥑", LeftUpTeeVector: "⥠", LeftUpVector: "↿", LeftUpVectorBar: "⥘", LeftVector: "↼", LeftVectorBar: "⥒", lEg: "⪋", leg: "⋚", leq: "≤", leqq: "≦", leqslant: "⩽", les: "⩽", lescc: "⪨", lesdot: "⩿", lesdoto: "⪁", lesdotor: "⪃", lesg: "⋚︀", lesges: "⪓", lessapprox: "⪅", lessdot: "⋖", lesseqgtr: "⋚", lesseqqgtr: "⪋", LessEqualGreater: "⋚", LessFullEqual: "≦", LessGreater: "≶", lessgtr: "≶", LessLess: "⪡", lesssim: "≲", LessSlantEqual: "⩽", LessTilde: "≲", lfisht: "⥼", lfloor: "⌊", Lfr: "𝔏", lfr: "𝔩", lg: "≶", lgE: "⪑", lHar: "⥢", lhard: "↽", lharu: "↼", lharul: "⥪", lhblk: "▄", LJcy: "Љ", ljcy: "љ", Ll: "⋘", ll: "≪", llarr: "⇇", llcorner: "⌞", Lleftarrow: "⇚", llhard: "⥫", lltri: "◺", Lmidot: "Ŀ", lmidot: "ŀ", lmoust: "⎰", lmoustache: "⎰", lnap: "⪉", lnapprox: "⪉", lnE: "≨", lne: "⪇", lneq: "⪇", lneqq: "≨", lnsim: "⋦", loang: "⟬", loarr: "⇽", lobrk: "⟦", LongLeftArrow: "⟵", Longleftarrow: "⟸", longleftarrow: "⟵", LongLeftRightArrow: "⟷", Longleftrightarrow: "⟺", longleftrightarrow: "⟷", longmapsto: "⟼", LongRightArrow: "⟶", Longrightarrow: "⟹", longrightarrow: "⟶", looparrowleft: "↫", looparrowright: "↬", lopar: "⦅", Lopf: "𝕃", lopf: "𝕝", loplus: "⨭", lotimes: "⨴", lowast: "∗", lowbar: "_", LowerLeftArrow: "↙", LowerRightArrow: "↘", loz: "◊", lozenge: "◊", lozf: "⧫", lpar: "(", lparlt: "⦓", lrarr: "⇆", lrcorner: "⌟", lrhar: "⇋", lrhard: "⥭", lrm: "\u200e", lrtri: "⊿", lsaquo: "‹", Lscr: "ℒ", lscr: "𝓁", Lsh: "↰", lsh: "↰", lsim: "≲", lsime: "⪍", lsimg: "⪏", lsqb: "[", lsquo: "‘", lsquor: "‚", Lstrok: "Ł", lstrok: "ł", LT: "<", Lt: "≪", lt: "<", ltcc: "⪦", ltcir: "⩹", ltdot: "⋖", lthree: "⋋", ltimes: "⋉", ltlarr: "⥶", ltquest: "⩻", ltri: "◃", ltrie: "⊴", ltrif: "◂", ltrPar: "⦖", lurdshar: "⥊", luruhar: "⥦", lvertneqq: "≨︀", lvnE: "≨︀", macr: "¯", male: "♂", malt: "✠", maltese: "✠", Map: "⤅", map: "↦", mapsto: "↦", mapstodown: "↧", mapstoleft: "↤", mapstoup: "↥", marker: "▮", mcomma: "⨩", Mcy: "М", mcy: "м", mdash: "—", mDDot: "∺", measuredangle: "∡", MediumSpace: " ", Mellintrf: "ℳ", Mfr: "𝔐", mfr: "𝔪", mho: "℧", micro: "µ", mid: "∣", midast: "*", midcir: "⫰", middot: "·", minus: "−", minusb: "⊟", minusd: "∸", minusdu: "⨪", MinusPlus: "∓", mlcp: "⫛", mldr: "…", mnplus: "∓", models: "⊧", Mopf: "𝕄", mopf: "𝕞", mp: "∓", Mscr: "ℳ", mscr: "𝓂", mstpos: "∾", Mu: "Μ", mu: "μ", multimap: "⊸", mumap: "⊸", nabla: "∇", Nacute: "Ń", nacute: "ń", nang: "∠⃒", nap: "≉", napE: "⩰̸", napid: "≋̸", napos: "ŉ", napprox: "≉", natur: "♮", natural: "♮", naturals: "ℕ", nbsp: " ", nbump: "≎̸", nbumpe: "≏̸", ncap: "⩃", Ncaron: "Ň", ncaron: "ň", Ncedil: "Ņ", ncedil: "ņ", ncong: "≇", ncongdot: "⩭̸", ncup: "⩂", Ncy: "Н", ncy: "н", ndash: "–", ne: "≠", nearhk: "⤤", neArr: "⇗", nearr: "↗", nearrow: "↗", nedot: "≐̸", NegativeMediumSpace: "​", NegativeThickSpace: "​", NegativeThinSpace: "​", NegativeVeryThinSpace: "​", nequiv: "≢", nesear: "⤨", nesim: "≂̸", NestedGreaterGreater: "≫", NestedLessLess: "≪", NewLine: "\u000a", nexist: "∄", nexists: "∄", Nfr: "𝔑", nfr: "𝔫", ngE: "≧̸", nge: "≱", ngeq: "≱", ngeqq: "≧̸", ngeqslant: "⩾̸", nges: "⩾̸", nGg: "⋙̸", ngsim: "≵", nGt: "≫⃒", ngt: "≯", ngtr: "≯", nGtv: "≫̸", nhArr: "⇎", nharr: "↮", nhpar: "⫲", ni: "∋", nis: "⋼", nisd: "⋺", niv: "∋", NJcy: "Њ", njcy: "њ", nlArr: "⇍", nlarr: "↚", nldr: "‥", nlE: "≦̸", nle: "≰", nLeftarrow: "⇍", nleftarrow: "↚", nLeftrightarrow: "⇎", nleftrightarrow: "↮", nleq: "≰", nleqq: "≦̸", nleqslant: "⩽̸", nles: "⩽̸", nless: "≮", nLl: "⋘̸", nlsim: "≴", nLt: "≪⃒", nlt: "≮", nltri: "⋪", nltrie: "⋬", nLtv: "≪̸", nmid: "∤", NoBreak: "\u2060", NonBreakingSpace: " ", Nopf: "ℕ", nopf: "𝕟", Not: "⫬", not: "¬", NotCongruent: "≢", NotCupCap: "≭", NotDoubleVerticalBar: "∦", NotElement: "∉", NotEqual: "≠", NotEqualTilde: "≂̸", NotExists: "∄", NotGreater: "≯", NotGreaterEqual: "≱", NotGreaterFullEqual: "≧̸", NotGreaterGreater: "≫̸", NotGreaterLess: "≹", NotGreaterSlantEqual: "⩾̸", NotGreaterTilde: "≵", NotHumpDownHump: "≎̸", NotHumpEqual: "≏̸", notin: "∉", notindot: "⋵̸", notinE: "⋹̸", notinva: "∉", notinvb: "⋷", notinvc: "⋶", NotLeftTriangle: "⋪", NotLeftTriangleBar: "⧏̸", NotLeftTriangleEqual: "⋬", NotLess: "≮", NotLessEqual: "≰", NotLessGreater: "≸", NotLessLess: "≪̸", NotLessSlantEqual: "⩽̸", NotLessTilde: "≴", NotNestedGreaterGreater: "⪢̸", NotNestedLessLess: "⪡̸", notni: "∌", notniva: "∌", notnivb: "⋾", notnivc: "⋽", NotPrecedes: "⊀", NotPrecedesEqual: "⪯̸", NotPrecedesSlantEqual: "⋠", NotReverseElement: "∌", NotRightTriangle: "⋫", NotRightTriangleBar: "⧐̸", NotRightTriangleEqual: "⋭", NotSquareSubset: "⊏̸", NotSquareSubsetEqual: "⋢", NotSquareSuperset: "⊐̸", NotSquareSupersetEqual: "⋣", NotSubset: "⊂⃒", NotSubsetEqual: "⊈", NotSucceeds: "⊁", NotSucceedsEqual: "⪰̸", NotSucceedsSlantEqual: "⋡", NotSucceedsTilde: "≿̸", NotSuperset: "⊃⃒", NotSupersetEqual: "⊉", NotTilde: "≁", NotTildeEqual: "≄", NotTildeFullEqual: "≇", NotTildeTilde: "≉", NotVerticalBar: "∤", npar: "∦", nparallel: "∦", nparsl: "⫽⃥", npart: "∂̸", npolint: "⨔", npr: "⊀", nprcue: "⋠", npre: "⪯̸", nprec: "⊀", npreceq: "⪯̸", nrArr: "⇏", nrarr: "↛", nrarrc: "⤳̸", nrarrw: "↝̸", nRightarrow: "⇏", nrightarrow: "↛", nrtri: "⋫", nrtrie: "⋭", nsc: "⊁", nsccue: "⋡", nsce: "⪰̸", Nscr: "𝒩", nscr: "𝓃", nshortmid: "∤", nshortparallel: "∦", nsim: "≁", nsime: "≄", nsimeq: "≄", nsmid: "∤", nspar: "∦", nsqsube: "⋢", nsqsupe: "⋣", nsub: "⊄", nsubE: "⫅̸", nsube: "⊈", nsubset: "⊂⃒", nsubseteq: "⊈", nsubseteqq: "⫅̸", nsucc: "⊁", nsucceq: "⪰̸", nsup: "⊅", nsupE: "⫆̸", nsupe: "⊉", nsupset: "⊃⃒", nsupseteq: "⊉", nsupseteqq: "⫆̸", ntgl: "≹", Ntilde: "Ñ", ntilde: "ñ", ntlg: "≸", ntriangleleft: "⋪", ntrianglelefteq: "⋬", ntriangleright: "⋫", ntrianglerighteq: "⋭", Nu: "Ν", nu: "ν", num: "#", numero: "№", numsp: " ", nvap: "≍⃒", nVDash: "⊯", nVdash: "⊮", nvDash: "⊭", nvdash: "⊬", nvge: "≥⃒", nvgt: ">⃒", nvHarr: "⤄", nvinfin: "⧞", nvlArr: "⤂", nvle: "≤⃒", nvlt: "<⃒", nvltrie: "⊴⃒", nvrArr: "⤃", nvrtrie: "⊵⃒", nvsim: "∼⃒", nwarhk: "⤣", nwArr: "⇖", nwarr: "↖", nwarrow: "↖", nwnear: "⤧", Oacute: "Ó", oacute: "ó", oast: "⊛", ocir: "⊚", Ocirc: "Ô", ocirc: "ô", Ocy: "О", ocy: "о", odash: "⊝", Odblac: "Ő", odblac: "ő", odiv: "⨸", odot: "⊙", odsold: "⦼", OElig: "Œ", oelig: "œ", ofcir: "⦿", Ofr: "𝔒", ofr: "𝔬", ogon: "˛", Ograve: "Ò", ograve: "ò", ogt: "⧁", ohbar: "⦵", ohm: "Ω", oint: "∮", olarr: "↺", olcir: "⦾", olcross: "⦻", oline: "‾", olt: "⧀", Omacr: "Ō", omacr: "ō", Omega: "Ω", omega: "ω", Omicron: "Ο", omicron: "ο", omid: "⦶", ominus: "⊖", Oopf: "𝕆", oopf: "𝕠", opar: "⦷", OpenCurlyDoubleQuote: "“", OpenCurlyQuote: "‘", operp: "⦹", oplus: "⊕", Or: "⩔", or: "∨", orarr: "↻", ord: "⩝", order: "ℴ", orderof: "ℴ", ordf: "ª", ordm: "º", origof: "⊶", oror: "⩖", orslope: "⩗", orv: "⩛", oS: "Ⓢ", Oscr: "𝒪", oscr: "ℴ", Oslash: "Ø", oslash: "ø", osol: "⊘", Otilde: "Õ", otilde: "õ", Otimes: "⨷", otimes: "⊗", otimesas: "⨶", Ouml: "Ö", ouml: "ö", ovbar: "⌽", OverBar: "‾", OverBrace: "⏞", OverBracket: "⎴", OverParenthesis: "⏜", par: "∥", para: "¶", parallel: "∥", parsim: "⫳", parsl: "⫽", part: "∂", PartialD: "∂", Pcy: "П", pcy: "п", percnt: "%", period: ".", permil: "‰", perp: "⊥", pertenk: "‱", Pfr: "𝔓", pfr: "𝔭", Phi: "Φ", phi: "φ", phiv: "ϕ", phmmat: "ℳ", phone: "☎", Pi: "Π", pi: "π", pitchfork: "⋔", piv: "ϖ", planck: "ℏ", planckh: "ℎ", plankv: "ℏ", plus: "+", plusacir: "⨣", plusb: "⊞", pluscir: "⨢", plusdo: "∔", plusdu: "⨥", pluse: "⩲", PlusMinus: "±", plusmn: "±", plussim: "⨦", plustwo: "⨧", pm: "±", Poincareplane: "ℌ", pointint: "⨕", Popf: "ℙ", popf: "𝕡", pound: "£", Pr: "⪻", pr: "≺", prap: "⪷", prcue: "≼", prE: "⪳", pre: "⪯", prec: "≺", precapprox: "⪷", preccurlyeq: "≼", Precedes: "≺", PrecedesEqual: "⪯", PrecedesSlantEqual: "≼", PrecedesTilde: "≾", preceq: "⪯", precnapprox: "⪹", precneqq: "⪵", precnsim: "⋨", precsim: "≾", Prime: "″", prime: "′", primes: "ℙ", prnap: "⪹", prnE: "⪵", prnsim: "⋨", prod: "∏", Product: "∏", profalar: "⌮", profline: "⌒", profsurf: "⌓", prop: "∝", Proportion: "∷", Proportional: "∝", propto: "∝", prsim: "≾", prurel: "⊰", Pscr: "𝒫", pscr: "𝓅", Psi: "Ψ", psi: "ψ", puncsp: " ", Qfr: "𝔔", qfr: "𝔮", qint: "⨌", Qopf: "ℚ", qopf: "𝕢", qprime: "⁗", Qscr: "𝒬", qscr: "𝓆", quaternions: "ℍ", quatint: "⨖", quest: "?", questeq: "≟", QUOT: "\"", quot: "\"", rAarr: "⇛", race: "∽̱", Racute: "Ŕ", racute: "ŕ", radic: "√", raemptyv: "⦳", Rang: "⟫", rang: "⟩", rangd: "⦒", range: "⦥", rangle: "⟩", raquo: "»", Rarr: "↠", rArr: "⇒", rarr: "→", rarrap: "⥵", rarrb: "⇥", rarrbfs: "⤠", rarrc: "⤳", rarrfs: "⤞", rarrhk: "↪", rarrlp: "↬", rarrpl: "⥅", rarrsim: "⥴", Rarrtl: "⤖", rarrtl: "↣", rarrw: "↝", rAtail: "⤜", ratail: "⤚", ratio: "∶", rationals: "ℚ", RBarr: "⤐", rBarr: "⤏", rbarr: "⤍", rbbrk: "❳", rbrace: "}", rbrack: "]", rbrke: "⦌", rbrksld: "⦎", rbrkslu: "⦐", Rcaron: "Ř", rcaron: "ř", Rcedil: "Ŗ", rcedil: "ŗ", rceil: "⌉", rcub: "}", Rcy: "Р", rcy: "р", rdca: "⤷", rdldhar: "⥩", rdquo: "”", rdquor: "”", rdsh: "↳", Re: "ℜ", real: "ℜ", realine: "ℛ", realpart: "ℜ", reals: "ℝ", rect: "▭", REG: "®", reg: "®", ReverseElement: "∋", ReverseEquilibrium: "⇋", ReverseUpEquilibrium: "⥯", rfisht: "⥽", rfloor: "⌋", Rfr: "ℜ", rfr: "𝔯", rHar: "⥤", rhard: "⇁", rharu: "⇀", rharul: "⥬", Rho: "Ρ", rho: "ρ", rhov: "ϱ", RightAngleBracket: "⟩", RightArrow: "→", Rightarrow: "⇒", rightarrow: "→", RightArrowBar: "⇥", RightArrowLeftArrow: "⇄", rightarrowtail: "↣", RightCeiling: "⌉", RightDoubleBracket: "⟧", RightDownTeeVector: "⥝", RightDownVector: "⇂", RightDownVectorBar: "⥕", RightFloor: "⌋", rightharpoondown: "⇁", rightharpoonup: "⇀", rightleftarrows: "⇄", rightleftharpoons: "⇌", rightrightarrows: "⇉", rightsquigarrow: "↝", RightTee: "⊢", RightTeeArrow: "↦", RightTeeVector: "⥛", rightthreetimes: "⋌", RightTriangle: "⊳", RightTriangleBar: "⧐", RightTriangleEqual: "⊵", RightUpDownVector: "⥏", RightUpTeeVector: "⥜", RightUpVector: "↾", RightUpVectorBar: "⥔", RightVector: "⇀", RightVectorBar: "⥓", ring: "˚", risingdotseq: "≓", rlarr: "⇄", rlhar: "⇌", rlm: "\u200f", rmoust: "⎱", rmoustache: "⎱", rnmid: "⫮", roang: "⟭", roarr: "⇾", robrk: "⟧", ropar: "⦆", Ropf: "ℝ", ropf: "𝕣", roplus: "⨮", rotimes: "⨵", RoundImplies: "⥰", rpar: ")", rpargt: "⦔", rppolint: "⨒", rrarr: "⇉", Rrightarrow: "⇛", rsaquo: "›", Rscr: "ℛ", rscr: "𝓇", Rsh: "↱", rsh: "↱", rsqb: "]", rsquo: "’", rsquor: "’", rthree: "⋌", rtimes: "⋊", rtri: "▹", rtrie: "⊵", rtrif: "▸", rtriltri: "⧎", RuleDelayed: "⧴", ruluhar: "⥨", rx: "℞", Sacute: "Ś", sacute: "ś", sbquo: "‚", Sc: "⪼", sc: "≻", scap: "⪸", Scaron: "Š", scaron: "š", sccue: "≽", scE: "⪴", sce: "⪰", Scedil: "Ş", scedil: "ş", Scirc: "Ŝ", scirc: "ŝ", scnap: "⪺", scnE: "⪶", scnsim: "⋩", scpolint: "⨓", scsim: "≿", Scy: "С", scy: "с", sdot: "⋅", sdotb: "⊡", sdote: "⩦", searhk: "⤥", seArr: "⇘", searr: "↘", searrow: "↘", sect: "§", semi: ";", seswar: "⤩", setminus: "∖", setmn: "∖", sext: "✶", Sfr: "𝔖", sfr: "𝔰", sfrown: "⌢", sharp: "♯", SHCHcy: "Щ", shchcy: "щ", SHcy: "Ш", shcy: "ш", ShortDownArrow: "↓", ShortLeftArrow: "←", shortmid: "∣", shortparallel: "∥", ShortRightArrow: "→", ShortUpArrow: "↑", shy: "\u00ad", Sigma: "Σ", sigma: "σ", sigmaf: "ς", sigmav: "ς", sim: "∼", simdot: "⩪", sime: "≃", simeq: "≃", simg: "⪞", simgE: "⪠", siml: "⪝", simlE: "⪟", simne: "≆", simplus: "⨤", simrarr: "⥲", slarr: "←", SmallCircle: "∘", smallsetminus: "∖", smashp: "⨳", smeparsl: "⧤", smid: "∣", smile: "⌣", smt: "⪪", smte: "⪬", smtes: "⪬︀", SOFTcy: "Ь", softcy: "ь", sol: "/", solb: "⧄", solbar: "⌿", Sopf: "𝕊", sopf: "𝕤", spades: "♠", spadesuit: "♠", spar: "∥", sqcap: "⊓", sqcaps: "⊓︀", sqcup: "⊔", sqcups: "⊔︀", Sqrt: "√", sqsub: "⊏", sqsube: "⊑", sqsubset: "⊏", sqsubseteq: "⊑", sqsup: "⊐", sqsupe: "⊒", sqsupset: "⊐", sqsupseteq: "⊒", squ: "□", Square: "□", square: "□", SquareIntersection: "⊓", SquareSubset: "⊏", SquareSubsetEqual: "⊑", SquareSuperset: "⊐", SquareSupersetEqual: "⊒", SquareUnion: "⊔", squarf: "▪", squf: "▪", srarr: "→", Sscr: "𝒮", sscr: "𝓈", ssetmn: "∖", ssmile: "⌣", sstarf: "⋆", Star: "⋆", star: "☆", starf: "★", straightepsilon: "ϵ", straightphi: "ϕ", strns: "¯", Sub: "⋐", sub: "⊂", subdot: "⪽", subE: "⫅", sube: "⊆", subedot: "⫃", submult: "⫁", subnE: "⫋", subne: "⊊", subplus: "⪿", subrarr: "⥹", Subset: "⋐", subset: "⊂", subseteq: "⊆", subseteqq: "⫅", SubsetEqual: "⊆", subsetneq: "⊊", subsetneqq: "⫋", subsim: "⫇", subsub: "⫕", subsup: "⫓", succ: "≻", succapprox: "⪸", succcurlyeq: "≽", Succeeds: "≻", SucceedsEqual: "⪰", SucceedsSlantEqual: "≽", SucceedsTilde: "≿", succeq: "⪰", succnapprox: "⪺", succneqq: "⪶", succnsim: "⋩", succsim: "≿", SuchThat: "∋", Sum: "∑", sum: "∑", sung: "♪", Sup: "⋑", sup: "⊃", sup1: "¹", sup2: "²", sup3: "³", supdot: "⪾", supdsub: "⫘", supE: "⫆", supe: "⊇", supedot: "⫄", Superset: "⊃", SupersetEqual: "⊇", suphsol: "⟉", suphsub: "⫗", suplarr: "⥻", supmult: "⫂", supnE: "⫌", supne: "⊋", supplus: "⫀", Supset: "⋑", supset: "⊃", supseteq: "⊇", supseteqq: "⫆", supsetneq: "⊋", supsetneqq: "⫌", supsim: "⫈", supsub: "⫔", supsup: "⫖", swarhk: "⤦", swArr: "⇙", swarr: "↙", swarrow: "↙", swnwar: "⤪", szlig: "ß", Tab: "\u0009", target: "⌖", Tau: "Τ", tau: "τ", tbrk: "⎴", Tcaron: "Ť", tcaron: "ť", Tcedil: "Ţ", tcedil: "ţ", Tcy: "Т", tcy: "т", tdot: "⃛", telrec: "⌕", Tfr: "𝔗", tfr: "𝔱", there4: "∴", Therefore: "∴", therefore: "∴", Theta: "Θ", theta: "θ", thetasym: "ϑ", thetav: "ϑ", thickapprox: "≈", thicksim: "∼", ThickSpace: "  ", thinsp: " ", ThinSpace: " ", thkap: "≈", thksim: "∼", THORN: "Þ", thorn: "þ", Tilde: "∼", tilde: "˜", TildeEqual: "≃", TildeFullEqual: "≅", TildeTilde: "≈", times: "×", timesb: "⊠", timesbar: "⨱", timesd: "⨰", tint: "∭", toea: "⤨", top: "⊤", topbot: "⌶", topcir: "⫱", Topf: "𝕋", topf: "𝕥", topfork: "⫚", tosa: "⤩", tprime: "‴", TRADE: "™", trade: "™", triangle: "▵", triangledown: "▿", triangleleft: "◃", trianglelefteq: "⊴", triangleq: "≜", triangleright: "▹", trianglerighteq: "⊵", tridot: "◬", trie: "≜", triminus: "⨺", TripleDot: "⃛", triplus: "⨹", trisb: "⧍", tritime: "⨻", trpezium: "⏢", Tscr: "𝒯", tscr: "𝓉", TScy: "Ц", tscy: "ц", TSHcy: "Ћ", tshcy: "ћ", Tstrok: "Ŧ", tstrok: "ŧ", twixt: "≬", twoheadleftarrow: "↞", twoheadrightarrow: "↠", Uacute: "Ú", uacute: "ú", Uarr: "↟", uArr: "⇑", uarr: "↑", Uarrocir: "⥉", Ubrcy: "Ў", ubrcy: "ў", Ubreve: "Ŭ", ubreve: "ŭ", Ucirc: "Û", ucirc: "û", Ucy: "У", ucy: "у", udarr: "⇅", Udblac: "Ű", udblac: "ű", udhar: "⥮", ufisht: "⥾", Ufr: "𝔘", ufr: "𝔲", Ugrave: "Ù", ugrave: "ù", uHar: "⥣", uharl: "↿", uharr: "↾", uhblk: "▀", ulcorn: "⌜", ulcorner: "⌜", ulcrop: "⌏", ultri: "◸", Umacr: "Ū", umacr: "ū", uml: "¨", UnderBar: "_", UnderBrace: "⏟", UnderBracket: "⎵", UnderParenthesis: "⏝", Union: "⋃", UnionPlus: "⊎", Uogon: "Ų", uogon: "ų", Uopf: "𝕌", uopf: "𝕦", UpArrow: "↑", Uparrow: "⇑", uparrow: "↑", UpArrowBar: "⤒", UpArrowDownArrow: "⇅", UpDownArrow: "↕", Updownarrow: "⇕", updownarrow: "↕", UpEquilibrium: "⥮", upharpoonleft: "↿", upharpoonright: "↾", uplus: "⊎", UpperLeftArrow: "↖", UpperRightArrow: "↗", Upsi: "ϒ", upsi: "υ", upsih: "ϒ", Upsilon: "Υ", upsilon: "υ", UpTee: "⊥", UpTeeArrow: "↥", upuparrows: "⇈", urcorn: "⌝", urcorner: "⌝", urcrop: "⌎", Uring: "Ů", uring: "ů", urtri: "◹", Uscr: "𝒰", uscr: "𝓊", utdot: "⋰", Utilde: "Ũ", utilde: "ũ", utri: "▵", utrif: "▴", uuarr: "⇈", Uuml: "Ü", uuml: "ü", uwangle: "⦧", vangrt: "⦜", varepsilon: "ϵ", varkappa: "ϰ", varnothing: "∅", varphi: "ϕ", varpi: "ϖ", varpropto: "∝", vArr: "⇕", varr: "↕", varrho: "ϱ", varsigma: "ς", varsubsetneq: "⊊︀", varsubsetneqq: "⫋︀", varsupsetneq: "⊋︀", varsupsetneqq: "⫌︀", vartheta: "ϑ", vartriangleleft: "⊲", vartriangleright: "⊳", Vbar: "⫫", vBar: "⫨", vBarv: "⫩", Vcy: "В", vcy: "в", VDash: "⊫", Vdash: "⊩", vDash: "⊨", vdash: "⊢", Vdashl: "⫦", Vee: "⋁", vee: "∨", veebar: "⊻", veeeq: "≚", vellip: "⋮", Verbar: "‖", verbar: "|", Vert: "‖", vert: "|", VerticalBar: "∣", VerticalLine: "|", VerticalSeparator: "❘", VerticalTilde: "≀", VeryThinSpace: " ", Vfr: "𝔙", vfr: "𝔳", vltri: "⊲", vnsub: "⊂⃒", vnsup: "⊃⃒", Vopf: "𝕍", vopf: "𝕧", vprop: "∝", vrtri: "⊳", Vscr: "𝒱", vscr: "𝓋", vsubnE: "⫋︀", vsubne: "⊊︀", vsupnE: "⫌︀", vsupne: "⊋︀", Vvdash: "⊪", vzigzag: "⦚", Wcirc: "Ŵ", wcirc: "ŵ", wedbar: "⩟", Wedge: "⋀", wedge: "∧", wedgeq: "≙", weierp: "℘", Wfr: "𝔚", wfr: "𝔴", Wopf: "𝕎", wopf: "𝕨", wp: "℘", wr: "≀", wreath: "≀", Wscr: "𝒲", wscr: "𝓌", xcap: "⋂", xcirc: "◯", xcup: "⋃", xdtri: "▽", Xfr: "𝔛", xfr: "𝔵", xhArr: "⟺", xharr: "⟷", Xi: "Ξ", xi: "ξ", xlArr: "⟸", xlarr: "⟵", xmap: "⟼", xnis: "⋻", xodot: "⨀", Xopf: "𝕏", xopf: "𝕩", xoplus: "⨁", xotime: "⨂", xrArr: "⟹", xrarr: "⟶", Xscr: "𝒳", xscr: "𝓍", xsqcup: "⨆", xuplus: "⨄", xutri: "△", xvee: "⋁", xwedge: "⋀", Yacute: "Ý", yacute: "ý", YAcy: "Я", yacy: "я", Ycirc: "Ŷ", ycirc: "ŷ", Ycy: "Ы", ycy: "ы", yen: "¥", Yfr: "𝔜", yfr: "𝔶", YIcy: "Ї", yicy: "ї", Yopf: "𝕐", yopf: "𝕪", Yscr: "𝒴", yscr: "𝓎", YUcy: "Ю", yucy: "ю", Yuml: "Ÿ", yuml: "ÿ", Zacute: "Ź", zacute: "ź", Zcaron: "Ž", zcaron: "ž", Zcy: "З", zcy: "з", Zdot: "Ż", zdot: "ż", zeetrf: "ℨ", ZeroWidthSpace: "​", Zeta: "Ζ", zeta: "ζ", Zfr: "ℨ", zfr: "𝔷", ZHcy: "Ж", zhcy: "ж", zigrarr: "⇝", Zopf: "ℤ", zopf: "𝕫", Zscr: "𝒵", zscr: "𝓏", zwj: "\u200d", zwnj: "\u200c"
  };
});
enifed('simple-html-tokenizer/index', ['exports', 'simple-html-tokenizer/html5-named-char-refs', 'simple-html-tokenizer/entity-parser', 'simple-html-tokenizer/evented-tokenizer', 'simple-html-tokenizer/tokenizer', 'simple-html-tokenizer/tokenize'], function (exports, _simpleHtmlTokenizerHtml5NamedCharRefs, _simpleHtmlTokenizerEntityParser, _simpleHtmlTokenizerEventedTokenizer, _simpleHtmlTokenizerTokenizer, _simpleHtmlTokenizerTokenize) {
  'use strict';

  exports.HTML5NamedCharRefs = _simpleHtmlTokenizerHtml5NamedCharRefs.default;
  exports.EntityParser = _simpleHtmlTokenizerEntityParser.default;
  exports.EventedTokenizer = _simpleHtmlTokenizerEventedTokenizer.default;
  exports.Tokenizer = _simpleHtmlTokenizerTokenizer.default;
  exports.tokenize = _simpleHtmlTokenizerTokenize.default;
});
enifed('simple-html-tokenizer/tokenize', ['exports', 'simple-html-tokenizer/tokenizer', 'simple-html-tokenizer/entity-parser', 'simple-html-tokenizer/html5-named-char-refs'], function (exports, _simpleHtmlTokenizerTokenizer, _simpleHtmlTokenizerEntityParser, _simpleHtmlTokenizerHtml5NamedCharRefs) {
  'use strict';

  exports.default = tokenize;

  function tokenize(input, options) {
    var tokenizer = new _simpleHtmlTokenizerTokenizer.default(new _simpleHtmlTokenizerEntityParser.default(_simpleHtmlTokenizerHtml5NamedCharRefs.default), options);
    return tokenizer.tokenize(input);
  }
});
enifed('simple-html-tokenizer/tokenizer', ['exports', 'simple-html-tokenizer/evented-tokenizer'], function (exports, _simpleHtmlTokenizerEventedTokenizer) {
  'use strict';

  function Tokenizer(entityParser, options) {
    this.token = null;
    this.startLine = 1;
    this.startColumn = 0;
    this.options = options || {};
    this.tokenizer = new _simpleHtmlTokenizerEventedTokenizer.default(this, entityParser);
  }

  Tokenizer.prototype = {
    tokenize: function (input) {
      this.tokens = [];
      this.tokenizer.tokenize(input);
      return this.tokens;
    },

    tokenizePart: function (input) {
      this.tokens = [];
      this.tokenizer.tokenizePart(input);
      return this.tokens;
    },

    tokenizeEOF: function () {
      this.tokens = [];
      this.tokenizer.tokenizeEOF();
      return this.tokens[0];
    },

    reset: function () {
      this.token = null;
      this.startLine = 1;
      this.startColumn = 0;
    },

    addLocInfo: function () {
      if (this.options.loc) {
        this.token.loc = {
          start: {
            line: this.startLine,
            column: this.startColumn
          },
          end: {
            line: this.tokenizer.line,
            column: this.tokenizer.column
          }
        };
      }
      this.startLine = this.tokenizer.line;
      this.startColumn = this.tokenizer.column;
    },

    // Data

    beginData: function () {
      this.token = {
        type: 'Chars',
        chars: ''
      };
      this.tokens.push(this.token);
    },

    appendToData: function (char) {
      this.token.chars += char;
    },

    finishData: function () {
      this.addLocInfo();
    },

    // Comment

    beginComment: function () {
      this.token = {
        type: 'Comment',
        chars: ''
      };
      this.tokens.push(this.token);
    },

    appendToCommentData: function (char) {
      this.token.chars += char;
    },

    finishComment: function () {
      this.addLocInfo();
    },

    // Tags - basic

    beginStartTag: function () {
      this.token = {
        type: 'StartTag',
        tagName: '',
        attributes: [],
        selfClosing: false
      };
      this.tokens.push(this.token);
    },

    beginEndTag: function () {
      this.token = {
        type: 'EndTag',
        tagName: ''
      };
      this.tokens.push(this.token);
    },

    finishTag: function () {
      this.addLocInfo();
    },

    markTagAsSelfClosing: function () {
      this.token.selfClosing = true;
    },

    // Tags - name

    appendToTagName: function (char) {
      this.token.tagName += char;
    },

    // Tags - attributes

    beginAttribute: function () {
      this._currentAttribute = ["", "", null];
      this.token.attributes.push(this._currentAttribute);
    },

    appendToAttributeName: function (char) {
      this._currentAttribute[0] += char;
    },

    beginAttributeValue: function (isQuoted) {
      this._currentAttribute[2] = isQuoted;
    },

    appendToAttributeValue: function (char) {
      this._currentAttribute[1] = this._currentAttribute[1] || "";
      this._currentAttribute[1] += char;
    },

    finishAttributeValue: function () {}
  };

  exports.default = Tokenizer;
});
enifed("simple-html-tokenizer/utils", ["exports"], function (exports) {
  "use strict";

  exports.isSpace = isSpace;
  exports.isAlpha = isAlpha;
  exports.preprocessInput = preprocessInput;
  var WSP = /[\t\n\f ]/;
  var ALPHA = /[A-Za-z]/;
  var CRLF = /\r\n?/g;

  function isSpace(char) {
    return WSP.test(char);
  }

  function isAlpha(char) {
    return ALPHA.test(char);
  }

  function preprocessInput(input) {
    return input.replace(CRLF, "\n");
  }
});
requireModule("ember-debug");
requireModule("ember-template-compiler");

}());

;
if (typeof exports === "object") {
  module.exports = Ember.__loader.require("ember-template-compiler");
 }