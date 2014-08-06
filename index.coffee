{EventEmitter} = require('events')
{Transform} = require('stream')
crypto = require('crypto')

util = require('util')
temp = require('temp')
rimraf = require('rimraf')
child_process = require('child_process')
path = require 'path'
fs = require 'fs'
async = require 'async'

hashDigest = (hash) -> hash.toString('base64').replace(/\=/g, '').replace(/\//g, '-')

STATES = [
  'waiting'   # depends on outputs of other jobs that have not finished yet
  'pending'   # ready to run, but blocked on hardware resources
  'running'   # self-explanatory
  'success'   # Ran and produced its outputs
  'fail'      # Ran and failed to produce its outputs. This status is cached. Another run with the same inputs would also fail.
  'abort'     # Ran and did not produce its output due to e.g. a network problem. Running again may succeed.
]

# A `Server` maintains the global job list and aggregates events for the UI
@Server = class Server extends EventEmitter
  constructor: (@jobStore, @blobStore) ->
    unless @blobStore
      @blobStore = new BlobStoreMem()
    unless @jobStore
      JobStoreSQLite = require('./jobstore_sqlite')
      @jobStore = new JobStoreSQLite(':memory:')

    @activeJobs = {}

  submit: (job, doneCb) ->
    server = this

    if job.alreadySubmitted
      if doneCb
        job.once 'settled', doneCb
      return
    job.alreadySubmitted = true

    @jobStore.addJob job, =>
      @activeJobs[job.id] = job

      server.emit 'submit', job

      job.on 'state', (state) ->
        server.emit 'job.state', this, state

      job.once 'settled', =>
        delete @activeJobs[job.id]
        doneCb() if doneCb

      job.submitted(this)

  job: (id, cb) ->
    id = parseInt(id, 10)
    if job = @activeJobs[id]
      setImmediate -> cb(job)
    else
      @jobStore.getJob(id, cb)

  pipeLogStream: (job, dest) ->
    if job instanceof Job
      pipe = ->
        dest.write(job.ctx.log)
        job.ctx.pipe(dest)
        dest.on 'close', -> job.ctx.unpipe(dest)
      if job.ctx
        pipe()
      else
        job.on 'started', pipe
        dest.on 'close', -> job.removeListener 'started', pipe
    else
      @blobStore.getBlob job.logBlob, (blob) ->
        dest.end(blob or '')


  relatedJobs: (id, cb) ->
    id = parseInt(id, 10)
    @jobStore.getRelatedJobs(id, cb)

  jsonableState: ->
    {}

# A `FutureResult` is a reference to a result of a `Job` which may not yet have completed
@FutureResult = class FutureResult
  constructor: (@job, @key) ->
  get: ->
    if @job.state == 'success'
      @job.result[@key]
    else
      throw new Error("Accessing result of job with status #{@job.status}")

  getBuffer: (cb) -> @get().getBuffer(cb)
  getId: -> @get().id

# `BlobStore` is the abstract base class for result file data storage.
# Subclasses persist Buffers and retrieve them by hash.
@BlobStore = class BlobStore
  newBlob: (buffer, meta) ->
    throw new Error("Abstract method")
  getBlob: (id, cb) ->
    throw new Error("Abstract method")
  hash: (buffer) ->
    hashDigest(crypto.createHash('sha256').update(buffer).digest())

# An item in a BlobStore
@Blob = class Blob
  constructor: (@store, @id, @meta) ->

  getBuffer: (cb) -> @store.getBlob(@id, cb)
  getId: -> @id

# Abstact base class for database of job history
@JobStore = class JobStore

# A Stream transformer that captures a copy of the streamed data and passes it through
class TeeStream extends Transform
  constructor: ->
    super()
    @log = ''

  _transform: (chunk, encoding, callback) ->
    @log += chunk.toString('utf8')
    this.push(chunk)
    callback()

@JobInfo = class JobInfo extends EventEmitter
  jsonableState: ->
    {@id, @name, @description, @state, settled: @settled()}

  settled: ->
    @state in ['success', 'fail', 'abort']

# Object containing the state and logic for a job. Subclasses can override the behavior
@Job = class Job extends JobInfo
  resultNames: []
  pure: false

  constructor: (@executor, @inputs={}) ->
    @explicitDependencies = []
    @state = null
    @result = {}

    for key in @resultNames
      @result[key] = @result[key] = new FutureResult(this, key)

    @config()

  config: ->

  submitted: (@server) ->
    @executor ?= @server.defaultExecutor

    @dependencies = @explicitDependencies.slice(0)
    for k, v of @inputs
      if v instanceof FutureResult
        @dependencies.push(v.job)

    for dep in @dependencies
      server.submit(dep)
      dep.withId (job) => @emit 'dependencyAdded', job

      unless dep.settled()
        dep.once 'settled', =>
          @checkDeps()

    @saveState 'waiting'
    @checkDeps()

  withId: (cb) ->
    job = this
    if @id?
      setImmediate -> cb(job)
    else
      @once 'state', -> cb(job)

  checkDeps: ->
    unless @state is 'waiting'
      throw new Error("checkDeps in state #{@state}")

    ready = true
    for dep in @dependencies
      switch dep.state
        when 'error'
          return @saveState 'error'
        when 'abort'
          return @saveState 'abort'
        when 'success'
          # nothing
        else
          ready = false

    if ready
      @emit 'inputsReady'
      if @pure
        @server.jobStore.resultByHash @hash(), (completion) =>
          if completion
            @result.fromCache = completion.id
            {@result, @startTime, @endTime} = completion
            @saveState(completion.status)
          else
            @enqueue()
      else
        @enqueue()

  hash: ->
    unless @pure
      throw new Error("Can't hash impure job (pure jobs cannot depend on impure jobs)")

    unless @_hash
      hasher = crypto.createHash('sha256')
      hasher.update(@name)

      depHashes = (dep.hash() for dep in @explicitDependencies)
      depHashes.sort()
      hasher.update(hash) for hash in depHashes

      for key in Object.keys(@inputs).sort()
        hasher.update(key)
        hasher.update(":")
        value = @inputs[key]
        if value instanceof FutureResult
          value = value.get()

        if value instanceof Blob
          hasher.update(value.hash)
        else
          hasher.update(JSON.stringify(value))
        hasher.update(",")

      @_hash = hashDigest(hasher.digest())

    @_hash

  enqueue: (executor) ->
    @executor ?= executor
    @saveState 'pending'
    @ctx = new Context(this)
    @executor.enqueue(this)
    @emit 'started'

  saveState: (state) ->
    if state not in STATES
      throw new Error("Invalid status '#{state}'")
    @state = state
    @emit 'state', state

    if @settled()
      @emit 'settled'

  beforeRun: () ->
    @startTime = new Date()
    @saveState 'running'

  afterRun: (result) ->
    @endTime = new Date()
    @fromCache = false

    if @server
      @logBlob = @server.blobStore.putBlob(@ctx.log, {from: 'log', jobId: @id})

    if @pure
      @emit 'computed'

    if result
      @saveState('success')
    else
      @saveState('fail')

  name: ''
  description: ''

  # Override this
  run: (ctx) ->
    ctx.write("Default exec!\n")
    setImmediate( -> ctx.done(null) )

# An in-memory BlobStore
@BlobStoreMem = class BlobStoreMem extends BlobStore
  constructor: ->
    @blobs = {}

  putBlob: (buffer, meta, cb) ->
    id = @hash(buffer)
    if not @blobs[id]
      @blobs[id] = buffer
    setImmediate(cb)
    new Blob(this, id, meta)

  getBlob: (id, cb) ->
    v = @blobs[id]
    setImmediate ->
      cb(v)
    return

# An Executor provides a Job a Context to access resources
Context: class Context extends TeeStream
  constructor: (@job)->
    super()
    @_completed = false
    @queue = []
    # Note: this needs to be piped somewhere by default so the Transform doesn't accumulate data.
    # If not stdout, then a null sink, or some other way of fixing this.
    @pipe(process.stdout)

  before: (cb) ->
    setImmediate(cb)

  after: (cb) ->
    setImmediate(cb)

  _doSeries: (cb) ->
    async.series @queue, @_done

  _done: (err) =>
    if err
      @write("Failed with error: #{err.stack ? err}\n")

    if @_completed
      console.trace("Job #{@job.constructor.name} completed multiple times")
      return
    @_completed = true

    @after (e) =>
      throw e if e
      @end()
      @job.log = @log
      @job.afterRun(!err)

  then: (fn) ->
    @queue.push(fn)

  mixin: (obj) ->
    for k, v of obj
      this[k] = v

# An Executor manages the execution of a set of jobs. May also wrap access to an execution resource
@Executor = class Executor
  enqueue: (job) ->
    try
      job.beforeRun()
      job.run(job.ctx)
      job.ctx._doSeries()
    catch e
      job.ctx._done(e)

# An executor combinator that runs jobs one at a time in series on a specified executor
@SeriesExecutor = class SeriesExecutor extends Executor
  constructor: (@executor) ->
    super()
    @currentJob = null
    @queue = []

  enqueue: (job) =>
    @queue.push(job)
    @shift() unless @currentJob

  shift: =>
      @currentJob = @queue.shift()
      if @currentJob
        @currentJob.on 'settled', @shift
        @executor.enqueue(@currentJob)

@LocalExecutor = class LocalExecutor extends Executor
  enqueue: (job) ->
    ctx = job.ctx
    temp.mkdir "jobserver-#{job.name}", (err, dir) =>
      ctx.mixin @ctxMixin
      ctx.dir = dir
      ctx._cwd = dir
      ctx._env ?= {}
      ctx.envImmediate(process.env)
      ctx.write("Working directory: #{dir}\n")

      ctx.on 'end', =>
        rimraf dir, ->

      Executor::enqueue.call(this, job)

  ctxMixin:
    envImmediate: (e) ->
      for k, v of e
        @_env[k] = v
      null

    env: (e) ->
      @then (cb) =>
        @envImmediate(e)
        cb()

    cd: (p) ->
      @then (cb) =>
        @_cwd = path.resolve(@_cwd, p)
        cb()

    run: (command, args) ->
      @then (cb) =>
        unless util.isArray(args)
          args = ['-c', command]
          command = 'sh'

        @write("$ #{command + if args then ' ' + args.join(' ') else ''}\n")
        p = child_process.spawn command, args, {cwd: @_cwd, env: @_env}
        p.stdout.pipe(this, {end: false})
        p.stderr.pipe(this, {end: false})
        p.on 'close', (code) =>
          cb(if code != 0 then "Exited with #{code}")

    put: (content, filename) ->
      @then (cb) =>
        content.getBuffer (data) =>
          @write("#{data.length} bytes to #{path.resolve(@_cwd, filename)}\n")
          fs.writeFile path.resolve(@_cwd, filename), data, cb

    get: (output, filename) ->
      @then (cb) =>
        fs.readFile path.resolve(@_cwd, filename), (err, data) =>
          return cb(err) if err
          @job.result[output] = @job.server.blobStore.putBlob(data, {from: 'file', jobId: @job.id, name: output})
          console.log("#{data.length} bytes from #{path.resolve(@_cwd, filename)} as #{output} on #{@job.id}:", @job.result[output])
          cb()

    git_clone: (repo, branch, dir) ->
      @run('git', ['clone', '--depth=1', '-b', branch, '--', repo, dir])
