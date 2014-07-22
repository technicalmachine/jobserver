assert = require 'assert'
jobserver = require '../jobserver'
fs = require('fs')
path = require('path')

describe 'blobStoreMem', ->
	it 'stores and retrieves blobs', (next) ->
		s = new jobserver.BlobStoreMem()
		data = new Buffer('asdfghjkl')
		b1 = s.putBlob(data)
		s.getBlob b1.id, (b2) ->
			assert.equal(b1.data, b2.data)
			next()

	it 'hashes identical blobs to the same id', ->
		s = new jobserver.BlobStoreMem()
		data = new Buffer('asdfghjkl')
		b1 = s.putBlob(data)
		b2 = s.putBlob(data)
		assert.equal(b1.id, b2.id)
		assert.equal(b1.data, b2.data)

describe 'blobStoreLocal', ->
	it 'creates the process env folder if none exists', ->
		setupTestEnv()
		s = new jobserver.BlobStoreLocal(process.env.LOCAL_STORE_TEST)
		assert(fs.existsSync(process.env.LOCAL_STORE_TEST))

	it 'stores and retrives blobs', (next) ->
		data = new Buffer('asdfghjkl')
		s = new jobserver.BlobStoreLocal(process.env.LOCAL_STORE_TEST)
		put = s.putBlob(data)
		assert.equal(put.meta.path.indexOf(process.env.LOCAL_STORE_TEST), 0)
		s.getBlob put.id, (blob2) ->
			assert.equal(put.meta.path, blob2.meta.path)
			assert.equal(put.id, blob2.id)
			next()

	it 'loads all files on fs during construction', (next) ->
		# create a bunch of test data
		setupTestEnv()
		s = new jobserver.BlobStoreLocal(process.env.LOCAL_STORE_TEST)
		first = s.putBlob('123qweasd')
		s.putBlob('123qweasd')
		s.putBlob('123qweasd1')
		s.putBlob('123qweasd2')
		# should have 3 items
		otherBlobStore = new jobserver.BlobStoreLocal(process.env.LOCAL_STORE_TEST)
		assert.equal(Object.keys(otherBlobStore.blobs).length, 3)

		#make sure that getblob calls can read files
		s.getBlob first.id , (blobData) ->
			assert.equal(blobData.cached.toString(), '123qweasd')
			next()

# sets up test fs area
setupTestEnv = ->
	process.env.LOCAL_STORE_TEST = path.join(__dirname, '/localstore')
	deleteFolderRecursive = (currPath) ->
		files = fs.readdirSync(currPath);
		for file in files
			do (file) ->
				filePath = path.join(currPath, file)
				if fs.lstatSync(filePath).isDirectory()
					deleteFolderRecursive(filePath);
				else
					fs.unlinkSync(filePath)
		fs.rmdirSync(currPath)

	if fs.existsSync(process.env.LOCAL_STORE_TEST)
		deleteFolderRecursive(process.env.LOCAL_STORE_TEST)

# Takes a list of names and returns an object with methods with those names, which must be called in order
orderingSpy = (events) ->
	r = {}
	counter = 0

	events.forEach (name) ->
		r[name] = ->
			if events[counter] == name
				counter++
			else
				throw new Error("`#{name}` called in the wrong order (expected `#{events[counter]}` next)")

	return r

describe 'orderingSpy', ->
	it 'passes', ->
		o = orderingSpy(['a', 'b', 'c', 'c', 'd'])
		o.a()
		o.b()
		o.c()
		o.c()
		o.d()

	it 'fails', ->
		o = orderingSpy(['a', 'b', 'c', 'd'])
		assert.throws ->
			o.a()
			o.b()
			o.d()
			o.c()

class TestJob extends jobserver.Job
	constructor: (@run) ->
		super()

describe 'Job', ->
	jobstore = blobstore = server = null

	beforeEach ->
		jobstore = new jobserver.JobStoreMem()
		blobstore = new jobserver.BlobStoreMem()
		server = new jobserver.Server(jobstore, blobstore)
		server.defaultExecutor = new jobserver.Executor()

	it 'Runs and emits states and collects a log', (cb) ->
		ordered = orderingSpy(['waiting', 'pending', 'running', 'exec', 'success', 'settled'])
		job = new TestJob (ctx, cb) ->
			ordered.exec()
			ctx.write("test1\n")
			setImmediate =>
				ctx.write("test2\n")
				cb(true)

		job.on 'state', (s) ->
			ordered[s]()

		job.on 'settled', ->
			ordered.settled()
			assert.equal(job.ctx.log, 'test1\ntest2\n')
			cb()

		server.submit(job)

	it 'Runs dependencies in order', (done) ->
		ordered = orderingSpy(['prestart', 'depstart', 'depstart', 'depdone', 'depdone' , 'parentstart'])

		job0 = new TestJob (ctx, cb) ->
			ordered.prestart()
			setImmediate(-> cb(true))

		depfn = (ctx, cb) ->
			ordered.depstart()
			setImmediate ->
				ordered.depdone()
				cb(true)

		job1 = new TestJob depfn
		job2 = new TestJob depfn
		job3 = new TestJob (ctx, cb) ->
			ordered.parentstart()
			setImmediate(-> cb(true))

		job1.explicitDependencies.push(job0)
		job2.explicitDependencies.push(job0)
		job3.explicitDependencies.push(job1, job2)

		server.submit job3, ->
			done()

	it 'Generates implicit dependencies based on input'
	it 'Rejects dependency cycles'
	it 'Hashes consistently'
	it 'Avoids recomputing calculated jobs'

describe 'SeriesExecutor', ->
		e = null
		beforeEach ->
			e = new jobserver.SeriesExecutor(new jobserver.Executor())

		it 'Runs jobs in order', ->
			spy = new orderingSpy(['j1_start', 'j1_done', 'j2', 'j3_start', 'j3_done'])
			j1 = new TestJob (ctx, cb) ->
				spy.j1_start()
				setTimeout (->
					spy.j1_done()
					cb(true)
				), 100

			j2 = new TestJob (ctx, cb) ->
				spy.j2()
				cb(true)

			j3 = new TestJob (ctx, cb) ->
				spi.j3_start()
				setTimeout (->
					spy.j3_done()
					cb(false)
				), 10

			e.enqueue(j1)
			e.enqueue(j2)
			e.enqueue(j3)
