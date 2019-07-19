const config = require('config')
const fetch = require('node-fetch')
const Queue = require('better-queue')
const zlib = require('zlib')
const split2 = require('split2')

const checkStatus = (res) => {
  if (res.ok) {
    return res
  } else {
    return Promise.reject(new Error(
      `${res.url}: ${res.status} ${res.statusText}`
    ))
  }
}

const mokurokuQueue = new Queue(async (t, cb) => {
  const url = `https://maps.gsi.go.jp/xyz/${t}/mokuroku.csv.gz`
  fetch(url)
    .then(checkStatus)
    .then(res => {
      const gunzip = zlib.createGunzip()
      gunzip.pipe(split2()).on('data', (line) => {
        const path = line.split(',')[0]
        if (path.endsWith('.geojson')) {
          tileQueue.push(
            `https://maps.gsi.go.jp/xyz/${t}/${path}`
          )
        }
      })
      res.body.pipe(gunzip)
      gunzip.on('data', data => {
      }).on('end', () => {
        cb(null, t)
      })
    })
    .catch(e => {
      const msg = `${url}: ${e}`
      console.error(msg)
      cb(msg, t)
    })
}, {
  maxRetries: 10,
  retryDelay: 5000
})

const tileQueue = new Queue(async (url, cb) => {
  fetch(url)
    .then(checkStatus)
    .then(res => res.json())
    .then(json => {
      for (f of json.features) {
        writeQueue.push(f)
      }
      cb(null, url)
    })
    .catch(e => {
      const msg = `${url}: ${e}`
      console.error(msg)
      cb(msg, t)
    })
}, {
  concurrent: 10,
  maxRetries: 10,
  retryDelay: 5000
})

const writeQueue = new Queue(async (s, cb) => {
  console.log(`\x1e${JSON.stringify(s)}`)
  cb(null, s)
}, {
  maxRetries: 10,
  retryDelay: 5000
})

const main = async () => {
  for (const t of config.get('ts')) {
    mokurokuQueue.push(t)
  }
}

main()

