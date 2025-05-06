const { BskyAgent } = require('@atproto/api')
const { Kafka } = require('kafkajs')
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')
const dotenv = require('dotenv')
dotenv.config()

const agent = new BskyAgent({
  service: 'https://bsky.social'
})

const username = process.env.BLUESKY_USERNAME
const password = process.env.BLUESKY_PASSWORD
const schemaRegistryUrl = process.env.SCHEMA_REGISTRY_URL 
const schemaRegistryUser = process.env.SCHEMA_REGISTRY_API_KEY
const schemaRegistryPass = process.env.SCHEMA_REGISTRY_API_SECRET

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
})

const registry = new SchemaRegistry({
  host: schemaRegistryUrl,
  auth: {
    username: schemaRegistryUser,
    password: schemaRegistryPass
  }
})

const producer = kafka.producer()
const TOPIC_NAME = 'BlueSkyMessages'

const avroSchema = {
  type: 'record',
  name: 'BlueSkyMessage',
  namespace: 'com.example.bluesky',
  fields: [
    { name: 'author', type: 'string' },
    { name: 'text', type: 'string' },
    { name: 'postedAt', type: 'string' },
    { name: 'images', type: 'int' },
    { name: 'hashtags', type: { type: 'array', items: 'string' } },
    { name: 'mentions', type: { type: 'array', items: 'string' } },
    { name: 'linkCount', type: 'int' },
    { name: 'repostBy', type: ['null', 'string'], default: null },
    { name: 'replyTo', type: ['null', 'string'], default: null },
    { name: 'likeCount', type: 'int' },
    { name: 'repostCount', type: 'int' },
    { name: 'replyCount', type: 'int' },
    { name: 'quoteCount', type: 'int' },
    { name: 'lang', type: ['null', 'string'], default: null }
  ]
}

const processedPosts = new Set()

async function subscribeToBlueSky() {
  try {
    // Register the schema (registers only once and returns the schema ID)
    const { id: schemaId } = await registry.register({
      type: SchemaType.AVRO,
      schema: JSON.stringify(avroSchema)
    })

    // Connect to Kafka
    await producer.connect()
    console.log('Connected to Kafka')

    // Login to Bluesky
    await agent.login({ identifier: username, password: password })
    console.log('Logged in to Bluesky')

    while (true) {
      try {
        const { data } = await agent.getTimeline()

        for (const item of data.feed) {
          // Use a unique field (e.g. "uri") to identify the post.
          const postId = item.post.uri
          
          // Skip if this post has already been processed
          if (processedPosts.has(postId)) {
            console.log('Skipping duplicate post:', postId)
            continue
          }
          
          // Mark this post as processed
          processedPosts.add(postId)

          console.log(JSON.stringify(item))
          const record = item.post.record
          const facets = record.facets || []
          const hashtags = []
          const mentions = []
          let linkCount = 0

          for (const facet of facets) {
            for (const feature of facet.features) {
              if (feature.$type === 'app.bsky.richtext.facet#tag') {
                hashtags.push(feature.tag)
              } else if (feature.$type === 'app.bsky.richtext.facet#mention') {
                mentions.push(feature.did)
              } else if (feature.$type === 'app.bsky.richtext.facet#link') {
                linkCount += 1
              }
            }
          }

          const payload = {
            author: item.post.author.handle,
            text: record.text,
            postedAt: item.post.indexedAt,
            images: item.post.embed?.images?.length || 0,
            hashtags,
            mentions,
            linkCount,
            repostBy: item.reason?.by?.handle || null,
            replyTo: record.reply?.parent?.uri || null,
            likeCount: item.post.likeCount || 0,
            repostCount: item.post.repostCount || 0,
            replyCount: item.post.replyCount || 0,
            quoteCount: item.post.quoteCount || 0,
            lang: record.langs?.[0] || null
          }

          // Serialize with Avro
          const encodedValue = await registry.encode(schemaId, payload)

          await producer.send({
            topic: TOPIC_NAME,
            messages: [{ value: encodedValue }]
          })

          console.log('Sent Avro message to Kafka:', payload)
        }

        // Sleep for 10 seconds before polling again
        await new Promise(resolve => setTimeout(resolve, 60000))
      } catch (error) {
        console.error('Error:', error)
        // In case of an error, wait 15 seconds before retrying
        await new Promise(resolve => setTimeout(resolve, 15000))
      }
    }
  } catch (error) {
    console.error('Startup Error:', error)
    await producer.disconnect()
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Received SIGTERM. Cleaning up...')
  await producer.disconnect()
  process.exit(0)
})

process.on('SIGINT', async () => {
  console.log('Received SIGINT. Cleaning up...')
  await producer.disconnect()
  process.exit(0)
})

subscribeToBlueSky()
