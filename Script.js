db

db.createCollection('youtubers')
db.createCollection('videos')
db.createCollection('raw_reviews')
db.createCollection('reviews')


/*db.youtubers.insertOne({
  channel_name: "RISABAE",
  url: "https://www.youtube.com/@RISABAE",
  foreign:true
})

db.reviews.find({status:'COMPLETED'},{})
db.raw_reviews.distinct("product_name")
db.reviews.countDocuments()
db.reviews.updateMany(
  {status: "NOTFOUND"},
  {$set: {status: "PENDING"}}
)
db.products.aggregate([
  { $match: { "inciapi_raw_data.efficacySummary.topEffects": { $exists: true } } },
  { $unwind: "$inciapi_raw_data.efficacySummary.topEffects" },
  { $group: { _id: "$inciapi_raw_data.efficacySummary.topEffects.target" } },
  { $sort: { _id: 1 } }
])
*/

// 얘너 잘 제외되는지 각스텝별로 봐라
db.reviews.find({video_id:'RKVf0uGzELI'},{})
db.raw_reviews.find({video_id:'RKVf0uGzELI'},{})
db.videos.find({video_id:'RKVf0uGzELI'},{})


db.youtubers.find({})
db.videos.find({})
db.raw_reviews.find({})
db.reviews.find({})
db.products.find({})
db.products_summarized.find({})




