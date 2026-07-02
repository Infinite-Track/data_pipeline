db.createCollection('products_summarized')
db.reviews.find({})    

db.createCollection('products')
db.products.findOne({product_name: 'powder primer'}, {ingredients: 1, _id: 0})
db
// 최상위 필드명만
Object.keys(db.product_summarized.findOne())
db.reviews.find({})   
db.products.find({})
db.products_summarized.find({})


db.reviews.countDocuments()
db.products.countDocuments()
db.products_summarized.countDocuments()



// 101~120번째만 (20개)
db.products.find().sort({ _id: 1 }).skip(100).limit(10)



db.products_summarized.drop()


