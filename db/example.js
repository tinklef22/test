
console.log(Table('users').find({ name: 'test', gender: { $like: "%M%" }, $select: ['name', 'age'], age: { $nin: [1, 2, 3] }, ok: { $lt: 200, $gt: 100 }, $sort: { name: 'asc', age: 'desc' }, $skip: 10, $limit: 10 }));

console.log(Table('users').find({
    $and: [
        { age: { $gte: 18 } },
        { $or: [{ country: 'USA' }, { country: 'Canada' }] }
    ]
}));

console.log(Table('users').find({ date: new Date().getTime() }));
console.log(Table('users').create({ name: 'test', age: 20 }));
console.log(Table('users').create([{ name: 'test', age: 20 }, { name: 'test2', age: 30 }]));


console.log(Table("users").aggregate([
    {
        $match: {
            score: {
                $gt: 80
            }
        }
    },
    {
        $group: { count: { $sum: 1 } }
    },
    {
        $sort: {
            total: -1
        }
    },
    {
        $limit: 10
    },
    {
        $count: "total"
    }
]));
