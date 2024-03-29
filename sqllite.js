// const CRUD = require("./sql");
const assert = require("assert");
const Table = require("./db");
const datas = require("./MOCK_DATA.json");
// const assert = require("assert");
const createTableFromJson = require("./createtable");

const knex = require("knex")({
  client: "sqlite3",
  connection: {
    filename: "./tinkle",
  },
  useNullAsDefault: true,
});

async function createUsersTable() {
  try {
    const result = await knex.raw(
      createTableFromJson({
        tableName: "users",
        columns: [
          {
            name: "id",
            type: "INTEGER",
            primaryKey: true,
            autoIncrement: true,
          },
          {
            name: "name",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "email",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "gender",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "ip_address",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "title",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "company",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "birthday",
            type: "INTEGER",
            nullable: false,
          },
          {
            name: "isSenior",
            type: "TEXT",
            nullable: false,
          },
          {
            name: "salary",
            type: "INTEGER",
            nullable: false,
          },
          {
            name: "address",
            type: "TEXT",
            nullable: false,
          },
        ],
      })
    );
    console.log(result);
  } catch (error) {
    console.error(error);
  } finally {
    knex.destroy();
  }
}

// createUsersTable();

async function createExampleTableData() {
  try {
    await knex.raw(Table("users").create(datas));
  } catch (error) {
    console.error(error);
  }
}

// createExampleTableData();

const testCases = [
  {
    name: "find with $limit",
    func: "find",
    input: {
      $limit: 3,
    },
    output: [
      {
        id: 1,
        name: "Garrard",
        email: "gpattemore0@tripadvisor.com",
        gender: "Male",
        ip_address: "32.34.141.142",
        title: "Rev",
        company: "Rhycero",
        birthday: 1654429211000,
        isSenior: "true",
        salary: 478272,
        address: "85 Superior Avenue",
      },
      {
        id: 2,
        name: "Marillin",
        email: "mwymer1@technorati.com",
        gender: "Female",
        ip_address: "241.137.201.222",
        title: "Dr",
        company: "Babbleopia",
        birthday: 1651862825000,
        isSenior: "true",
        salary: 420919,
        address: "05294 Kingsford Circle",
      },
      {
        id: 3,
        name: "Bo",
        email: "bpoulney2@wikipedia.org",
        gender: "Male",
        ip_address: "140.136.1.98",
        title: "Ms",
        company: "Topicshots",
        birthday: 1654435962000,
        isSenior: "true",
        salary: 484610,
        address: "699 Corscot Way",
      },
    ],
  },
  {
    name: "find with $skip $limit",
    func: "find",
    input: {
      $skip: 1,
      $limit: 3,
    },
    output: [
      {
        id: 2,
        name: "Marillin",
        email: "mwymer1@technorati.com",
        gender: "Female",
        ip_address: "241.137.201.222",
        title: "Dr",
        company: "Babbleopia",
        birthday: 1651862825000,
        isSenior: "true",
        salary: 420919,
        address: "05294 Kingsford Circle",
      },
      {
        id: 3,
        name: "Bo",
        email: "bpoulney2@wikipedia.org",
        gender: "Male",
        ip_address: "140.136.1.98",
        title: "Ms",
        company: "Topicshots",
        birthday: 1654435962000,
        isSenior: "true",
        salary: 484610,
        address: "699 Corscot Way",
      },
      {
        id: 4,
        name: "Vanni",
        email: "vtwiname3@github.io",
        gender: "Genderqueer",
        ip_address: "53.55.144.197",
        title: "Dr",
        company: "Dablist",
        birthday: 1680082459000,
        isSenior: "false",
        salary: 272366,
        address: "5 Debs Drive",
      },
    ],
  },
  {
    name: "find with $sort $limit",
    func: "find",
    input: {
      $sort: { id: "DESC" },
      $limit: 3,
    },
    output: [
      {
        id: 1000,
        name: "Wilton",
        email: "wpallesenrr@ow.ly",
        gender: "Male",
        ip_address: "21.194.112.172",
        title: "Honorable",
        company: "Tagcat",
        birthday: 1653447658000,
        isSenior: "true",
        salary: 211903,
        address: "987 Scoville Plaza",
      },
      {
        id: 999,
        name: "Lyndel",
        email: "lenrigorq@imdb.com",
        gender: "Female",
        ip_address: "155.53.8.154",
        title: "Ms",
        company: "Rhyzio",
        birthday: 1665795526000,
        isSenior: "true",
        salary: 211948,
        address: "8258 Scofield Trail",
      },

      {
        id: 998,
        name: "Bank",
        email: "bhegartyrp@virginia.edu",
        gender: "Bigender",
        ip_address: "82.184.248.105",
        title: "Mrs",
        company: "Meevee",
        birthday: 1655760923000,
        isSenior: "true",
        salary: 207443,
        address: "500 Lillian Lane",
      },
    ],
  },
  {
    name: "find with $limit $select $gt",
    func: "find",
    input: {
      $limit: 3,
      $select: ["name", "salary"],
      salary: { $gt: 478271 },
    },
    output: [
      { name: "Garrard", salary: 478272 },
      { name: "Bo", salary: 484610 },
      { name: "Whitney", salary: 489280 },
    ],
  },
  {
    name: "find with $limit $select $or",
    func: "find",
    input: {
      $limit: 3,
      $select: ["name", "salary"],
      $or: [{ name: "Garrard" }, { name: "Camila" }],
    },
    output: [
      { name: "Garrard", salary: 478272 },
      { name: "Camila", salary: 474385 },
    ],
  },
  {
    name: "find with $limit $select $and",
    func: "find",
    input: {
      $limit: 3,
      $select: ["name", "salary"],
      $and: [{ name: "Garrard", name: "478275" }],
    },
    output: [],
  },
  {
    name: "find with $limit $lt",
    func: "find",
    input: {
      id: { $lt: 30 },
      $limit: 3,
    },
    output: [
      {
        id: 1,
        name: "Garrard",
        email: "gpattemore0@tripadvisor.com",
        gender: "Male",
        ip_address: "32.34.141.142",
        title: "Rev",
        company: "Rhycero",
        birthday: 1654429211000,
        isSenior: "true",
        salary: 478272,
        address: "85 Superior Avenue",
      },
      {
        id: 2,
        name: "Marillin",
        email: "mwymer1@technorati.com",
        gender: "Female",
        ip_address: "241.137.201.222",
        title: "Dr",
        company: "Babbleopia",
        birthday: 1651862825000,
        isSenior: "true",
        salary: 420919,
        address: "05294 Kingsford Circle",
      },
      {
        id: 3,
        name: "Bo",
        email: "bpoulney2@wikipedia.org",
        gender: "Male",
        ip_address: "140.136.1.98",
        title: "Ms",
        company: "Topicshots",
        birthday: 1654435962000,
        isSenior: "true",
        salary: 484610,
        address: "699 Corscot Way",
      },
    ],
  },
  {
    name: "find with $limit $in $nin",
    func: "find",
    input: {
      id: { $in: [50, 60] },
      isSenior: { $nin: ["false"] },
      $limit: 3,
    },
    output: [
      {
        id: 50,
        name: "Porter",
        email: "pcoomes1d@wikipedia.org",
        gender: "Male",
        ip_address: "53.110.70.152",
        title: "Mrs",
        company: "Yamia",
        birthday: 1679802343000,
        isSenior: "true",
        salary: 327365,
        address: "95994 Sunnyside Point",
      },
    ],
  },
  {
    name: "find with $limit $gte $lte",
    func: "find",
    input: {
      birthday: { $lte: 1678233912000 },
      birthday: { $gte: 1654435962000 },
      $limit: 3,
    },
    output: [
      {
        id: 3,
        name: "Bo",
        email: "bpoulney2@wikipedia.org",
        gender: "Male",
        ip_address: "140.136.1.98",
        title: "Ms",
        company: "Topicshots",
        birthday: 1654435962000,
        isSenior: "true",
        salary: 484610,
        address: "699 Corscot Way",
      },
      {
        id: 4,
        name: "Vanni",
        email: "vtwiname3@github.io",
        gender: "Genderqueer",
        ip_address: "53.55.144.197",
        title: "Dr",
        company: "Dablist",
        birthday: 1680082459000,
        isSenior: "false",
        salary: 272366,
        address: "5 Debs Drive",
      },
      {
        id: 5,
        name: "Maren",
        email: "mjosefsson4@163.com",
        gender: "Female",
        ip_address: "23.138.83.245",
        title: "Mr",
        company: "Fatz",
        birthday: 1657949918000,
        isSenior: "true",
        salary: 32229,
        address: "79540 Hanson Lane",
      },
    ],
  },
  {
    name: "find with $ne $gte $select",
    func: "find",
    input: {
      isSenior: { $ne: ["false"] },
      $limit: 3,
      $select: ["name", "birthday"],
    },
    output: [
      { name: "Garrard", birthday: 1654429211000 },
      { name: "Marillin", birthday: 1651862825000 },
      { name: "Bo", birthday: 1654435962000 },
    ],
  },
  {
    name: "find with $like $limit $select",
    func: "find",
    input: {
      email: { $like: ["s%"] },
      $limit: 3,
      $select: ["name", "birthday , email"],
    },
    output: [
      { name: "Sly", birthday: 1663040029000, email: "sjuzekj@epa.gov" },
      {
        name: "Stephen",
        birthday: 1656089944000,
        email: "serikl@people.com.cn",
      },
      {
        name: "Sissie",
        birthday: 1671114333000,
        email: "spenticost13@home.pl",
      },
    ],
  },
  {
    name: "find with $nlike $limit $select",
    func: "find",
    input: {
      email: { $nlike: ["s%"] },
      $limit: 3,
      $select: ["name", "birthday , email"],
    },
    output: [
      {
        name: "Garrard",
        birthday: 1654429211000,
        email: "gpattemore0@tripadvisor.com",
      },
      {
        name: "Marillin",
        birthday: 1651862825000,
        email: "mwymer1@technorati.com",
      },
      { name: "Bo", birthday: 1654435962000, email: "bpoulney2@wikipedia.org" },
    ],
  },
  {
    name: "Aggregate with $match and $limit",
    func: "aggregate",
    input: [
      {
        $match: {
          salary: { $gt: 32229 },
        },
      },
      {
        $limit: 3,
      },
    ],
    output: [
      {
        id: 1,
        name: "Garrard",
        email: "gpattemore0@tripadvisor.com",
        gender: "Male",
        ip_address: "32.34.141.142",
        title: "Rev",
        company: "Rhycero",
        birthday: 1654429211000,
        isSenior: "true",
        salary: 478272,
        address: "85 Superior Avenue",
      },
      {
        id: 2,
        name: "Marillin",
        email: "mwymer1@technorati.com",
        gender: "Female",
        ip_address: "241.137.201.222",
        title: "Dr",
        company: "Babbleopia",
        birthday: 1651862825000,
        isSenior: "true",
        salary: 420919,
        address: "05294 Kingsford Circle",
      },
      {
        id: 3,
        name: "Bo",
        email: "bpoulney2@wikipedia.org",
        gender: "Male",
        ip_address: "140.136.1.98",
        title: "Ms",
        company: "Topicshots",
        birthday: 1654435962000,
        isSenior: "true",
        salary: 484610,
        address: "699 Corscot Way",
      },
    ],
  },
  {
    name: "Aggregate with $match and $group",
    func: "aggregate",
    input: [
      {
        $match: {
          salary: { $gt: 300000, $lt: 310600 },
        },
      },
      {
        $group: {
          _id: "$isSenior",
          totalAmount: { $sum: "$salary" },
        },
      },
    ],
    output: [
      { isSenior: "false", totalAmount: 4558163 },
      { isSenior: "true", totalAmount: 3656130 },
    ],
  },
  {
    name: "Aggregate with $match and $group",
    func: "aggregate",
    input: [
      {
        $match: {
          salary: { $gt: 300000, $lt: 310600 },
        },
      },
      {
        $group: {
          _id: "$isSenior",
          totalAmount: { $sum: "$salary" },
        },
      },
    ],
    output: [
      { isSenior: "false", totalAmount: 4558163 },
      { isSenior: "true", totalAmount: 3656130 },
    ],
  },
  {
    name: "Aggregate with $project , $short and $limit",
    func: "aggregate",
    input: [
      {
        $project: { name: 1, salary: 2 },
      },
      {
        $sort: { salary: "DESC" },
      },
      {
        $limit: 5,
      },
    ],
    output: [
      { name: "Rowen", salary: 499801 },
      { name: "Randal", salary: 499128 },
      { name: "Bartram", salary: 497128 },
      { name: "Arel", salary: 496351 },
      { name: "Adi", salary: 496310 },
    ],
  },
];

// const testCases = [
//     {
//       name: "Aggregate with $match and $group",
//       func: "aggregate",
//       input: [
//         {
//           $match: {
//             score: { $gt: 80 },
//           },
//         },
//         {
//           $group: { count: { $sum: 1 } },
//         },
//       ],
//       output: [],
//     },
//     {
//       name: "Find with $and and $or",
//       func: "find",
//       input: {
//         $and: [
//           { age: { $gte: 18 } },
//           { $or: [{ country: "USA" }, { country: "Canada" }] },
//         ],
//       },
//       output: [
//         /* KNEX DB Rows */
//       ],
//     },
//   ];

async function runTests() {
  for (const { name, func, input, output } of testCases) {
    console.log(`Running test: ${name}`);
    try {
      const sql = Table("users")[func](input);

      const result = await knex.raw(sql);
      console.log(`SQL executed successfully.`);
      const actualResult = result.rows || result;
      //   console.log("result ", actualResult);

      assert.deepEqual(
        actualResult,
        output,
        `Test failed: Expected result \n\n"${JSON.stringify(
          output
        )}" \nbut got \n\n"${JSON.stringify(actualResult)}"`
      );

      console.log(`Test passed.`);
    } catch (error) {
      console.error(`Test failed:`, error);
    }
  }
}

runTests();

// createExampleTableData();

// async function getUsers() {
//   try {
//     const data = await knex.raw(
//       Table("users").find({ $skip: 1, $limit: 10, $sort: { birthday: "DESC" } })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   }
// }
// getUsers();

// async function getUsers2() {
//   try {
//     const data = await knex.raw(
//       CRUD("users").find({ $skip: 1, $limit: 3, $sort: { age: "DESC" } })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   } finally {
//     knex.destroy();
//   }
// }
// // getUsers2();

// async function getUsers3() {
//   try {
//     const data = await knex.raw(
//       CRUD("users").find({ age: { $lt: 30 }, $select: ["name", "age"] })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   } finally {
//     knex.destroy();
//   }
// }
// // getUsers3();

// async function getUsers4() {
//   try {
//     const data = await knex.raw(
//       CRUD("users").find({
//         $or: [{ name: "User 2" }, { name: "User 3" }],
//         $select: ["name", "age"],
//       })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   } finally {
//     knex.destroy();
//   }
// }
// // getUsers4();
// async function getUsers5() {
//   try {
//     const data = await knex.raw(
//       CRUD("users").find({
//         $and: [{ name: "User 2", age: 60 }],
//         $select: ["name", "age"],
//       })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   } finally {
//     knex.destroy();
//   }
// }
// // getUsers5();

// async function getUsers6() {
//   try {
//     const data = await knex.raw(
//       CRUD("users").find({
//         name: { $like: "User 1%" },
//         $select: ["name", "age"],
//       })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   } finally {
//     knex.destroy();
//   }
// }
// // getUsers6();

// async function getUsers7() {
//   try {
//     const data = await knex.raw(
//       CRUD("users").find({
//         name: { $nlike: "User 1%" },
//         $skip: 1,
//         $limit: 3,
//         $sort: { age: "DESC" },
//       })
//     );
//     console.log(data);
//   } catch (error) {
//     console.error(error);
//   } finally {
//     knex.destroy();
//   }
// }
// getUsers7();
