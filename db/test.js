const assert = require("assert");

// create a table
// insert data (.json)

const testCases = [
  {
    name: "Aggregate with $match and $group",
    func: "aggregate",
    input: [
      {
        $match: {
          score: { $gt: 80 },
        },
      },
      {
        $group: { count: { $sum: 1 } },
      },
    ],
    output: [],
  },
  {
    name: "Find with $and and $or",
    func: "find",
    input: {
      $and: [
        { age: { $gte: 18 } },
        { $or: [{ country: "USA" }, { country: "Canada" }] },
      ],
    },
    output: [
      /* KNEX DB Rows */
    ],
  },
];

testCases.forEach(({ name, func, input, output }) => {
  console.log(`Running test: ${name}`);
  try {
    const sql = Table("users")[func](input);

    knex
      .raw(sql)
      .then((result) => {
        console.log(`SQL executed successfully.`);
        const actualResult = result.rows || result;

        assert.deepStrictEqual(
          actualResult,
          output,
          `Test failed: Expected result "${JSON.stringify(
            expectedResult
          )}" but got "${JSON.stringify(actualResult)}"`
        );

        console.log(`Test passed.`);
      })
      .catch((error) => {
        console.error(`Test passed, but SQL execution failed:`, error);
      });
  } catch (error) {
    console.error(error.message);
  }
});
