const CRUD = require("./sql");
const knex = require("knex")({
  client: "pg",
  connection:
    "postgresql://postgres:ZdR2jDNWyyvgbXTqF5Tk@containers-us-west-198.railway.app:5519/railway",
});

// create

// knex
//   .raw(
//     CRUD("example_table").create({
//       name: "User 17",
//       email: "user17@example.com",
//       age: 30,
//     })
//   )
//   .then((result) => {
//     console.log(result.rowCount);
//   })
//   .catch((error) => {
//     console.error(error);
//   })
//   .finally(() => {
//     knex.destroy();
//   });

// create multiple

// knex
//   .raw(
//     CRUD("example_table").create([
//       { name: "User 20", email: "user20@example.com", age: 25 },
//       { name: "User 21", email: "user21@example.com", age: 35 },
//     ])
//   )
//   .then((result) => {
//     console.log({ rowCount: result.rowCount });
//   })
//   .catch((error) => {
//     console.error(error);
//   })
//   .finally(() => {
//     knex.destroy();
//   });

// update

// knex
//   .raw(
//     CRUD("example_table").update(
//       { name: "User 2", age: 20 },
//       { name: "User 2" }
//     )
//   )
//   .then((result) => {
//     console.log({method:result.command  ,rowCount: result.rowCount });
//   })
//   .catch((error) => {
//     console.error(error);
//   })
//   .finally(() => {
//     knex.destroy();
//   });

// delete

// knex
//   .raw(CRUD("example_table").delete({ id: 13 }))
//   .then((result) => {
//     console.log({ method: result.command, rowCount: result.rowCount });
//   })
//   .catch((error) => {
//     console.error(error);
//   })
//   .finally(() => {
//     knex.destroy();
//   });

knex
  .raw(CRUD("example_table").find({ name: "User 0", $select: ["name", "age"] }))
  .then((data) => {
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({ $skip: 1, $limit: 3, $sort: { age: "DESC" } })
  )
  .then((data) => {
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(CRUD("example_table").find({ age: { $gt: 50 } }))
  .then((data) => {
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({ age: { $lt: 30 }, $select: ["name", "age"] })
  )
  .then((data) => {
    console.log("lt:");
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({
      age: { $lte: 50 },
      $select: ["name", "age"],
      $limit: 3,
    })
  )
  .then((data) => {
    console.log("lte:");
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(CRUD("example_table").find({ age: { $in: [50] } }))
  .then((data) => {
    console.log("in:");
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({
      age: { $nin: [50, 80] },
      $skip: 1,
      $limit: 3,
      $select: ["name", "age"],
    })
  )
  .then((data) => {
    console.log("nin:");
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(CRUD("example_table").find({ age: { $ne: 20 } }))
  .then((data) => {
    console.log("ne:");
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({
      $or: [{ name: "User 2" }, { name: "User 3" }],
      $select: ["name", "age"],
    })
  )
  .then((data) => {
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({
      $and: [{ name: "User 2", age: 60 }],
      $select: ["name", "age"],
    })
  )
  .then((data) => {
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

knex
  .raw(
    CRUD("example_table").find({
      $and: [{ name: "User 2", age: 30 }],
      $select: ["name", "age"],
    })
  )
  .then((data) => {
    console.log(data.rows);
  })
  .catch((err) => {
    console.log(err);
  })
  .finally(() => {
    knex.destroy();
  });

async function fetchData() {
  try {
    const data = await knex.raw(
      CRUD("example_table").find({ name: { $like: "User 1%" } })
    );
    console.log("like:");
    console.log(data.rows);
  } catch (error) {
    console.log(error);
  } finally {
    knex.destroy();
  }
}

fetchData();

async function fetchData2() {
  try {
    const data = await knex.raw(
      CRUD("example_table").find({ name: { $ilike: "User 1%" } })
    );
    console.log("ilike:");
    console.log(data.rows);
  } catch (error) {
    console.log(error);
  } finally {
    knex.destroy();
  }
}

fetchData2();

async function fetchData3() {
  try {
    const data = await knex.raw(
      CRUD("example_table").find({ name: { $nlike: "User 1%" } })
    );
    console.log("nlike:");
    console.log(data.rows);
  } catch (error) {
    console.log(error);
  } finally {
    knex.destroy();
  }
}

fetchData3();
