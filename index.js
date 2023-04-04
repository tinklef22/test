const { Client } = require("pg");

const connectionString =
  "postgresql://postgres:ZdR2jDNWyyvgbXTqF5Tk@containers-us-west-198.railway.app:5519/railway";

const client = new Client({
  connectionString: connectionString,
});

// Define the table using a JSON object
const tableDefinition = {
  tableName: "example_table",
  columns: [
    {
      name: "id",
      type: "SERIAL",
      primaryKey: true,
      unique: true,
    },
    {
      name: "name",
      type: "TEXT",
      nullable: false,
    },
    {
      name: "email",
      type: "TEXT",
      unique: true,
      nullable: false,
    },
    {
      name: "age",
      type: "INTEGER",
      nullable: true,
    },
  ],
};

// Function to create the table from the JSON object
function createTableFromJson(tableDefinition) {
  let sqlQuery = "CREATE TABLE " + tableDefinition.tableName + " (";

  for (let column of tableDefinition.columns) {
    sqlQuery += column.name + " " + column.type;

    if (column.primaryKey) {
      sqlQuery += " PRIMARY KEY";
    }

    if (column.unique) {
      sqlQuery += " UNIQUE";
    }

    if (!column.nullable) {
      sqlQuery += " NOT NULL";
    }

    sqlQuery += ",";
  }

  sqlQuery = sqlQuery.slice(0, -1);

  sqlQuery += ")";

  return sqlQuery;
}

// Call the function to create the table
// const createTableQuery = createTableFromJson(tableDefinition);
// client.connect();

// client.query(createTableQuery, (err, res) => {
//   if (err) {
//     console.error(err);
//     client.end();
//     return;
//   }
//   console.log("Table created successfully");
// });

// Function to insert data into the table
// async function insertData() {
//   try {
//     for (let i = 0; i < 15; i++) {
//       const name = "User " + i;
//       const email = "user" + i + "@example.com";
//       const age = Math.floor(Math.random() * 100);
//       const insertQuery = `INSERT INTO example_table (name, email, age) VALUES ('${name}', '${email}', ${age})`;
//       await client.query(insertQuery);
//       console.log(`Row ${i + 1} inserted successfully`);
//     }
//   } catch (err) {
//     console.error(err);
//   } finally {
//     client.end();
//   }
// }

// insertData();
