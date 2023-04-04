function convertMongoToSql(mongoQuery) {
  let sqlQuery = "";

  if (Object.keys(mongoQuery).length > 0) {
    let whereClause = convertWhereClause(mongoQuery);
    if (whereClause != "") {
      sqlQuery += " WHERE " + whereClause;
    }
  }

  if (mongoQuery.$sort) {
    let sortClause = convertSortClause(mongoQuery.$sort);
    sqlQuery += " ORDER BY " + sortClause;
  }

  if (mongoQuery.$limit) {
    let limitClause = convertLimitClause(mongoQuery.$limit);
    sqlQuery += " " + limitClause;
  }

  if (mongoQuery.$skip) {
    let skipClause = convertSkipClause(mongoQuery.$skip);
    sqlQuery += " " + skipClause;
  }

  return sqlQuery;
}

function convertWhereClause(mongoQuery) {
  let whereClause = "";

  if (mongoQuery.$and) {
    let andClauses = mongoQuery.$and.map(convertWhereClause).join(" AND ");
    whereClause += "(" + andClauses + ")";
  }

  if (mongoQuery.$or) {
    let orClauses = mongoQuery.$or.map(convertWhereClause).join(" OR ");
    whereClause += "(" + orClauses + ")";
  }

  for (let field in mongoQuery) {
    let value = mongoQuery[field];

    if (
      field === "$and" ||
      field === "$or" ||
      field === "$sort" ||
      field === "$skip" ||
      field === "$limit" ||
      field === "$select"
    ) {
      continue;
    }

    if (typeof value === "object") {
      let innerClause = "";

      if (Array.isArray(value)) {
        let sqlValue = convertValue(value);
        innerClause += field + " IN " + sqlValue;
      } else {
        for (let operator in value) {
          let sqlOperator = convertOperator(operator);
          let sqlValue = convertValue(value[operator]);

          if (operator === "$like") {
            innerClause += field + " LIKE " + sqlValue;
          } else if (operator === "$ilike") {
            innerClause += "LOWER(" + field + ") LIKE LOWER(" + sqlValue + ")";
          } else if (operator === "$nlike") {
            innerClause += field + " NOT LIKE " + sqlValue;
          } else {
            innerClause += field + " " + sqlOperator + " " + sqlValue;
          }

          innerClause += " AND ";
        }

        innerClause = innerClause.slice(0, -5);
      }

      if (whereClause) {
        whereClause += " AND ";
      }

      whereClause += "(" + innerClause + ")";
    } else {
      if (whereClause) {
        whereClause += " AND ";
      }

      let sqlValue = convertValue(value);
      whereClause += field + " = " + sqlValue;
    }
  }

  return whereClause;
}

function convertOperator(operator) {
  switch (operator) {
    case "$lt":
      return "<";
    case "$lte":
      return "<=";
    case "$gt":
      return ">";
    case "$gte":
      return ">=";
    case "$ne":
      return "<>";
    case "$in":
      return "IN";
    case "$nin":
      return "NOT IN";
    default:
      return operator;
  }
}

function convertValue(value) {
  if (Array.isArray(value)) {
    let sqlValues = value.map(convertValue).join(", ");
    return "(" + sqlValues + ")";
  } else if (value instanceof Date) {
    return "'" + value.toISOString() + "'";
  } else if (typeof value === "string") {
    return "'" + value.replace(/'/g, "''") + "'";
  } else {
    return value.toString();
  }
}

function convertSortClause(sortClauses) {
  let sortClause = "";

  for (let field in sortClauses) {
    let order = sortClauses[field].toLowerCase();
    sortClause += field + " " + order + ", ";
  }

  sortClause = sortClause.slice(0, -2);

  return sortClause;
}

function convertSkipClause(skipValue) {
  let skipClause = "OFFSET " + skipValue;
  return skipClause;
}

function convertLimitClause(limitValue) {
  let limitClause = "LIMIT " + limitValue;
  return limitClause;
}

const CRUD = (table) => {
  return {
    find: (mongoQuery) => {
      if (mongoQuery.$select) {
        let selectClause = mongoQuery.$select.join(", ");
        sqlQuery = "SELECT " + selectClause + " FROM " + table;
      } else {
        sqlQuery = "SELECT * FROM " + table;
      }

      let query = convertMongoToSql(mongoQuery);
      const sql = `${sqlQuery} ${query}`;
      return sql;
    },

    create: (data) => {
      if (Array.isArray(data) && data.length > 0) {
        const columns = Object.keys(data[0]).join(", ");
        const values = data
          .map(
            (item) =>
              `(${Object.values(item)
                .map((value) => `'${value}'`)
                .join(", ")})`
          )
          .join(", ");
        const sql = `INSERT INTO ${table} (${columns}) VALUES ${values}`;
        return sql;
      } else {
        const columns = Object.keys(data).join(", ");
        const values = Object.values(data)
          .map((value) => `'${value}'`)
          .join(", ");
        const sql = `INSERT INTO ${table} (${columns}) VALUES (${values})`;
        return sql;
      }
    },
    update: (data, mongoQuery) => {
      let query = convertMongoToSql(mongoQuery);
      const columns = Object.keys(data)
        .map((column) => `${column} = '${data[column]}'`)
        .join(", ");
      const sql = `UPDATE ${table} SET ${columns} ${query}`;
      return sql;
    },
    delete: (mongoQuery) => {
      let query = convertMongoToSql(mongoQuery);
      const sql = `DELETE FROM ${table} ${query}`;
      return sql;
    },

    patch: (data, mongoQuery) => {
      let query = convertMongoToSql(mongoQuery);
      const columns = Object.keys(data)
        .map((column) => `${column} = '${data[column]}'`)
        .join(", ");
      const sql = `UPDATE ${table} SET ${columns} ${query}`;
      return sql;
    },
  };
};

module.exports = CRUD;

// console.log(CRUD("users").find({ name: "test", $select: ["name", "age"] }));
// console.log(CRUD("users").create({ name: "test", age: 20 }));
// console.log(CRUD("users").update({ name: "test", age: 20 }, { name: "test" }));
// console.log(CRUD("users").delete({ name: "test" }));
// console.log(
//   CRUD("users").patch({ name: "test", age: 20 }, { name: "test to patch" })
// );
