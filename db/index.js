const convertOperator = (operator) => {
  const operators = {
    $lt: "<",
    $lte: "<=",
    $gt: ">",
    $gte: ">=",
    $ne: "<>",
    $in: "IN",
    $nin: "NOT IN",
  };
  return operators[operator] || operator;
};

const escapeString = (str) => {
  const replacements = {
    "'": "''",
    "\\": "\\\\",
    "\0": "\\0",
    "\n": "\\n",
    "\r": "\\r",
    "\x1a": "\\Z",
  };

  return str.replace(/[\0\n\r\x1a'\\]/g, (char) => replacements[char]);
};

const convertValue = (value) => {
  if (Array.isArray(value)) {
    const sqlValues = value.map(convertValue).join(", ");
    return `(${sqlValues})`;
  } else if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  } else if (typeof value === "string") {
    return `'${escapeString(value)}'`;
  } else if (value === null || value === undefined) {
    return "NULL";
  } else {
    return value.toString();
  }
};

const likeMapping = (field, sqlValue, sqlOperator) =>
  ({
    $like: `${field} LIKE ${sqlValue}`,
    $ilike: `LOWER(${field}) LIKE LOWER(${sqlValue})`,
    $nlike: `${field} NOT LIKE ${sqlValue}`,
    $regex: `${field} REGEXP ${sqlValue}`,
  }[sqlOperator] || `${field} ${sqlOperator} ${sqlValue}`);

const whereClause = (mongoQuery) => {
  const andClause = mongoQuery.$and
    ? `(${mongoQuery.$and.map(whereClause).join(" AND ")})`
    : null;
  const orClause = mongoQuery.$or
    ? `(${mongoQuery.$or.map(whereClause).join(" OR ")})`
    : null;

  const fieldClauses = Object.entries(mongoQuery)
    .filter(
      ([field]) =>
        !["$and", "$or", "$sort", "$skip", "$limit", "$select"].includes(field)
    )
    .map(([field, value]) => {
      const innerClause =
        typeof value === "object" && !Array.isArray(value)
          ? handleOperators(field, value)
          : `${field} = ${convertValue(value)}`;
      return `(${innerClause})`;
    });

  const allClauses = [andClause, orClause, ...fieldClauses].filter(Boolean);
  return allClauses.length ? allClauses.join(" AND ") : "1=1";
};

const handleOperators = (field, value) =>
  Object.entries(value)
    .reduce((innerClause, [operator, val]) => {
      const sqlOperator = convertOperator(operator);
      const sqlValue = convertValue(val);
      const sqlMapping = likeMapping(field, sqlValue, sqlOperator);
      return `${innerClause} ${sqlMapping} AND `;
    }, "")
    .slice(0, -5);

const sortClause = (sortClauses) =>
  Object.entries(sortClauses)
    .reduce((sortClause, [field, order]) => {
      const sortOrder =
        typeof order === "string"
          ? order.toLowerCase()
          : order === 1
          ? "asc"
          : "desc";
      return `${sortClause}${field} ${sortOrder}, `;
    }, "ORDER BY ")
    .slice(0, -2);

const skipClause = (skipValue) => `OFFSET ${skipValue}`;

const limitClause = (limitValue) => `LIMIT ${limitValue}`;

const convertMongoToSql = (mongoQuery) => {
  if (!mongoQuery) return "";

  const where = whereClause(mongoQuery);
  const sort = mongoQuery.$sort ? sortClause(mongoQuery.$sort) : "";
  const limit = mongoQuery.$limit ? limitClause(mongoQuery.$limit) : "";
  const skip = mongoQuery.$skip ? skipClause(mongoQuery.$skip) : "";

  return `WHERE ${where} ${sort} ${limit} ${skip}`.trim().replace(/\s+/g, " ");
};

const condToSql = (cond) => {
  const [condition, trueValue, falseValue] = cond;
  const sqlCondition = whereClause(condition);
  const sqlTrueValue = convertValue(trueValue);
  const sqlFalseValue = convertValue(falseValue);

  return `CASE WHEN ${sqlCondition} THEN ${sqlTrueValue} ELSE ${sqlFalseValue} END`;
};

const ifNullToSql = ([field, defaultValue]) => {
  const sqlField = field.slice(1); // Remove the $ from the field name
  const sqlDefaultValue = convertValue(defaultValue);

  return `COALESCE(${sqlField}, ${sqlDefaultValue})`;
};

const switchToSql = (switchExpression) => {
  const { branches, default: defaultValue } = switchExpression;
  const cases = branches
    .map(({ case: condition, then: value }) => {
      const sqlCondition = whereClause(condition);
      const sqlValue = convertValue(value);

      return `WHEN ${sqlCondition} THEN ${sqlValue}`;
    })
    .join(" ");

  const sqlDefaultValue = convertValue(defaultValue);

  return `CASE ${cases} ELSE ${sqlDefaultValue} END`;
};

const expressionToSql = (expression) => {
  const operator = Object.keys(expression)[0];
  const value = expression[operator];

  switch (operator) {
    case "$cond":
      return condToSql(value);
    case "$ifNull":
      return ifNullToSql(value);
    case "$switch":
      return switchToSql(value);
    case "$eq":
    case "$ne":
    case "$gt":
    case "$gte":
    case "$lt":
    case "$lte":
    case "$and":
    case "$or":
      return whereClause({ [operator]: value });
    default:
      return "";
  }
};

const pipelineToSql = (pipeline, tableName) => {
  let sql = `SELECT * FROM ${tableName}`;
  let groupBy = "";

  pipeline.forEach((stage) => {
    const stageName = Object.keys(stage)[0];
    const stageValue = stage[stageName];

    switch (stageName) {
      case "$match":
        sql += ` WHERE ${whereClause(stageValue)}`;
        break;
      case "$group":
        const groupFields = [];
        const aggFields = [];

        Object.entries(stageValue).forEach(([field, value]) => {
          if (field === "_id") {
            if (typeof value === "object") {
              groupFields.push(expressionToSql(value));
            } else {
              groupFields.push(value.slice(1)); // Remove the $ from the field name
            }
          } else {
            const operator = Object.keys(value)[0];
            const sourceField =
              typeof value[operator] === "string"
                ? value[operator].slice(1)
                : value[operator]; // Remove the $ from the field name if it's a string
            const sqlOperator = operator.slice(1); // Remove the $ from the operator

            aggFields.push(
              `${sqlOperator.toUpperCase()}(${sourceField}) AS ${field}`
            );
          }
        });
        groupBy =
          groupFields.length > 0 ? ` GROUP BY ${groupFields.join(", ")}` : "";
        sql = `SELECT ${[...groupFields, ...aggFields].join(
          ", "
        )} FROM (${sql}) AS subquery`;
        break;
      case "$project":
        const projectionFields = Object.entries(stageValue)
          .filter(([, included]) => included)
          .map(([field]) => field)
          .join(", ");

        sql = `SELECT ${projectionFields} FROM (${sql}) AS subquery`;
        break;
      case "$sort":
        sql += ` ${sortClause(stageValue)}`;
        break;
      case "$skip":
        sql += ` ${skipClause(stageValue)}`;
        break;
      case "$limit":
        sql += ` ${limitClause(stageValue)}`;
        break;
      case "$count":
        groupBy = ""; // Clear the groupBy clause
        sql = `SELECT COUNT(*) AS ${stageValue} FROM (${sql}) AS subquery`;
        break;
    }
  });
  console.log(sql + groupBy);
  return sql + groupBy;
};

const selectStatement = (table, mongoQuery) =>
  `SELECT ${
    mongoQuery.$select ? mongoQuery.$select.join(", ") : "*"
  } FROM ${table}`;

const valuesStatement = (data) => {
  if (Array.isArray(data)) {
    const columns = Object.keys(data[0]).join(", ");
    const values = Array.from(
      data,
      (item) =>
        `(${Object.values(item)
          .map((value) => `'${value}'`)
          .join(", ")})`
    ).join(", ");
    return `(${columns}) VALUES ${values}`;
  } else {
    const columns = Object.keys(data).join(", ");
    const values = Object.values(data)
      .map((value) => `'${value}'`)
      .join(", ");
    return `(${columns}) VALUES (${values})`;
  }
};

const setStatement = (data) =>
  Object.entries(data)
    .map(([column, value]) => `${column} = '${value}'`)
    .join(", ");

const fromJSON = (table, definition, target) => {
  const columnTypes = {
    string: {
      mysql: "VARCHAR(255)",
      postgres: "TEXT",
      mssql: "NVARCHAR(255)",
      oracle: "VARCHAR2(255)",
      default: "TEXT",
    },
    number: {
      mysql: "INT",
      postgres: "INTEGER",
      mssql: "INT",
      oracle: "NUMBER",
      default: "INTEGER",
    },
    boolean: {
      mysql: "BOOLEAN",
      postgres: "BOOLEAN",
      mssql: "BIT",
      oracle: "NUMBER(1)",
      default: "BOOLEAN",
    },
    date: {
      mysql: "DATETIME",
      postgres: "TIMESTAMP",
      mssql: "DATETIME2",
      oracle: "DATE",
      default: "TIMESTAMP",
    },
    default: {
      mysql: "TEXT",
      postgres: "TEXT",
      mssql: "TEXT",
      oracle: "VARCHAR2(4000)",
      default: "TEXT",
    },
  };

  let sqlQuery = `CREATE TABLE ${table} (`;

  for (let column of definition.columns) {
    sqlQuery += `${column.name} `;

    const type = columnTypes[column.type] || columnTypes.default;
    const columnType =
      column.native?.toUpperCase() || type[target] || type.default;

    sqlQuery += columnType;

    if (column.primaryKey) {
      sqlQuery += " PRIMARY KEY";
    }

    if (column.unique) {
      sqlQuery += " UNIQUE";
    }

    if (!column.nullable) {
      sqlQuery += " NOT NULL";
    }

    if (column.autoIncrement) {
      const autoIncrement = {
        mysql: " AUTO_INCREMENT",
        postgres: " GENERATED ALWAYS AS IDENTITY",
        mssql: " IDENTITY(1,1)",
        oracle: " GENERATED ALWAYS AS IDENTITY",
        default: " AUTO_INCREMENT",
      };
      const autoIncrementType = autoIncrement[target] || autoIncrement.default;
      sqlQuery += autoIncrementType;
    }

    sqlQuery += ",";
  }

  // Remove the last comma from the statement
  sqlQuery = sqlQuery.slice(0, -1);

  sqlQuery += ")";

  return sqlQuery;
};

const Table = (table) => {
  const dbType = "postgres";
  return {
    jsonTable: fromJSON,
    alter: {
      add: (columns) => {
        const columnsSql = columns
          .map(({ name, type }) => `ADD ${name} ${type}`)
          .join(", ");
        let sql = `ALTER TABLE ${table} ${columnsSql}`;
        return sql;
      },
      drop: (columnNames) => {
        const columnsSql = columnNames
          .map((name) => `DROP COLUMN ${name}`)
          .join(", ");
        let sql = `ALTER TABLE ${table} ${columnsSql}`;
        return sql;
      },
      rename: (columnMappings) => {
        const columnsSql = columnMappings
          .map(
            ({ oldName, newName }) => `RENAME COLUMN ${oldName} TO ${newName}`
          )
          .join(", ");
        let sql = `ALTER TABLE ${table} ${columnsSql}`;
        if (dbType === "mssql") {
          sql = `EXEC sp_rename '${table}.${oldName}', '${newName}', 'COLUMN'`;
        }
        return sql;
      },
      modify: (columnModifications) => {
        let sql = "";
        if (dbType === "postgres" || dbType === "mssql") {
          const columnsSqlPostgres = columnModifications
            .map(
              ({ name, type }) =>
                `ALTER TABLE ${table} ALTER COLUMN ${name} TYPE ${type}`
            )
            .join("; ");
          sql = `${columnsSqlPostgres};`;
        } else {
          const columnsSql = columnModifications
            .map(
              ({ name, type }) => `ALTER TABLE ${table} MODIFY ${name} ${type}`
            )
            .join("; ");
          sql = `${columnsSql};`;
        }
        return sql;
      },
    },
    aggregate: (mongoQuery) => {
      if (!mongoQuery) return "";
      return pipelineToSql(mongoQuery, table);
    },
    find: (mongoQuery = {}) => {
      const sqlQuery = selectStatement(table, mongoQuery);
      const query = convertMongoToSql(mongoQuery);
      return `${sqlQuery} ${query}`;
    },
    create: (data) => {
      const values = valuesStatement(data);
      return `INSERT INTO ${table} ${values}`;
    },
    update: (data, mongoQuery) => {
      const query = convertMongoToSql(mongoQuery);
      const columns = setStatement(data);
      if (!columns) return;
      return `UPDATE ${table} SET ${columns} ${query}`;
    },
    delete: (mongoQuery) => {
      const query = convertMongoToSql(mongoQuery);
      return `DELETE FROM ${table} ${query}`;
    },
    patch: (data, mongoQuery) => update(data, mongoQuery),
    findById: (id) => {
      return `SELECT * FROM ${table} WHERE id = '${id}'`;
    },
    updateById: (id, data) => {
      const columns = setStatement(data);
      return `UPDATE ${table} SET ${columns} WHERE id = '${id}'`;
    },
    deleteById: (id) => {
      return `DELETE FROM ${table} WHERE id = '${id}'`;
    },
    patchById: (id, data) => updateById(id, data),
  };
};

module.exports = Table;
