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
            if (value !== null) {
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

const Table = (table) => {
  return {
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
