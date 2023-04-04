function createTableFromJson(tableDefinition) {
  let sqlQuery = "DROP TABLE IF EXISTS " + tableDefinition.tableName + ";";
  sqlQuery += "CREATE TABLE " + tableDefinition.tableName + " (";

  for (let column of tableDefinition.columns) {
    sqlQuery += column.name + " " + column.type;

    if (column.primaryKey) {
      sqlQuery += " PRIMARY KEY";
    }

    if (column.unique) {
      sqlQuery += " UNIQUE";
    }

    if (column.autoIncrement) {
      sqlQuery += " AUTOINCREMENT";
    }

    if (!column.nullable) {
      sqlQuery += " NOT NULL";
    }

    sqlQuery += ",";
  }

  // Remove the last comma from the statement
  sqlQuery = sqlQuery.slice(0, -1);

  sqlQuery += ")";

  return sqlQuery;
}

module.exports = createTableFromJson;
