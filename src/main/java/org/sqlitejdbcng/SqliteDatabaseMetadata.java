/*
 * Copyright (c) 2013, Timothy Stack
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sqlitejdbcng;

import org.sqlitejdbcng.bridj.Sqlite3;
import org.sqlitejdbcng.internal.ColumnData;
import org.sqlitejdbcng.internal.SQLKeywords;
import org.sqlitejdbcng.internal.SQLTemplate;

import java.sql.*;
import java.util.*;

public class SqliteDatabaseMetadata implements DatabaseMetaData {
    private static final String KEYWORD_LIST;
    private static final String GET_PROCEDURES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-procedures.sql");
    private static final String GET_PROCEDURE_COLUMNS_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-procedure-columns.sql");
    private static final String GET_INDEX_INFO_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-index-info.sql");
    private static final String GET_TABLES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-tables.sql");
    private static final String GET_COLUMNS_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-columns.sql");
    private static final String GET_COLUMN_PRIVILEGES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-column-privileges.sql");
    private static final String GET_TABLE_PRIVILEGES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-table-privileges.sql");
    private static final String GET_PRIMARY_KEY_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-primary-key.sql");
    private static final String GET_FOREIGN_KEY_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-foreign-key.sql");
    private static final String GET_UDTS_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-udts.sql");
    private static final String GET_ATTRIBUTES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-attributes.sql");
    private static final String GET_CLIENT_INFO_PROPERTIES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-client-info-properties.sql");
    private static final String GET_SUPER_TYPES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-super-types.sql");
    private static final String GET_SUPER_TABLES_TEMPLATE =
            SQLTemplate.readTemplate("/metadata-get-super-tables.sql");
    private static final String GET_FUNCTION_COLUMNS_TEMPALTE =
            SQLTemplate.readTemplate("/metadata-get-function-columns.sql");
    private static SqliteConnection METADATA_DATABASE_CONNECTION;

    static {
        SQLKeywords keywords = new SQLKeywords();
        List<String> sqliteList =
                new ArrayList<String>(Arrays.asList(keywords.getSqliteKeywords()));

        sqliteList.removeAll(Arrays.asList(keywords.getSqlKeywords()));

        KEYWORD_LIST = Sqlite3.join(sqliteList.toArray(), ",");
    }

    private static synchronized SqliteConnection getMetadataDatabaseConnection() {
        if (METADATA_DATABASE_CONNECTION == null) {
            try {
                METADATA_DATABASE_CONNECTION = new SqliteConnection("jdbc:sqlite::memory:",
                        new Properties());
                Statement stmt = null;
                try {
                    int maxLength = Sqlite3.sqlite3_limit(
                            METADATA_DATABASE_CONNECTION.getHandle(),
                            Sqlite3.Limit.SQLITE_LIMIT_LENGTH.value(), -1);
                    String[][] FUNCTION_STATEMENTS = {
                            SQLTemplate.readTemplateArray("/metadata-types.sql", maxLength),
                            SQLTemplate.readTemplateArray("/metadata-functions.sql"),
                    };
                    stmt = METADATA_DATABASE_CONNECTION.createStatement();
                    for (String[] stmtStrings : FUNCTION_STATEMENTS) {
                        for (String stmtString : stmtStrings) {
                            try {
                                stmt.execute(stmtString);
                            }
                            catch (SQLException e) {
                                throw new RuntimeException(String.format(
                                        "Static statement failed: %s", stmtString), e);
                            }
                        }
                    }
                }
                finally {
                    SqliteCommon.closeQuietly(stmt);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException("Unable to open metadata database", e);
            }
        }
        return METADATA_DATABASE_CONNECTION;
    }

    private final SqliteConnection conn;

    public SqliteDatabaseMetadata(SqliteConnection conn) {
        this.conn = conn;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return this.conn.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.conn.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "SQLite";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return Sqlite3.sqlite3_libversion().getCString();
    }

    @Override
    public String getDriverName() throws SQLException {
        return SqliteDriver.class.getPackage().getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "" + SqliteDriver.VERSION[0] + "." + SqliteDriver.VERSION[1];
    }

    @Override
    public int getDriverMajorVersion() {
        return SqliteDriver.VERSION[0];
    }

    @Override
    public int getDriverMinorVersion() {
        return SqliteDriver.VERSION[1];
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return KEYWORD_LIST;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsConvert(int i, int i2) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_LENGTH.value(), -1);
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_LENGTH.value(), -1);
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_COLUMN.value(), -1);
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_COLUMN.value(), -1);
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_COLUMN.value(), -1);
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_COLUMN.value(), -1);
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_COLUMN.value(), -1);
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_LENGTH.value(), -1);
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return Sqlite3.sqlite3_limit(this.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_SQL_LENGTH.value(), -1);
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private ResultSet executeConstantQuery(String constantQuery) throws SQLException {
        Statement stmt = this.conn.createStatement();

        try {
            ((SqliteStatement)stmt).closeOnCompletion();
            return stmt.executeQuery(constantQuery);
        }
        catch (SQLException e) {
            stmt.close();

            throw e;
        }
    }

    @Override
    public ResultSet getProcedures(String s, String s2, String s3) throws SQLException {
        return this.executeConstantQuery(GET_PROCEDURES_TEMPLATE);
    }

    @Override
    public ResultSet getProcedureColumns(String s, String s2, String s3, String s4) throws SQLException {
        return this.executeConstantQuery(GET_PROCEDURE_COLUMNS_TEMPLATE);
    }

    private static final String[] DEFAULT_TABLE_TYPES = { "TABLE", "VIEW" };

    @Override
    public ResultSet getTables(String catalog,
                               String schemaPattern,
                               String tableNamePattern,
                               String[] types) throws SQLException {
        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            throw new SQLFeatureNotSupportedException("SQLite does not support schemas", "0A000");
        }

        if (catalog == null || catalog.isEmpty()) {
            catalog = "main";
        }

        if (types == null) {
            types = DEFAULT_TABLE_TYPES;
        }

        String sql = Sqlite3.mprintf(GET_TABLES_TEMPLATE,
                catalog,
                Sqlite3.join(Collections.nCopies(types.length, "?").toArray(), ", "));

        PreparedStatement ps = this.conn.prepareStatement(sql);

        ((SqlitePreparedStatement)ps).closeOnCompletion();
        try {
            ps.setString(1, catalog);

            if (tableNamePattern == null)
                tableNamePattern = "%";
            ps.setString(2, tableNamePattern);
            for (int lpc = 0; lpc < types.length; lpc++) {
                ps.setString(3 + lpc, types[lpc]);
            }

            return ps.executeQuery();
        }
        catch (SQLException e) {
            ps.close();

            throw e;
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return this.executeConstantQuery(
                "SELECT null as TABLE_SCHEM, null as TABLE_CATALOG LIMIT 0");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = this.conn.createStatement();
            List<String> dbNames = new ArrayList<String>();

            try {
                rs = stmt.executeQuery("PRAGMA database_list");
                while (rs.next()) {
                    dbNames.add(rs.getString(2));
                }
            } finally {
                SqliteCommon.closeQuietly(rs);
            }

            String query = Sqlite3.join(
                    Collections.nCopies(dbNames.size(), "SELECT ? as TABLE_CAT").toArray(),
                    " UNION ALL ");

            PreparedStatement preparedStatement = this.conn.prepareStatement(query);

            ((SqlitePreparedStatement)preparedStatement).closeOnCompletion();
            try {
                for (int lpc = 0; lpc < dbNames.size(); lpc++) {
                    preparedStatement.setString(lpc + 1, dbNames.get(lpc));
                }

                return preparedStatement.executeQuery();
            }
            catch (SQLException e) {
                preparedStatement.close();

                throw e;
            }
        } finally {
            SqliteCommon.closeQuietly(stmt);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return this.executeConstantQuery(
                "SELECT 'TABLE' as TABLE_TYPE UNION ALL " +
                        "SELECT 'VIEW' as TABLE_TYPE");
    }

    @Override
    public ResultSet getColumns(String catalog,
                                String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        List<String> tableList = new ArrayList<String>();
        String query;

        /* XXX We should iterate over the catalogs instead of just defaulting to "main" */
        if (catalog == null || catalog.isEmpty())
            catalog = "main";

        if (tableNamePattern == null)
            tableNamePattern = "%";
        if (columnNamePattern == null)
            columnNamePattern = "%";

        query = Sqlite3.mprintf("SELECT tbl_name FROM %Q.sqlite_master WHERE type='table' AND tbl_name LIKE ?",
                catalog);
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = this.conn.prepareStatement(query);
            ps.setString(1, tableNamePattern);
            try {
                rs = ps.executeQuery();
                while (rs.next()) {
                    tableList.add(rs.getString(1));
                }
            } finally {
                SqliteCommon.closeQuietly(rs);
            }
        } finally {
            SqliteCommon.closeQuietly(ps);
        }

        List<ColumnData> columnList = new ArrayList<ColumnData>();

        columnNamePattern = columnNamePattern.replaceAll("%", ".*");

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            for (String tableName : tableList) {

                query = Sqlite3.mprintf("PRAGMA %Q.table_info(%Q)", catalog, tableName);

                try {
                    rs = stmt.executeQuery(query);
                    while (rs.next()) {
                        ColumnData cd = new ColumnData(this.conn.getHandle(), catalog, tableName, rs);

                        if (!cd.name.matches(columnNamePattern))
                            continue;
                        columnList.add(cd);
                    }
                } finally {
                    SqliteCommon.closeQuietly(rs);
                }
            }
        } finally {
            SqliteCommon.closeQuietly(stmt);
        }

        String constantQuery = "", limit = "";

        for (int lpc = 0; lpc < columnList.size(); lpc++) {
            if (!constantQuery.isEmpty())
                constantQuery += " UNION ALL ";
            constantQuery += GET_COLUMNS_TEMPLATE;
        }
        if (constantQuery.isEmpty()) {
            constantQuery = GET_COLUMNS_TEMPLATE;
            limit = " LIMIT 0";
        }
        constantQuery += " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
        constantQuery += limit;

        ps = this.conn.prepareStatement(constantQuery);

        ((SqlitePreparedStatement)ps).closeOnCompletion();

        int index = 1;

        for (ColumnData column : columnList) {
            ps.setString(index++, catalog);
            ps.setString(index++, column.tableName);
            ps.setString(index++, column.name);
            ps.setInt(index++, column.sqlType);
            ps.setString(index++, column.type);
            ps.setInt(index++, 0);
            ps.setInt(index++, 0);
            ps.setInt(index++, column.notNull);
            ps.setString(index++, column.defaultValue);
            ps.setInt(index++, column.index);
            ps.setString(index++, column.notNull == columnNoNulls ? "NO" : "YES");
            ps.setInt(index++, 0);
            ps.setInt(index++, 0);
        }

        return ps.executeQuery();
    }

    @Override
    public ResultSet getColumnPrivileges(String s, String s2, String s3, String s4) throws SQLException {
        return this.executeConstantQuery(GET_COLUMN_PRIVILEGES_TEMPLATE);
    }

    @Override
    public ResultSet getTablePrivileges(String s, String s2, String s3) throws SQLException {
        return this.executeConstantQuery(GET_TABLE_PRIVILEGES_TEMPLATE);
    }

    @Override
    public ResultSet getBestRowIdentifier(String s, String s2, String s3, int i, boolean b) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getVersionColumns(String s, String s2, String s3) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
        List<ColumnData> columnList = new ArrayList<ColumnData>();
        String query, limit = "";
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = this.conn.createStatement();
            if (catalog != null && !catalog.isEmpty())
                query = Sqlite3.mprintf("PRAGMA %Q.table_info(%Q)", catalog, tableName);
            else
                query = Sqlite3.mprintf("PRAGMA table_info(%Q)", tableName);

            try {
                rs = stmt.executeQuery(query);
                while (rs.next()) {
                    ColumnData cd = new ColumnData(this.conn.getHandle(), catalog, tableName, rs);

                    if (cd.primaryKey == 0)
                        continue;
                    columnList.add(cd);
                }
            } finally {
                SqliteCommon.closeQuietly(rs);
            }
        } finally {
            SqliteCommon.closeQuietly(stmt);
        }

        String constantQuery = "";

        for (int lpc = 0; lpc < columnList.size(); lpc++) {
            if (!constantQuery.isEmpty())
                constantQuery += " UNION ALL ";
            constantQuery += GET_PRIMARY_KEY_TEMPLATE;
        }
        if (constantQuery.isEmpty()) {
            constantQuery = GET_PRIMARY_KEY_TEMPLATE;
            limit = " LIMIT 0";
        }
        constantQuery += " ORDER BY COLUMN_NAME";
        constantQuery += limit;

        PreparedStatement ps = this.conn.prepareStatement(constantQuery);

        ((SqlitePreparedStatement)ps).closeOnCompletion();

        int index = 1;

        for (ColumnData column : columnList) {
            ps.setString(index++, catalog);
            ps.setString(index++, tableName);
            ps.setString(index++, column.name);
            ps.setInt(index++, column.primaryKey);
        }

        return ps.executeQuery();
    }

    public static class ForeignKeyData {
        private static final Map<String, Integer> ACTION_MAP = new HashMap<String, Integer>();

        static {
            ACTION_MAP.put("SET NULL", DatabaseMetaData.importedKeySetNull);
            ACTION_MAP.put("SET DEFAULT", DatabaseMetaData.importedKeySetDefault);
            ACTION_MAP.put("CASCADE", DatabaseMetaData.importedKeyCascade);
            ACTION_MAP.put("RESTRICT", DatabaseMetaData.importedKeyRestrict);
            ACTION_MAP.put("NO ACTION", DatabaseMetaData.importedKeyNoAction);
        }

        private static final int actionStringToInt(String actionStr) {
            Integer actionInt = ACTION_MAP.get(actionStr);

            if (actionInt == null)
                throw new RuntimeException("Unknown sqlite action string " + actionStr);

            return actionInt;
        }

        public final int id;
        public final int seq;
        public final String fromTable;
        public final String fromColumn;
        public final String toTable;
        public final String toColumn;
        public final int onUpdate;
        public final int onDelete;
        public final String match;

        public ForeignKeyData(String fromTable, ResultSet rs) throws SQLException {
            this.fromTable = fromTable;
            this.id = rs.getInt("id");
            this.seq = rs.getInt("seq");
            this.toTable = rs.getString("table");
            this.fromColumn = rs.getString("from");
            this.toColumn = rs.getString("to");
            this.onUpdate = actionStringToInt(rs.getString("on_update"));
            this.onDelete = actionStringToInt(rs.getString("on_delete"));
            this.match = rs.getString("match");
        }

        public ForeignKeyData(String fromTable, String toTable) {
            this.id = -1;
            this.seq = 0;
            this.fromTable = fromTable;
            this.fromColumn = null;
            this.toTable = toTable;
            this.toColumn = null;
            this.onUpdate = -1;
            this.onDelete = -1;
            this.match = null;
        }
    }

    private Map<String, List<ForeignKeyData>> getForeignKeyData(String catalog) throws SQLException {
        Map<String, List<ForeignKeyData>> table2Key = new HashMap<String, List<ForeignKeyData>>();
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = this.conn.createStatement();
            List<String> allTables = new ArrayList<String>();
            String tableQuery;

            /* XXX We need to iterate over all of the catalogs */
            if (catalog != null && !catalog.isEmpty()) {
                tableQuery = Sqlite3.mprintf(
                    "SELECT name FROM %Q.sqlite_master WHERE type='table'", catalog);
            }
            else {
                tableQuery = "SELECT name FROM sqlite_master WHERE type='table'";
            }

            try {
                rs = stmt.executeQuery(tableQuery);
                while (rs.next()) {
                    allTables.add(rs.getString(1));
                }
            } finally {
                SqliteCommon.closeQuietly(rs);
            }

            for (String catalogTable : allTables) {
                if (!stmt.execute(Sqlite3.mprintf("PRAGMA %Q.foreign_key_list(%Q)",
                        catalog, catalogTable))) {
                    continue;
                }

                try {
                    rs = stmt.getResultSet();
                    while (rs.next()) {
                        ForeignKeyData fkd = new ForeignKeyData(catalogTable, rs);

                        if (!table2Key.containsKey(fkd.fromTable))
                            table2Key.put(fkd.fromTable, new ArrayList<ForeignKeyData>());
                        if (!table2Key.containsKey(fkd.toTable))
                            table2Key.put(fkd.toTable, new ArrayList<ForeignKeyData>());

                        table2Key.get(fkd.fromTable).add(fkd);
                        table2Key.get(fkd.toTable).add(fkd);
                    }
                } finally {
                    SqliteCommon.closeQuietly(rs);
                }
            }
        } finally {
            SqliteCommon.closeQuietly(stmt);
        }

        return table2Key;
    }

    private ResultSet getForeignKeys(String catalog, String fromTable, String toTable) throws SQLException {
        Map<String, List<ForeignKeyData>> table2Key = getForeignKeyData(catalog);
        List<ForeignKeyData> columnList = new ArrayList<ForeignKeyData>();
        String limit = "";

        if (table2Key.containsKey(fromTable)) {
            columnList.addAll(table2Key.get(fromTable));
        }
        if (table2Key.containsKey(toTable)) {
            columnList.addAll(table2Key.get(toTable));
        }

        String constantQuery = "";

        for (ForeignKeyData fkd : columnList) {
            if (fromTable != null && !fromTable.equals(fkd.fromTable))
                continue;
            if (toTable != null && !toTable.equals(fkd.toTable))
                continue;

            if (!constantQuery.isEmpty())
                constantQuery += " UNION ALL ";
            constantQuery += GET_FOREIGN_KEY_TEMPLATE;
        }
        if (constantQuery.isEmpty()) {
            constantQuery = GET_FOREIGN_KEY_TEMPLATE;
            limit = " LIMIT 0";
        }
        constantQuery += " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";
        constantQuery += limit;

        PreparedStatement ps = this.conn.prepareStatement(constantQuery);

        try {
            ((SqlitePreparedStatement)ps).closeOnCompletion();

            int index = 1;

            for (ForeignKeyData fkd : columnList) {
                if (fromTable != null && !fromTable.equals(fkd.fromTable))
                    continue;
                if (toTable != null && !toTable.equals(fkd.toTable))
                    continue;

                ps.setString(index++, catalog);
                ps.setString(index++, fkd.toTable);
                ps.setString(index++, fkd.toColumn);
                ps.setString(index++, catalog);
                ps.setString(index++, fkd.fromTable);
                ps.setString(index++, fkd.fromColumn);
                ps.setInt(index++, fkd.seq + 1);
                ps.setInt(index++, fkd.onUpdate);
                ps.setInt(index++, fkd.onDelete);
                ps.setInt(index++, importedKeyInitiallyImmediate); // XXX
            }

            return ps.executeQuery();
        }
        catch (SQLException e) {
            SqliteCommon.closeQuietly(ps);
            throw e;
        }
    }

    /**
     * The DEFERRABILITY is always set to the value of 'importedKeyInitiallyImmediate'
     * since there is no way to tell at run time whether it is set to immediate or
     * deferred.
     *
     * {@inheritDoc}
     */
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return this.getForeignKeys(catalog, table, null);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return this.getForeignKeys(catalog, null, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog,
                                       String parentSchema,
                                       String parentTable,
                                       String foreignCatalog,
                                       String foreignSchema,
                                       String foreignTable) throws SQLException {
        if (!SqliteCommon.stringEquals(parentCatalog, foreignCatalog)) {
            throw new SQLNonTransientException("Catalog names must be the same", "42000");
        }

        return this.getForeignKeys(parentCatalog, foreignTable, parentTable);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        SqliteConnection connection = getMetadataDatabaseConnection();
        Statement stmt = connection.createStatement();
        ((SqliteStatement) stmt).closeOnCompletion();
        try {
            return stmt.executeQuery("SELECT * FROM metadata_types ORDER BY DATA_TYPE");
        }
        catch (SQLException e) {
            SqliteCommon.closeQuietly(stmt);
            throw e;
        }
    }

    private static class IndexInfo {
        String name;
        boolean unique;
        List<String> columnNames = new ArrayList<String>();

        public IndexInfo(String name, boolean unique) {
            this.name = name;
            this.unique = unique;
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String tableName,
            boolean unique, boolean approximate) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;

        if (schema != null && !schema.isEmpty()) {
            throw new SQLFeatureNotSupportedException("SQLite does not support schemas", "0A000");
        }

        if (catalog == null || catalog.isEmpty()) {
            catalog = "main";
        }

        try {
            List<IndexInfo> indexInfoList = new ArrayList<IndexInfo>();
            stmt = this.conn.createStatement();

            try {
                int rows = 0;
                rs = stmt.executeQuery(Sqlite3.mprintf("PRAGMA %Q.index_list(%Q)",
                        catalog, tableName));
                while (rs.next()) {
                    if (unique && !rs.getBoolean(3)) {
                        continue;
                    }
                    indexInfoList.add(new IndexInfo(rs.getString(2), rs.getBoolean(3)));
                }

                for (IndexInfo indexInfo : indexInfoList) {
                    rs = stmt.executeQuery(Sqlite3.mprintf("PRAGMA %Q.index_info(%Q)",
                            catalog, indexInfo.name));
                    while (rs.next()) {
                        indexInfo.columnNames.add(rs.getString(3));
                    }
                    rows += indexInfo.columnNames.size();
                }

                String constantQuery = Sqlite3.join(
                        Collections.nCopies(rows, GET_INDEX_INFO_TEMPLATE).toArray(),
                        " UNION ALL ");

                if (constantQuery.isEmpty()) {
                    constantQuery = String.format("%s LIMIT 0", GET_INDEX_INFO_TEMPLATE);
                }
                else {
                    constantQuery += " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";
                }

                PreparedStatement ps = this.conn.prepareStatement(constantQuery);

                ((SqlitePreparedStatement)ps).closeOnCompletion();

                int index = 1;
                for (IndexInfo indexInfo : indexInfoList) {
                    for (int lpc = 0; lpc < indexInfo.columnNames.size(); lpc++) {
                        ps.setString(index++, catalog);
                        ps.setString(index++, tableName);
                        ps.setBoolean(index++, !indexInfo.unique);
                        ps.setString(index++, catalog);
                        ps.setString(index++, indexInfo.name);
                        ps.setInt(index++, tableIndexOther);
                        ps.setInt(index++, lpc);
                        ps.setString(index++, indexInfo.columnNames.get(lpc));
                    }
                }

                return ps.executeQuery();
            } finally {
                SqliteCommon.closeQuietly(rs);
            }
        } finally {
            SqliteCommon.closeQuietly(stmt);
        }
    }

    @Override
    public boolean supportsResultSetType(int i) throws SQLException {
        return (i == ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean ownDeletesAreVisible(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean ownInsertsAreVisible(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean othersUpdatesAreVisible(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean othersDeletesAreVisible(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean othersInsertsAreVisible(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean updatesAreDetected(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String s, String s2, String s3, int[] ints) throws SQLException {
        return this.executeConstantQuery(GET_UDTS_TEMPLATE);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getSuperTypes(String s, String s2, String s3) throws SQLException {
        return this.executeConstantQuery(GET_SUPER_TYPES_TEMPLATE);
    }

    @Override
    public ResultSet getSuperTables(String s, String s2, String s3) throws SQLException {
        return this.executeConstantQuery(GET_SUPER_TABLES_TEMPLATE);
    }

    @Override
    public ResultSet getAttributes(String s, String s2, String s3, String s4) throws SQLException {
        return this.executeConstantQuery(GET_ATTRIBUTES_TEMPLATE);
    }

    @Override
    public boolean supportsResultSetHoldability(int i) throws SQLException {
        return (i == ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        int version = Sqlite3.sqlite3_libversion_number();

        return version / 1000000;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        int version = Sqlite3.sqlite3_libversion_number();

        return (version / 1000) % 1000;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return true;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    @Override
    public ResultSet getSchemas(String s, String s2) throws SQLException {
        return this.executeConstantQuery(
                "SELECT null as TABLE_SCHEM, null as TABLE_CATALOG LIMIT 0");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return this.executeConstantQuery(GET_CLIENT_INFO_PROPERTIES_TEMPLATE);
    }

    @Override
    public ResultSet getFunctions(String s, String s2, String s3) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
            String functionNamePattern, String columnNamePattern) throws SQLException {
        SqliteConnection connection = getMetadataDatabaseConnection();

        if (functionNamePattern == null) {
            functionNamePattern = "%";
        }
        if (columnNamePattern == null) {
            columnNamePattern = "%";
        }
        PreparedStatement ps = connection.prepareStatement(GET_FUNCTION_COLUMNS_TEMPALTE);
        ((SqlitePreparedStatement)ps).closeOnCompletion();
        try {
            ps.setString(1, functionNamePattern);
            ps.setString(2, columnNamePattern);
            return ps.executeQuery();
        }
        catch (SQLException e) {
            SqliteCommon.closeQuietly(ps);
            throw e;
        }
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    @Override
    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
