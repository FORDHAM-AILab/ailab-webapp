import json
import logging
import os.path
import pickle
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import asyncio
from fermi_backend.webapp.config import WRDS_PASSWORD, WRDS_USERNAME, \
    WRDS_POSTGRES_HOST, WRDS_POSTGRES_PORT, WRDS_POSTGRES_DB
from fermi_backend.webapp.helpers import sql_session_scope, pd_read_sql_async

logger = logging.getLogger(__name__)


class NotSubscribedError(PermissionError):
    pass


class SchemaNotFoundError(FileNotFoundError):
    pass


class AsyncWRDS:
    def __init__(self, username: str = None, password: str = None):
        self._username = username if username else WRDS_USERNAME
        self._password = password if password else WRDS_PASSWORD
        self._hostname = WRDS_POSTGRES_HOST
        self._port = WRDS_POSTGRES_PORT
        self._dbname = WRDS_POSTGRES_DB

        self.engine, self.connection, self.session, self.wrds_pgsql_session_scope = (None,) * 4
        self.schema_perm = None

    async def init(self):
        self.make_async_engine_conn()
        await self.load_library_list()

    def make_async_engine_conn(self):
        username = self._username
        hostname = self._hostname
        port = self._port
        dbname = self._dbname
        password = self._password
        pguri = f"postgresql+asyncpg://{username}:{password}@{hostname}:{port}/{dbname}"
        try:
            self.engine = create_async_engine(pguri)
            self.connection = self.engine.connect()
            self.session = sessionmaker(self.engine, class_=AsyncSession)
            self.wrds_pgsql_session_scope = sql_session_scope(self.session)
        except Exception as err:

            logger.error("Error while creating the WRDS session: ", err)

    async def load_library_list(self):
        """Load the list of Postgres schemata (c.f. SAS LIBNAMEs)
        the user has permission to access."""
        if self.schema_perm is not None:
            print("Done")
            return

        print("Loading library list...")
        query = """
    WITH pgobjs AS (
        -- objects we care about - tables, views, foreign tables, partitioned tables
        SELECT oid, relnamespace, relkind
        FROM pg_class
        WHERE relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"])
    ),
    schemas AS (
        -- schemas we have usage on that represent products
        SELECT nspname AS schemaname,
            pg_namespace.oid,
            array_agg(DISTINCT relkind) AS relkind_a
        FROM pg_namespace
        JOIN pgobjs ON pg_namespace.oid = relnamespace
        WHERE nspname !~ '(^pg_)|(_old$)|(_new$)|(information_schema)'
            AND has_schema_privilege(nspname, 'USAGE') = TRUE
        GROUP BY nspname, pg_namespace.oid
    )
    SELECT schemaname
    FROM schemas
    WHERE relkind_a != ARRAY['v'::"char"] -- any schema except only views
    UNION
    -- schemas w/ views (aka "friendly names") that reference accessable product tables
    SELECT nv.schemaname
    FROM schemas nv
    JOIN pgobjs v ON nv.oid = v.relnamespace AND v.relkind = 'v'::"char"
    JOIN pg_depend dv ON v.oid = dv.refobjid AND dv.refclassid = 'pg_class'::regclass::oid
        AND dv.classid = 'pg_rewrite'::regclass::oid AND dv.deptype = 'i'::"char"
    JOIN pg_depend dt ON dv.objid = dt.objid AND dv.refobjid <> dt.refobjid
        AND dt.classid = 'pg_rewrite'::regclass::oid
        AND dt.refclassid = 'pg_class'::regclass::oid
    JOIN pgobjs t ON dt.refobjid = t.oid
        AND (t.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"]))
    JOIN schemas nt ON t.relnamespace = nt.oid
    GROUP BY nv.schemaname
    ORDER BY 1;
            """

        async with self.wrds_pgsql_session_scope as session:
            cursor = await session.execute(sa.text(query))
            self.schema_perm = [x[0] for x in cursor.fetchall()]
        print("Done")

    async def __check_schema_perms(self, schema):
        """
        Check the permissions of the schema.
        Raise permissions error if user does not have access.
        Raise other error if the schema does not exist.

        Else, return True

        :param schema: Postgres schema name.
        :rtype: bool

        """
        if schema in self.schema_perm:
            return True
        else:
            async with self.engine.begin() as conn:
                def inner(conn):
                    insp = sa.inspect(conn)
                    return insp.get_schema_names()
                insp = await conn.run_sync(inner)
                if schema in insp:
                    raise NotSubscribedError(
                        "You do not have permission to access "
                        "the {} library".format(schema)
                    )
                else:
                    raise SchemaNotFoundError("The {} library is not found.".format(schema))

    def list_libraries(self):
        """
        Return all the libraries (schemas) the user can access.

        :rtype: list

        Usage::
        >>> db.list_libraries()
        ['aha', 'audit', 'block', 'boardex', ...]
        """
        return self.schema_perm

    async def list_tables(self, library):
        """
        Returns a list of all the views/tables/foreign tables within a schema.

        :param library: Postgres schema name.

        :rtype: list

        Usage::
        >>> db.list_tables('wrdssec')
        ['wciklink_gvkey', 'dforms', 'wciklink_cusip', 'wrds_forms', ...]
        """
        if await self.__check_schema_perms(library):
            def get_names(conn):
                insp = sa.inspect(conn)
                output = insp.get_view_names(schema=library) + \
                         insp.get_table_names(schema=library) + \
                         insp.get_foreign_table_names(schema=library)
                return output

            async with self.engine.begin() as conn:
                output = await conn.run_sync(get_names)

            return output

    async def __get_schema_for_view(self, schema, table):
        """
        Internal function for getting the schema based on a view
        """
        sql_code = """SELECT distinct(source_ns.nspname) AS source_schema
                      FROM pg_depend
                      JOIN pg_rewrite
                        ON pg_depend.objid = pg_rewrite.oid
                      JOIN pg_class as dependent_view
                        ON pg_rewrite.ev_class = dependent_view.oid
                      JOIN pg_class as source_table
                        ON pg_depend.refobjid = source_table.oid
                      JOIN pg_attribute
                        ON pg_depend.refobjid = pg_attribute.attrelid
                          AND pg_depend.refobjsubid = pg_attribute.attnum
                      JOIN pg_namespace dependent_ns
                        ON dependent_ns.oid = dependent_view.relnamespace
                      JOIN pg_namespace source_ns
                        ON source_ns.oid = source_table.relnamespace
                      WHERE dependent_ns.nspname = '{schema}'
                        AND dependent_view.relname = '{view}';
                    """.format(
            schema=schema, view=table
        )
        if await self.__check_schema_perms(schema):
            with self.wrds_pgsql_session_scope as session:
                result = session.execute(sql_code)
            return result.fetchone()[0]

    async def get_row_count(self, library, table):
        """
        Uses the library and table to get the approximate row count for the table.

        :param library: Postgres schema name.
        :param table: Postgres table name.

        :rtype: int

        Usage::
        >>> db.get_row_count('wrdssec', 'dforms')
        16378400
        """

        sqlstmt = """
            EXPLAIN (FORMAT 'json')  SELECT 1 FROM {}.{} ;
        """.format(
            sa.sql.quoted_name(library, True), sa.sql.quoted_name(table, True)
        )

        try:
            with self.wrds_pgsql_session_scope as session:
                result = await session.execute(sqlstmt)
            return int(result.fetchone()[0][0]["Plan"]["Plan Rows"])
        except Exception as e:
            print("There was a problem with retrieving the row count: {}".format(e))
            return 0

    async def describe_table(self, library, table):
        """
        Takes the library and the table and describes all the columns
          in that table.
        Includes Column Name, Column Type, Nullable?, Comment

        :param library: Postgres schema name.
        :param table: Postgres table name.

        :rtype: pandas.DataFrame

        Usage::
        >>> db.describe_table('wrdssec_all', 'dforms')
                    name nullable     type comment
              0      cik     True  VARCHAR
              1    fdate     True     DATE
              2  secdate     True     DATE
              3     form     True  VARCHAR
              4   coname     True  VARCHAR
              5    fname     True  VARCHAR
        """
        rows = await self.get_row_count(library, table)
        print("Approximately {} rows in {}.{}.".format(rows, library, table))
        async with self.session.bind.connect() as conn:
            insp = conn.run_sync(sa.inspect)
            cols = await conn.run_sync(insp.get_columns, table_name=table, schema=library)
        table_info = pd.DataFrame.from_dict(cols)
        return table_info[["name", "nullable", "type", "comment"]]

    async def raw_sql(
            self,
            sql,
            coerce_float=True,
            date_cols=None,
            index_col=None,
            params=None,
            chunksize=500000,
            return_iter=False,
    ):
        """
        Queries the database using a raw SQL string.

        :param sql: SQL code in string object.
        :param coerce_float: (optional) boolean, default: True
            Attempt to convert values to non-string, non-numeric objects
            to floating point. Can result in loss of precision.
        :param date_cols: (optional) list or dict, default: None
            - List of column names to parse as date
            - Dict of ``{column_name: format string}`` where
                format string is:
                  strftime compatible in case of parsing string times or
                  is one of (D, s, ns, ms, us) in case of parsing
                    integer timestamps
            - Dict of ``{column_name: arg dict}``,
                where the arg dict corresponds to the keyword arguments of
                  :func:`pandas.to_datetime`
        :param index_col: (optional) string or list of strings,
          default: None
            Column(s) to set as index(MultiIndex)
        :param params: parameters to SQL query, if parameterized.
        :param chunksize: (optional) integer or None default: 500000
            Process query in chunks of this size. Smaller chunksizes can save
            a considerable amount of memory while query is being processed.
            Set to None run query w/o chunking.
        :param return_iter: (optional) boolean, default:False
            When chunksize is not None, return an iterator where chunksize
            number of rows is included in each chunk.

        :rtype: pandas.DataFrame or or Iterator[pandas.DataFrame]


        Usage ::
        # Basic Usage
        >>> data = db.raw_sql('select cik, fdate, coname from wrdssec_all.dforms;', date_cols=['fdate'], index_col='cik')
        >>> data.head()
            cik        fdate       coname
            0000000003 1995-02-15  DEFINED ASSET FUNDS MUNICIPAL INVT TR FD NEW Y...
            0000000003 1996-02-14  DEFINED ASSET FUNDS MUNICIPAL INVT TR FD NEW Y...
            0000000003 1997-02-19  DEFINED ASSET FUNDS MUNICIPAL INVT TR FD NEW Y...
            0000000003 1998-03-02  DEFINED ASSET FUNDS MUNICIPAL INVT TR FD NEW Y...
            0000000003 1998-03-10  DEFINED ASSET FUNDS MUNICIPAL INVT TR FD NEW Y..
            ...

        # Parameterized SQL query
        >>> parm = {'syms': ('A', 'AA', 'AAPL'), 'num_shares': 50000}
        >>> data = db.raw_sql('select * from taqmsec.ctm_20030910 where sym_root in %(syms)s and size > %(num_shares)s', params=parm)
        >>> data.head()
                  date           time_m ex sym_root sym_suffix tr_scond      size   price tr_stopind tr_corr     tr_seqnum tr_source tr_rf
            2003-09-10  11:02:09.485000  T        A       None     None  211400.0  25.350          N      00  1.929952e+15         C  None
            2003-09-10  11:04:29.508000  N        A       None     None   55500.0  25.180          N      00  1.929952e+15         C  None
            2003-09-10  15:08:21.155000  N        A       None     None   50500.0  24.470          N      00  1.929967e+15         C  None
            2003-09-10  16:10:35.522000  T        A       None        B   71900.0  24.918          N      00  1.929970e+15         C  None
            2003-09-10  09:35:20.709000  N       AA       None     None  108100.0  28.200          N      00  1.929947e+15         C  None
        """  # noqa

        try:
            df = await pd_read_sql_async(
                self.engine,
                sql=sa.text(sql),
                coerce_float=coerce_float,
                parse_dates=date_cols,
                index_col=index_col,
                chunksize=chunksize,
                params=params,
            )
            if return_iter or chunksize is None:
                return df
            else:
                full_df = pd.DataFrame()
                for chunk in df:
                    full_df = pd.concat([full_df, chunk])
                return full_df
        except sa.exc.ProgrammingError as e:
            raise e

    def close(self):
        """
        Close the connection to the database.
        """
        self.connection.close()
        self.engine.dispose()
        self.engine = None

    async def get_avail_products(self):
        saved_metafile_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'wrds_avail_products.pkl')
        if os.path.exists(saved_metafile_path):
            with open(saved_metafile_path, 'rb') as f:
                return pickle.load(f)
        login_url = "https://wrds-www.wharton.upenn.edu/login/"

        driver = webdriver.Chrome()
        driver.get(login_url)
        driver.find_element(by='name', value="username").send_keys(self._username)
        driver.find_element(by='name', value="password").send_keys(self._password)

        submit_button = driver.find_element(by='name', value='submit')
        submit_button.click()

        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "trust-browser-button"))).click()
        await asyncio.sleep(5)

        driver.get("https://wrds-www.wharton.upenn.edu/users/products/")
        table_list = driver.find_elements("tag name", 'tr')
        results = []
        for row in table_list[1:]:
            tds = row.find_elements("tag name", 'td')
            product_code, description = tds[0].find_element("tag name", "a"), tds[1]
            link, product_code = product_code.get_attribute('href'), product_code.text
            description = description.text
            try:
                results.append((link, product_code, description))
            except Exception as e:
                logger.error(e)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'wrds_avail_products.pkl'), 'wb') as file:
            pickle.dump(results, file)
        return results

    async def get_table_cols(self, schema, table_name):
        result = await self.raw_sql(f"""select column_name, 
                                        CASE 
                                         WHEN data_type = 'character varying' THEN 'VARCHAR'
                                         WHEN data_type = 'double precision'  THEN 'DOUBLE'
                                         WHEN data_type = 'date' THEN 'DATE'
                                         ELSE data_type 
                                        END AS data_type
                                        from INFORMATION_SCHEMA.COLUMNS 
                                    WHERE TABLE_NAME = '{table_name}'
                                    AND TABLE_SCHEMA = '{schema}';""")
        col_names = result['column_name'].tolist()
        data_types = result['data_type'].tolist()
        return [(col_name, data_type) for col_name, data_type in zip(col_names, data_types)]

    async def get_products_dict(self, db_list):

        db_list = [db[1] for db in db_list]
        saved_metafile_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'wrds_metadata.json')
        if os.path.exists(saved_metafile_path):
            with open(saved_metafile_path) as f:
                return json.load(f)
        result = {}

        async def get_cols(database, table):
            if database not in result:
                result[database] = {}
            cols = await self.get_table_cols(database, table)
            result[database][table] = cols

        async def get_tables(db):
            try:
                tables = await self.list_tables(db)
                await asyncio.gather(*[get_cols(db, t) for t in tables])
            except Exception as e:
                logger.info(f"DB: {db} is not found")
                print(f"DB: {db} is not found")

        await asyncio.gather(*[get_tables(db) for db in db_list])
        for db, db_dict in result.items():
            result[db] = dict(sorted(db_dict.items()))
        result = dict(sorted(result.items()))
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'wrds_metadata.json'), 'w') as file:
            json.dump(result, file)

        return result

