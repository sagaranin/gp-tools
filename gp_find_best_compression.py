import psycopg2
import argparse

from prettytable import PrettyTable
from psycopg2.extras import RealDictCursor
from multiprocessing.dummy import Pool


def get_cursor(config):
    conn = psycopg2.connect("dbname={database} user={user} host={host} port={port} password={password}".format(**config))
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return cursor


def run_query(cursor, sql):
    cursor.execute(sql)
    try:
        return cursor.fetchall()
    except psycopg2.ProgrammingError:
        return {}


QUICKLZ_1 = 1

ZLIB_1 = 2
ZLIB_5 = 3
ZLIB_9 = 4


RLE_TYPE_1 = 3
RLE_TYPE_2 = RLE_TYPE_1 + ZLIB_1
RLE_TYPE_3 = RLE_TYPE_1 + ZLIB_5
RLE_TYPE_4 = RLE_TYPE_1 + ZLIB_9

WEIGHTS = {

    'ZLIB_1': ZLIB_1,
    'ZLIB_5': ZLIB_5,
    'ZLIB_9': ZLIB_9,
    'RLE_TYPE_1': RLE_TYPE_1,
    'RLE_TYPE_2': RLE_TYPE_2,
    'RLE_TYPE_3': RLE_TYPE_3,
    'RLE_TYPE_4': RLE_TYPE_4
}

compressions = {
    'NONE': [1],
    'RLE_TYPE': [1, 2, 3, 4],
    'ZLIB': [1, 5, 9],
    'ZSTD': [1, 10, 19]
}


def is_current_compression_method(original_column_info, column_info):
    return original_column_info.get('compresslevel', None) == column_info.get('compresslevel', None) and original_column_info.get('compresstype', '') == column_info.get('compresstype', '').lower()


def out_info(results, original_column_info):
    sorted_results = sorted(results, key=lambda k: k['size'])
    current_column = {'size': sorted_results[0]['size']}

    for column_info in sorted_results:
        if is_current_compression_method(original_column_info, column_info):
            current_column = column_info

    summary_table = PrettyTable(['Column', 'Compression', 'Level', 'Size', 'Diff', 'Current'])

    for column_info in sorted_results:
        current_text = ''
        if column_info == current_column:
            current_text = '<<<'
        if current_column:
            diff = str(round(100.0 / current_column['size'] * column_info['size'], 2)) + ' %'
            summary_table.add_row([
                column_info['column_name'],
                column_info['compresstype'],
                column_info['compresslevel'],
                column_info['size_h'],
                diff,
                current_text
            ])
        else:
            summary_table.add_row([
                column_info['column_name'],
                column_info['compresstype'],
                column_info['compresslevel'],
                column_info['size_h'],
                '',
                current_text
            ])

    print(summary_table)


def bench_column(config, column):
    curr = get_cursor(config)
    results = []
    for compress_type, levels in compressions.items():
        for compress_level in levels:

            create_test_table = """
                CREATE TABLE compression_test_{table}_{column_name}
                WITH (
                  appendonly=true,
                  orientation=column,
                  compresstype={compresstype},
                  compresslevel={compresslevel}
                )
                AS (SELECT "{column_name}" from {schema}.{table} LIMIT {lines})
            """.format(compresstype=compress_type, compresslevel=compress_level, column_name=column['column_name'], schema=config['schema'], table=config['table'], lines=config['lines'])
            run_query(curr, create_test_table)

            get_table_size = """
                SELECT
                '{column_name}' as column_name,
                '{compresslevel}' as compresslevel,
                '{compresstype}' as compresstype,
                pg_size_pretty(pg_relation_size('compression_test_{table}_{column_name}'::regclass::oid)) as size_h,
                '{attnum}' as attnum,
                pg_relation_size('compression_test_{table}_{column_name}'::regclass::oid) as size
            """.format(compresstype=compress_type, compresslevel=compress_level, column_name=column['column_name'], attnum=column['attnum'], table=config['table'],)

            size_info = run_query(curr, get_table_size)[0]
            results.append(size_info)

            run_query(curr, 'drop table compression_test_{table}_{column_name}'.format(table=config['table'], column_name=column['column_name']))

    out_info(results, column)
    return results


def format_col(source_col):
    col = {
        'column_name': source_col['column_name'],
        'attnum': source_col['attnum']
    }
    opts = source_col.get('col_opts', [])

    if opts is None:
        return col

    for opt in opts:
        [param, value] = opt.split('=')
        col[param] = value.lower()
    return col


#  chose best, need model
def get_best_column_format(column_info, config):
    sorted_results = sorted(column_info, key=lambda k: k['size'])
    best = sorted_results[0]
    competitors = []
    for column_info in sorted_results[1:]:
        if 100 * best['size'] / column_info['size'] >= config['tradeoff_treshold']:
            comp_key = '{compresstype}_{compresslevel}'.format(**column_info)
            column_info['weight'] = WEIGHTS.get(comp_key, 5)
            competitors.append(column_info)
    sorted_competitors_by_cost = sorted(competitors, key=lambda k: k['weight'])
    return sorted_competitors_by_cost[0] if len(sorted_competitors_by_cost) else best


def make_magic(config):
    curr = get_cursor(config)
    get_table_metadata = f"""
        SELECT a.attname as column_name,
            e.attoptions as col_opts,
            a.attnum as attnum,
            null weight
            FROM pg_catalog.pg_attribute a
            LEFT  JOIN pg_catalog.pg_attribute_encoding e ON  e.attrelid = a.attrelid AND e.attnum = a.attnum
            LEFT JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  a.attnum > 0
                AND NOT a.attisdropped
                AND c.relname = '{config['table']}' and n.nspname = '{config['schema']}'
        ORDER BY a.attnum
    """
    table_info = run_query(curr, get_table_metadata)

    thread_params = []

    for column in table_info:
        column = format_col(column)
        thread_params.append((config, column))

    results = Pool(config['threads']).starmap(bench_column, thread_params)
    sorted_as_source_table = sorted(results, key=lambda k: k[0]['attnum'])

    column_list = []
    for column_info in sorted_as_source_table:
        best_colum_format = get_best_column_format(column_info, config)
        sql = 'COLUMN "{column_name}" ENCODING (compresstype={compresstype}, COMPRESSLEVEL={compresslevel})'.format(**best_colum_format)
        column_list.append(sql)

    suggested_sql = """
        CREATE TABLE {schema}.{table}_new_type (
            LIKE {schema}.{table},
            {column_list}
        )
        WITH (
          appendonly=true,
          orientation=column,
          compresstype=RLE_TYPE,
          COMPRESSLEVEL=3
        );
        
        INSERT INTO {schema}.{table}_new_type SELECT * FROM {schema}.{table};
        ANALYZE {schema}.{table}_new_type;

        ALTER TABLE {schema}.{table} RENAME TO {table}_old;
        ALTER TABLE {schema}.{table}_new_type RENAME TO {table};


    """.format(schema=config['schema'], table=config['table'], column_list=',\n\t\t'.join(column_list))
    print(suggested_sql)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", type=str, help="hostname", default="localhost")
    parser.add_argument("--port", type=int, help="port", default=5432)
    parser.add_argument("--user", type=str, help="username", default='gpadmin')
    parser.add_argument("--password", type=str, help="password")

    parser.add_argument("--database", type=str, help="db name", default="adb")
    parser.add_argument("-t", "--table", type=str, help="table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="schema name", required=True)
    parser.add_argument("-l", "--lines", type=str, help="rows to examine", default=10000000)
    parser.add_argument("--threads", type=int, help="number of threads to run bench func", default=4)
    parser.add_argument("--tradeoff_treshold", type=int, help="compaction treshhold tradeofff %", default=90, choices=range(1, 100))

    params = parser.parse_args()
    make_magic(vars(params))
