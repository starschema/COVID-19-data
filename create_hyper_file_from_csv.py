# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException


data_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="COVID-19",
    columns=[
        TableDefinition.Column("Country/Region", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Province/State", SqlType.text(), NULLABLE),
        TableDefinition.Column("Latitude", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("Longitude", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("Case_Type", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Date", SqlType.date(), NOT_NULLABLE),
        TableDefinition.Column("Cases", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("Difference", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("Last_Update_Date", SqlType.date(), NOT_NULLABLE),
    ]
)


def run_create_hyper_file_from_csv():
    """
    An example demonstrating loading data from a csv into a new Hyper file
    """
    print("EXAMPLE - Load data from CSV into table in new Hyper file")

    path_to_database = Path("covid-19.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "customer.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_table(table_definition=data_table)

            # Using path to current file, create a path that locates CSV file packaged with these examples.
            path_to_csv = str(Path(__file__).parent / "JHU_COVID-19.csv")

            # Load all rows into "Customers" table from the CSV file.
            # `execute_command` executes a SQL statement and returns the impacted row count.
            count_in_customer_table = connection.execute_command(
                command=f"COPY {data_table.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header)")

            print(f"The number of rows in table {data_table.table_name} is {count_in_customer_table}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
