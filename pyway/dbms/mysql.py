from pyway.migration import Migration
import mysql.connector

CREATE_VERSION_MIGRATIONS = "create table if not exists %s ("\
    "installed_rank serial PRIMARY KEY,"\
    "version varchar(20) NOT NULL,"\
    "extension varchar(20) NOT NULL,"\
    "name varchar(125) NOT NULL,"\
    "checksum varchar(25) NOT NULL,"\
    "apply_timestamp timestamp DEFAULT NOW()"\
    ");"
SELECT_FIELDS = ("version", "extension", "name", "checksum","apply_timestamp")
ORDER_BY_FIELD_ASC = "installed_rank"
ORDER_BY_FIELD_DESC = "installed_rank desc"
INSERT_VERSION_MIGRATE = "insert into %s (version, extension, name, checksum) values ('%s', '%s', '%s', '%s');"

class Mysql():

    def __init__(self, config):
        self.config = config
        self.version_table = config.args.database_table
        self.create_version_table_if_not_exists()

    def connect(self):
        return mysql.connector.connect(
            host=self.config.args.database_host,
            port=self.config.args.database_port,
            database=self.config.args.database_name,
            user=self.config.args.database_username,
            password=self.config.args.database_password
        )

    def create_version_table_if_not_exists(self):
        self.execute(CREATE_VERSION_MIGRATIONS % self.version_table)

    def execute(self, script):
        cnx = self.connect()
        for _ in cnx.cmd_query_iter(script):
            pass
        cnx.commit()
        cnx.close()

    def get_all_schema_migrations(self):
        cnx = self.connect()
        cursor = cnx.cursor()
        cursor.execute(f"SELECT {','.join(SELECT_FIELDS)} FROM {self.version_table} ORDER BY {ORDER_BY_FIELD_ASC}")
        migrations = []
        for row in cursor.fetchall():
            migrations.append(Migration(row[0], row[1], row[2], row[3], row[4]))
        cursor.close()
        cnx.close()
        return migrations

    def upgrade_version(self, migration):
        self.execute(INSERT_VERSION_MIGRATE % (self.version_table, migration.version,
            migration.extension, migration.name, migration.checksum))
