from psycopg2 import connect


class Database(object):
    def __init__(self, options):
        self.name = options.get('db_name', False)
        self.host = options.get('db_host', False)
        self.user = options.get('db_user', False)
        self.port = options.get('db_port', False)
        self.password = options.get('db_password', False)
        self.cursor = None
        self.conn = None
        self.connect()

    def connect(self):
        try:
            # declare a new PostgreSQL connection object
            self.conn = connect(
                dbname=self.name,
                user=self.user,
                host=self.host,
                password=self.password,
                # attempt to connect for 3 seconds then raise exception
                connect_timeout=3
            )
            self.cursor = Cursor(self.conn.cursor())
        except Exception as err:
            self.conn = None
            self.cursor = None
            raise err

    def close(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()


# TODO: inherit psycopg2 Cursor class
class Cursor(object):
    def __init__(self, obj):
        self._obj = obj

    def __call__(self, *args, **kwargs):
        pass

    def __build_dict(self, row):
        return {d.name: row[i] for i, d in enumerate(self._obj.description)}

    def dictfetchone(self):
        row = self._obj.fetchone()
        return row and self.__build_dict(row)

    def dictfetchmany(self, size):
        return [self.__build_dict(row) for row in self._obj.fetchmany(size)]

    def dictfetchall(self):
        return [self.__build_dict(row) for row in self._obj.fetchall()]

    def execute(self, *args, **kwargs):
        self._obj.execute(*args, **kwargs)

    def fetchall(self):
        return self._obj.fetchall()

    def mogrify(self, *args, **kwargs):
        return self._obj.mogrify(*args, **kwargs)
