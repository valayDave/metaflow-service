from services.utils import DBConfiguration
import shlex
# Shell Related Password and username setting happens here for Goose. 
# Ensure we escape the right characters in username/password to avoid goose failures. 
db_conf = DBConfiguration()

host = db_conf.host
port = db_conf.port
user = shlex.quote(db_conf.user)
password = shlex.quote(db_conf.password)
database_name = db_conf.database_name
